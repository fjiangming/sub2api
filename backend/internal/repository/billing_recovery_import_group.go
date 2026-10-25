package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *billingRecoveryRepository) ReserveImportGroup(ctx context.Context, in *service.BillingRecoveryImportGroup, actor string) ([]service.BillingRecoveryImportClaim, error) {
	if in == nil || !billingImportDigest.MatchString(in.Identity) || !billingImportDigest.MatchString(in.PayloadDigest) || len(in.Members) < 2 || len(in.Members) > 20 {
		return nil, service.ErrBillingRecoveryConflict
	}
	first := &in.Members[0].Source
	bills, sources := map[string]bool{}, map[string]bool{}
	for _, member := range in.Members {
		source, usage := &member.Source, member.Usage
		if !validBillingImport(source) || source.CaseID != "" || usage == nil || usage.GroupID == nil ||
			usage.UserID != source.UserID || usage.APIKeyID != source.APIKeyID || usage.AccountID != source.AccountID ||
			usage.SubscriptionID != nil || usage.BillingType != service.BillingTypeBalance ||
			source.UserID != first.UserID || source.APIKeyID != first.APIKeyID || source.AccountID != first.AccountID ||
			!service.ValidBillingImportGroupUsage(usage) || bills[source.BillIdentity] || sources[importLocalReference(source)] {
			return nil, service.ErrBillingRecoveryConflict
		}
		bills[source.BillIdentity], sources[importLocalReference(source)] = true, true
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("billing-import:%d", first.AccountID)); err != nil {
		return nil, err
	}
	var digest string
	var saved []byte
	err = tx.QueryRowContext(ctx, `SELECT payload_digest,receipt FROM sub2api_custom.billing_import_groups WHERE account_id=$1 AND identity=$2`, first.AccountID, in.Identity).Scan(&digest, &saved)
	if err == nil {
		var receipt []service.BillingRecoveryImportClaim
		if digest != in.PayloadDigest || json.Unmarshal(saved, &receipt) != nil || len(receipt) != len(in.Members) {
			return nil, service.ErrBillingRecoveryConflict
		}
		return receipt, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	// An uncertain time allocation must not change rolling quota effects. Keep
	// quota-bearing requests on the exact per-request recovery path.
	var quotaBound bool
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(k.rate_limit_5h,0)>0 OR COALESCE(k.rate_limit_1d,0)>0 OR COALESCE(k.rate_limit_7d,0)>0
      OR COALESCE((a.extra->>'quota_limit')::numeric,0)>0 OR COALESCE((a.extra->>'quota_daily_limit')::numeric,0)>0
      OR COALESCE((a.extra->>'quota_weekly_limit')::numeric,0)>0
      FROM api_keys k JOIN accounts a ON a.id=$2 AND a.deleted_at IS NULL
      WHERE k.id=$1 AND k.user_id=$3 AND k.deleted_at IS NULL FOR SHARE OF k,a`, first.APIKeyID, first.AccountID, first.UserID).Scan(&quotaBound); err != nil {
		return nil, err
	}
	if quotaBound {
		return nil, service.ErrBillingRecoveryConflict
	}
	// Validate every source before staging any of them. A related dispatch intent
	// must be reconciled through that intent, never a second historical case.
	var groupID int64
	for _, member := range in.Members {
		entry, readErr := r.historicalImport(ctx, tx, &member.Source, true)
		if readErr != nil {
			return nil, readErr
		}
		if entry.GroupID == nil || entry.SubscriptionID != nil || *entry.GroupID != *member.Usage.GroupID ||
			(groupID != 0 && groupID != *entry.GroupID) {
			return nil, service.ErrBillingRecoveryConflict
		}
		groupID = *entry.GroupID
	}
	receipt := make([]service.BillingRecoveryImportClaim, 0, len(in.Members))
	for _, member := range in.Members {
		caseID, reserveErr := r.reserveImport(ctx, tx, &member.Source, actor)
		if reserveErr != nil {
			return nil, reserveErr
		}
		usage, encodeErr := json.Marshal(member.Usage)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if _, err = tx.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET request_snapshot=request_snapshot ||
          jsonb_build_object('csv_group_identity',$2::text,'csv_group_usage',$3::jsonb) WHERE id=$1`, caseID, in.Identity, usage); err != nil {
			return nil, err
		}
		receipt = append(receipt, service.BillingRecoveryImportClaim{AccountID: first.AccountID, BillIdentity: member.Source.BillIdentity,
			LocalReference: importLocalReference(&member.Source), CaseID: caseID, State: "review", UserID: first.UserID, APIKeyID: first.APIKeyID, GroupID: &groupID})
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_import_groups(account_id,identity,payload_digest,receipt) VALUES($1,$2,$3,$4)`, first.AccountID, in.Identity, in.PayloadDigest, encoded); err != nil {
		return nil, err
	}
	return receipt, tx.Commit()
}

func (r *billingRecoveryRepository) ImportClaims(ctx context.Context, account int64, from, to time.Time) ([]service.BillingRecoveryImportClaim, error) {
	if account <= 0 || from.IsZero() || !to.After(from) || to.Sub(from) > 34*24*time.Hour {
		return nil, service.ErrBillingRecoveryConflict
	}
	rows, err := r.db.QueryContext(ctx, `SELECT c.account_id,c.bill_identity,c.local_reference,c.case_id,r.state,r.user_id,r.api_key_id,r.group_id
      FROM sub2api_custom.billing_import_claims c JOIN sub2api_custom.billing_recovery r ON r.id=c.case_id
      WHERE c.account_id=$1 AND r.created_at >= $2 AND r.created_at < $3 ORDER BY c.bill_identity LIMIT 20001`, account, from, to)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	claims := []service.BillingRecoveryImportClaim{}
	for rows.Next() {
		var claim service.BillingRecoveryImportClaim
		if err = rows.Scan(&claim.AccountID, &claim.BillIdentity, &claim.LocalReference, &claim.CaseID, &claim.State, &claim.UserID, &claim.APIKeyID, &claim.GroupID); err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	if len(claims) > 20000 {
		return nil, service.ErrBillingRecoveryConflict
	}
	return claims, rows.Err()
}
