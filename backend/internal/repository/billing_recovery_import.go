package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

var billingImportDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (r *billingRecoveryRepository) ReserveImport(ctx context.Context, in *service.BillingRecoveryImport, actor string) (string, error) {
	if in == nil || in.AccountID <= 0 || in.UserID <= 0 || in.APIKeyID <= 0 ||
		!billingImportDigest.MatchString(in.BillIdentity) || !billingImportDigest.MatchString(in.PayloadDigest) ||
		strings.TrimSpace(in.Model) == "" || utf8.RuneCountInString(in.Model) > 128 || utf8.RuneCountInString(in.UpstreamID) > 128 ||
		utf8.RuneCountInString(in.Evidence) < 8 || utf8.RuneCountInString(in.Evidence) > 4000 ||
		!validImportSource(in) ||
		in.StartedAt.IsZero() || time.Since(in.StartedAt) < service.BillingRecoveryTimeout()+time.Minute {
		return "", service.ErrBillingRecoveryConflict
	}
	local := "case:" + in.CaseID
	if in.CaseID == "" {
		local = fmt.Sprintf("error:%d:%d", in.ErrorID, in.EventIndex)
		if in.LogSource != nil {
			local = "log:" + in.LogSource.RequestID
		}
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	// Serialize claims for an account, including claims through different exports.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("billing-import:%d", in.AccountID)); err != nil {
		return "", err
	}
	var id, bill, digest, reference string
	var account int64
	err = tx.QueryRowContext(ctx, `SELECT case_id,account_id,bill_identity,payload_digest,local_reference
      FROM sub2api_custom.billing_import_claims WHERE (account_id=$1 AND bill_identity=$2) OR local_reference=$3`,
		in.AccountID, in.BillIdentity, local).Scan(&id, &account, &bill, &digest, &reference)
	if err == nil {
		if account != in.AccountID || bill != in.BillIdentity || digest != in.PayloadDigest || reference != local {
			return "", service.ErrBillingRecoveryConflict
		}
		return id, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if in.CaseID != "" {
		entry, readErr := scanRecovery(tx.QueryRowContext(ctx, `SELECT `+recoveryColumns+` FROM sub2api_custom.billing_recovery WHERE id=$1 FOR UPDATE`, in.CaseID))
		if readErr != nil {
			return "", readErr
		}
		if entry.Version != in.Version || entry.UserID != in.UserID || entry.APIKeyID != in.APIKeyID || entry.AccountID != in.AccountID ||
			entry.Usage != nil || (entry.State != "unknown" && entry.State != "review") || time.Since(entry.UpdatedAt) < time.Minute {
			return "", service.ErrBillingRecoveryConflict
		}
		id = entry.ID
	} else {
		id, err = r.stageHistoricalError(ctx, tx, in)
		if err != nil {
			return "", err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_import_claims
      (account_id,bill_identity,local_reference,payload_digest,case_id,actor) VALUES($1,$2,$3,$4,$5,$6)`,
		in.AccountID, in.BillIdentity, local, in.PayloadDigest, id, actor); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery_events(case_id,action,actor,detail)
      VALUES($1,'csv_import_reserved',$2,$3)`, id, actor, in.Evidence); err != nil {
		return "", err
	}
	return id, tx.Commit()
}

func (r *billingRecoveryRepository) stageHistoricalError(ctx context.Context, tx *sql.Tx, in *service.BillingRecoveryImport) (string, error) {
	entry, err := r.historicalImport(ctx, tx, in, false)
	if err != nil {
		return "", err
	}
	return entry.ID, nil
}

// ImportSource performs the same attribution and duplicate checks as reservation,
// without creating a case, claim, usage record or financial effect.
func (r *billingRecoveryRepository) ImportSource(ctx context.Context, in *service.BillingRecoveryImport) (*service.BillingRecoveryCase, error) {
	if in == nil || !validImportSource(in) || in.UserID <= 0 || in.APIKeyID <= 0 || in.AccountID <= 0 ||
		in.StartedAt.IsZero() || time.Since(in.StartedAt) < service.BillingRecoveryTimeout()+time.Minute {
		return nil, service.ErrBillingRecoveryConflict
	}
	if in.CaseID != "" {
		entry, err := r.Get(ctx, in.CaseID)
		if err != nil {
			return nil, err
		}
		if entry.Version != in.Version || entry.UserID != in.UserID || entry.APIKeyID != in.APIKeyID || entry.AccountID != in.AccountID ||
			entry.Usage != nil || (entry.State != "unknown" && entry.State != "review") || time.Since(entry.UpdatedAt) < time.Minute {
			return nil, service.ErrBillingRecoveryConflict
		}
		return entry, nil
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return r.historicalImport(ctx, tx, in, true)
}

func validImportSource(in *service.BillingRecoveryImport) bool {
	count := 0
	if in.CaseID != "" {
		count++
		if in.Version < 1 {
			return false
		}
	}
	if in.ErrorID != 0 {
		count++
		if in.ErrorID < 0 || in.EventIndex < -1 {
			return false
		}
	}
	if in.LogSource != nil {
		count++
		log := in.LogSource
		if strings.TrimSpace(log.RequestID) == "" || len(log.RequestID) > 128 || len(log.ClientID) > 128 ||
			log.GroupID <= 0 || !billingImportDigest.MatchString(log.Digest) || !strings.EqualFold(log.Model, in.Model) ||
			log.LatencyMS < 0 || log.LatencyMS > 14400000 || log.EndedAt.Before(in.StartedAt) ||
			time.Since(log.EndedAt) < time.Minute ||
			log.EndedAt.Sub(in.StartedAt)-time.Duration(log.LatencyMS)*time.Millisecond > 5*time.Second ||
			log.EndedAt.Sub(in.StartedAt)-time.Duration(log.LatencyMS)*time.Millisecond < -5*time.Second {
			return false
		}
		switch log.StatusCode {
		case 499, 500, 502, 503, 504, 524:
		default:
			return false
		}
	}
	return count == 1
}

func (r *billingRecoveryRepository) historicalImport(ctx context.Context, tx *sql.Tx, in *service.BillingRecoveryImport, inspect bool) (*service.BillingRecoveryCase, error) {
	var userID, keyID int64
	var accountID, groupID sql.NullInt64
	var at time.Time
	var model, upstreamModel, platform, endpoint, requestID, clientID string
	var events []byte
	var stream bool
	var user, key sql.NullInt64
	upstreamID := in.UpstreamID
	var nativeUpstreamID string
	var err error
	if in.LogSource != nil {
		log := in.LogSource
		userID, keyID = in.UserID, in.APIKeyID
		accountID, groupID = sql.NullInt64{Int64: in.AccountID, Valid: true}, sql.NullInt64{Int64: log.GroupID, Valid: true}
		at, model, requestID, clientID, stream = log.EndedAt, log.Model, log.RequestID, log.ClientID, log.Stream
		if err = tx.QueryRowContext(ctx, `SELECT platform FROM accounts WHERE id=$1 AND deleted_at IS NULL`, in.AccountID).Scan(&platform); err != nil {
			return nil, err
		}
	} else {
		err = tx.QueryRowContext(ctx, `SELECT user_id,api_key_id,account_id,group_id,created_at,COALESCE(model,''),
      COALESCE(upstream_model,''),COALESCE(platform,''),COALESCE(request_path,''),COALESCE(request_id,''),
      COALESCE(client_request_id,''),upstream_errors,COALESCE(stream,false) FROM ops_error_logs WHERE id=$1`, in.ErrorID).
			Scan(&user, &key, &accountID, &groupID, &at, &model, &upstreamModel, &platform, &endpoint, &requestID, &clientID, &events, &stream)
		if err != nil {
			return nil, err
		}
		userID, keyID = user.Int64, key.Int64
		if in.EventIndex >= 0 {
			var attempts []service.OpsUpstreamErrorEvent
			if json.Unmarshal(events, &attempts) != nil || in.EventIndex >= len(attempts) {
				return nil, service.ErrBillingRecoveryConflict
			}
			attempt := attempts[in.EventIndex]
			nativeUpstreamID = attempt.UpstreamRequestID
			accountID = sql.NullInt64{Int64: attempt.AccountID, Valid: attempt.AccountID > 0}
			if in.UpstreamID != "" && attempt.UpstreamRequestID != "" && in.UpstreamID != attempt.UpstreamRequestID {
				return nil, service.ErrBillingRecoveryConflict
			}
			if upstreamID == "" {
				upstreamID = attempt.UpstreamRequestID
			}
		} else {
			var attempts []service.OpsUpstreamErrorEvent
			_ = json.Unmarshal(events, &attempts)
			for _, attempt := range attempts {
				if attempt.AccountID > 0 {
					return nil, service.ErrBillingRecoveryConflict
				}
			}
		}
	}
	if userID != in.UserID || keyID != in.APIKeyID || !accountID.Valid || accountID.Int64 != in.AccountID ||
		!groupID.Valid || at.Before(in.StartedAt) || at.Sub(in.StartedAt) > 4*time.Hour ||
		(requestID == "" && clientID == "" && nativeUpstreamID == "") ||
		(!strings.EqualFold(in.Model, model) && !strings.EqualFold(in.Model, upstreamModel)) {
		return nil, service.ErrBillingRecoveryConflict
	}
	ids := []string{}
	for _, value := range []string{requestID, clientID, upstreamID} {
		if value != "" {
			ids = append(ids, value)
		}
	}
	if in.LogSource != nil {
		// Native billing may prefix the client correlation ID.
		for _, value := range []string{requestID, clientID} {
			if value != "" {
				ids = append(ids, "client:"+value)
			}
		}
	}
	var exists bool
	// Refuse already billed requests and legacy rows which have a durable intent.
	// The operator must use that intent instead, so it cannot later settle twice.
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM usage_logs u WHERE user_id=$1 AND api_key_id=$2 AND account_id=$3
      AND (request_id=ANY($4::text[]) OR to_jsonb(u)->>'upstream_request_id'=ANY($4::text[])))
      OR EXISTS(SELECT 1 FROM sub2api_custom.billing_recovery WHERE user_id=$1 AND api_key_id=$2 AND account_id=$3
      AND (request_snapshot->>'request_id'=ANY($4::text[]) OR request_snapshot->>'client_request_id'=ANY($4::text[])
        OR upstream_request_id=ANY($4::text[])))
      OR EXISTS(SELECT 1 FROM usage_billing_dedup WHERE api_key_id=$2 AND request_id=ANY($4::text[]))
      OR EXISTS(SELECT 1 FROM usage_billing_dedup_archive WHERE api_key_id=$2 AND request_id=ANY($4::text[])) AS exists`, userID, keyID, in.AccountID, pq.Array(ids)).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, service.ErrBillingRecoveryConflict
	}
	var subscriptionID *int64
	var groupType string
	if err = tx.QueryRowContext(ctx, `SELECT subscription_type FROM groups WHERE id=$1`, groupID.Int64).Scan(&groupType); err != nil {
		return nil, err
	}
	if groupType == service.SubscriptionTypeSubscription {
		rows, queryErr := tx.QueryContext(ctx, `SELECT id FROM user_subscriptions WHERE user_id=$1 AND group_id=$2
          AND starts_at<=$3 AND expires_at>$3 AND deleted_at IS NULL ORDER BY id LIMIT 2`, userID, groupID.Int64, in.StartedAt)
		if queryErr != nil {
			return nil, queryErr
		}
		var matches []int64
		for rows.Next() {
			var value int64
			if err = rows.Scan(&value); err != nil {
				_ = rows.Close()
				return nil, err
			}
			matches = append(matches, value)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		if len(matches) != 1 {
			return nil, service.ErrBillingRecoveryConflict
		}
		subscriptionID = &matches[0]
	}
	snapshot, err := json.Marshal(map[string]any{"csv_import": true, "original_error_id": in.ErrorID,
		"original_event_index": in.EventIndex, "request_id": requestID, "client_request_id": clientID, "downstream_stream": stream,
		"operator_verified_log": in.LogSource})
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	root := "historical-error:" + fmt.Sprintf("%d:%d", in.ErrorID, in.EventIndex)
	if in.LogSource != nil {
		root = "historical-log:" + in.LogSource.RequestID
	}
	entry := &service.BillingRecoveryCase{ID: id, RequestRoot: root, UserID: userID, APIKeyID: keyID, AccountID: in.AccountID,
		GroupID: &groupID.Int64, SubscriptionID: subscriptionID, Platform: platform, Endpoint: endpoint, Model: in.Model,
		UpstreamRequestID: upstreamID, Snapshot: snapshot, CreatedAt: in.StartedAt, State: "review"}
	if inspect {
		return entry, nil
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery
      (id,request_root,user_id,api_key_id,account_id,group_id,subscription_id,platform,endpoint,model,
       upstream_request_id,state,request_snapshot,evidence,created_at)
      SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'review',$12::jsonb || jsonb_build_object(
        'account_type',a.type,'account_rate_multiplier',COALESCE(a.rate_multiplier,1),
        'user_email',u.email,'api_key_name',k.name,'account_name',a.name,
        'account_quota',COALESCE((a.extra->>'quota_limit')::numeric,0)>0
          OR COALESCE((a.extra->>'quota_daily_limit')::numeric,0)>0
          OR COALESCE((a.extra->>'quota_weekly_limit')::numeric,0)>0),$13,$14
      FROM accounts a JOIN api_keys k ON k.id=$4 AND k.user_id=$3 AND k.deleted_at IS NULL
      JOIN users u ON u.id=$3 AND u.deleted_at IS NULL WHERE a.id=$5 AND a.deleted_at IS NULL`,
		id, root, userID, keyID, in.AccountID, groupID.Int64,
		subscriptionID, platform, endpoint, in.Model, upstreamID, snapshot, in.Evidence, in.StartedAt)
	if err != nil {
		return nil, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, service.ErrBillingRecoveryConflict
	}
	return entry, nil
}
