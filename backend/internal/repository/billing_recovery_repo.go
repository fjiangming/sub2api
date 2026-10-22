package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
)

type billingRecoveryRepository struct{ db *sql.DB }

func (r *billingRecoveryRepository) Begin(ctx context.Context, c *service.BillingRecoveryCase) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery
      (id,request_root,user_id,api_key_id,group_id,subscription_id,account_id,platform,endpoint,model,request_snapshot)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11::jsonb,'{}'::jsonb)||COALESCE((SELECT jsonb_build_object('account_type',type,'account_quota',
       COALESCE((extra->>'quota_limit')::numeric,0)>0 OR COALESCE((extra->>'quota_daily_limit')::numeric,0)>0 OR COALESCE((extra->>'quota_weekly_limit')::numeric,0)>0)
       FROM accounts WHERE id=$7),'{}'::jsonb))`, c.ID, c.RequestRoot, c.UserID, c.APIKeyID, c.GroupID, c.SubscriptionID, c.AccountID, c.Platform, c.Endpoint, c.Model, c.Snapshot)
	return err
}

func (r *billingRecoveryRepository) Observe(ctx context.Context, id string, status int, requestID string, usage json.RawMessage, code string) error {
	if len(usage) == 0 {
		usage = json.RawMessage(`null`)
	}
	_, err := r.db.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET http_status=$2,
      upstream_request_id=CASE WHEN $3<>'' THEN $3 ELSE upstream_request_id END,
      observed_usage=COALESCE(observed_usage,'{}'::jsonb)||COALESCE(NULLIF($4::jsonb,'null'::jsonb),'{}'::jsonb),
      last_error=CASE WHEN $5<>'' THEN $5 ELSE last_error END, updated_at=NOW() WHERE id=$1`, id, status, requestID, []byte(usage), code)
	return err
}

func cleanRecoveryUsage(usage *service.UsageLog) *service.UsageLog {
	result := *usage
	result.User, result.APIKey, result.Account = nil, nil, nil
	result.Group, result.Subscription = nil, nil
	return &result
}

func (r *billingRecoveryRepository) Prepare(ctx context.Context, id string, cmd *service.UsageBillingCommand, usage *service.UsageLog) (string, error) {
	// A stable native key must select the same immutable command on every retry.
	var existing, fingerprint string
	err := r.db.QueryRowContext(ctx, `SELECT id,billing_command->>'RequestFingerprint' FROM sub2api_custom.billing_recovery WHERE api_key_id=$1 AND native_request_id=$2`, cmd.APIKeyID, cmd.RequestID).Scan(&existing, &fingerprint)
	if err == nil {
		if fingerprint != cmd.RequestFingerprint {
			return "", service.ErrUsageBillingRequestConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	commandJSON, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	record := cleanRecoveryUsage(usage)
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	state, code := "prepared", ""
	if cmd.RecoveryPricingMissing {
		state, code = "review", "PRICING_UNAVAILABLE"
	}
	if id == "" {
		id = uuid.NewString()
		_, err = r.db.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery
         (id,request_root,user_id,api_key_id,group_id,subscription_id,account_id,model,native_request_id,state,usage_record,billing_command,last_error)
         VALUES ($1,$1,$2,$3,$4,$5,$6,$7,$8,$11,$9,$10,$12)
         ON CONFLICT(api_key_id,native_request_id) DO NOTHING`, id, cmd.UserID, cmd.APIKeyID, record.GroupID, cmd.SubscriptionID, cmd.AccountID, cmd.Model, cmd.RequestID, recordJSON, commandJSON, state, code)
	} else {
		var result sql.Result
		result, err = r.db.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET native_request_id=$2,
         state=$12,last_error=CASE WHEN $13<>'' THEN $13 ELSE last_error END,usage_record=$3,billing_command=$4,model=$5,updated_at=NOW(),version=version+1,
         upstream_request_id=COALESCE(NULLIF($9,''),upstream_request_id),group_id=$10,subscription_id=$11
         WHERE id=$1 AND state='unknown' AND user_id=$6 AND api_key_id=$7 AND account_id=$8`, id, cmd.RequestID, recordJSON, commandJSON, cmd.Model, cmd.UserID, cmd.APIKeyID, cmd.AccountID, record.UpstreamRequestID, record.GroupID, record.SubscriptionID, state, code)
		if err == nil {
			if count, _ := result.RowsAffected(); count != 1 {
				return "", service.ErrBillingRecoveryConflict
			}
		}
	}
	if err != nil {
		return "", err
	}
	// Resolve concurrent insert winners and verify their fingerprint.
	err = r.db.QueryRowContext(ctx, `SELECT id,billing_command->>'RequestFingerprint' FROM sub2api_custom.billing_recovery WHERE api_key_id=$1 AND native_request_id=$2`, cmd.APIKeyID, cmd.RequestID).Scan(&existing, &fingerprint)
	if err == nil && fingerprint != cmd.RequestFingerprint {
		err = service.ErrUsageBillingRequestConflict
	}
	return existing, err
}

const recoveryColumns = `id,request_root,user_id,api_key_id,group_id,subscription_id,account_id,platform,endpoint,model,upstream_request_id,state,http_status,observed_usage,usage_record,billing_command,evidence,last_error,attempts,version,created_at,updated_at,next_retry_at,cache_pending,parent_case_id,request_snapshot`

type recoveryScanner interface{ Scan(...any) error }

func scanRecovery(row recoveryScanner) (*service.BillingRecoveryCase, error) {
	c := &service.BillingRecoveryCase{}
	var usage, command []byte
	err := row.Scan(&c.ID, &c.RequestRoot, &c.UserID, &c.APIKeyID, &c.GroupID, &c.SubscriptionID, &c.AccountID, &c.Platform, &c.Endpoint, &c.Model, &c.UpstreamRequestID, &c.State, &c.HTTPStatus, &c.ObservedUsage, &usage, &command, &c.Evidence, &c.LastError, &c.Attempts, &c.Version, &c.CreatedAt, &c.UpdatedAt, &c.NextRetryAt, &c.CachePending, &c.ParentCaseID, &c.Snapshot)
	if err != nil {
		return nil, err
	}
	if len(usage) > 0 {
		if err = json.Unmarshal(usage, &c.Usage); err != nil {
			return nil, err
		}
	}
	if len(command) > 0 {
		if err = json.Unmarshal(command, &c.Command); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (r *billingRecoveryRepository) Get(ctx context.Context, id string) (*service.BillingRecoveryCase, error) {
	return scanRecovery(r.db.QueryRowContext(ctx, `SELECT `+recoveryColumns+` FROM sub2api_custom.billing_recovery WHERE id=$1`, id))
}
func (r *billingRecoveryRepository) List(ctx context.Context, state string, limit, offset int) ([]service.BillingRecoveryCase, int64, error) {
	if limit < 1 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	var total int64
	const filter = `($1='' OR state=$1 OR ($1='pending' AND (state='prepared' OR (state='settled' AND cache_pending)) AND next_retry_at<=NOW()))`
	if err := r.db.QueryRowContext(ctx, `SELECT count(*) FROM sub2api_custom.billing_recovery WHERE `+filter, state).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+recoveryColumns+` FROM sub2api_custom.billing_recovery
      WHERE `+filter+` ORDER BY CASE WHEN $1='pending' THEN next_retry_at END ASC,created_at DESC,id LIMIT $2 OFFSET $3`, state, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	result := []service.BillingRecoveryCase{}
	for rows.Next() {
		c, err := scanRecovery(rows)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, *c)
	}
	return result, total, rows.Err()
}

func (r *billingRecoveryRepository) Review(ctx context.Context, id string, version int, state, evidence, actor string) error {
	if state != "review" && state != "ignored" && state != "prepared" {
		return service.ErrBillingRecoveryConflict
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET state=$3,evidence=$4,
       updated_at=NOW(),next_retry_at=NOW(),attempts=0,version=version+1 WHERE id=$1 AND version=$2 AND state<>'settled'
       AND ($3<>'prepared' OR billing_command IS NOT NULL)
       AND (state<>'unknown' OR created_at<NOW()-($5*INTERVAL '1 second'))`, id, version, state, evidence, int(service.BillingRecoveryTimeout().Seconds())+60)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return service.ErrBillingRecoveryConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery_events(case_id,action,actor,detail) VALUES($1,$2,$3,$4)`, id, state, actor, evidence); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *billingRecoveryRepository) Restore(ctx context.Context, id string, version int, usage *service.UsageLog, cmd *service.UsageBillingCommand, evidence, actor string) error {
	recordJSON, err := json.Marshal(cleanRecoveryUsage(usage))
	if err != nil {
		return err
	}
	commandJSON, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET state='review',usage_record=$3,
      billing_command=$4,native_request_id=$5,evidence=$6,updated_at=NOW(),version=version+1
      WHERE id=$1 AND version=$2 AND state IN ('unknown','review')`, id, version, recordJSON, commandJSON, cmd.RequestID, evidence)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return service.ErrBillingRecoveryConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery_events(case_id,action,actor,detail) VALUES($1,'usage_restored',$2,$3)`, id, actor, evidence); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *billingRecoveryRepository) Failure(ctx context.Context, id, code string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET last_error=$2,attempts=attempts+1,
       state=CASE WHEN state='prepared' AND attempts>=11 THEN 'review' ELSE state END,
       version=version+CASE WHEN state='prepared' AND attempts>=11 THEN 1 ELSE 0 END,
       next_retry_at=NOW()+LEAST(3600,power(2,LEAST(attempts+1,12))) * INTERVAL '1 second', updated_at=NOW()
       WHERE id=$1 AND (state='prepared' OR (state='settled' AND cache_pending))`, id, code)
	return err
}
func (r *billingRecoveryRepository) Events(ctx context.Context, id string) ([]map[string]any, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT action,actor,detail,created_at FROM sub2api_custom.billing_recovery_events WHERE case_id=$1 ORDER BY id LIMIT 500`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []map[string]any{}
	for rows.Next() {
		var a, b, c string
		var at any
		if err := rows.Scan(&a, &b, &c, &at); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"action": a, "actor": b, "detail": c, "created_at": at})
	}
	return items, rows.Err()
}

func stageRecoveryUsageInTransaction(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand) error {
	if cmd.RecoveryUsage == nil {
		return errors.New("priced usage record required")
	}
	var state, fingerprint string
	if err := tx.QueryRowContext(ctx, `SELECT state,billing_command->>'RequestFingerprint' FROM sub2api_custom.billing_recovery WHERE id=$1 FOR UPDATE`, cmd.RecoveryCaseID).Scan(&state, &fingerprint); err != nil {
		return err
	}
	if (state != "prepared" && state != "settled") || fingerprint != cmd.RequestFingerprint {
		return service.ErrBillingRecoveryConflict
	}
	usage := cmd.RecoveryUsage
	for _, v := range []float64{usage.ActualCost, usage.TotalCost, cmd.BalanceCost, cmd.SubscriptionCost} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return fmt.Errorf("invalid usage amount")
		}
	}
	// Reuse the native insert shape; record creation and all financial effects commit together.
	if _, err := (&usageLogRepository{}).createSingle(ctx, tx, usage); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE usage_logs SET actual_cost=$3 WHERE request_id=$1 AND api_key_id=$2
       AND user_id=$4 AND account_id=$5 AND actual_cost=0`, cmd.RequestID, cmd.APIKeyID, usage.ActualCost, cmd.UserID, cmd.AccountID)
	return err
}
