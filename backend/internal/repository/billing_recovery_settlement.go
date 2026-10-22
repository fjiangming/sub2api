package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
)

func (r *billingRecoveryRepository) CacheComplete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET cache_pending=FALSE,
      last_error=CASE WHEN last_error='CACHE_INVALIDATION_FAILED' THEN '' ELSE last_error END WHERE id=$1 AND state='settled'`, id)
	return err
}

// Each correction is a separate usage row. Lock the original case so two operators cannot
// charge the same difference, even when a request is retried after losing its receipt.
func (r *billingRecoveryRepository) Supplement(ctx context.Context, id string, version int, u *service.UsageLog, cmd *service.UsageBillingCommand, evidence, actor string) (string, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	var actual int
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT version,state FROM sub2api_custom.billing_recovery WHERE id=$1 FOR UPDATE`, id).Scan(&actual, &state); err != nil {
		return "", err
	}
	if actual != version || state != "settled" {
		return "", service.ErrBillingRecoveryConflict
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sub2api_custom.billing_recovery WHERE parent_case_id=$1 AND state<>'ignored')`, id).Scan(&exists); err != nil {
		return "", err
	}
	// A correction is immutable once submitted. Further discrepancies receive a new
	// correction against the last settled child, producing an auditable chain.
	if exists {
		return "", service.ErrBillingRecoveryConflict
	}
	child := uuid.NewString()
	u.RequestID = "recovery:" + child
	cmd.RequestID = u.RequestID
	record, err := json.Marshal(cleanRecoveryUsage(u))
	if err != nil {
		return "", err
	}
	command, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery
 (id,request_root,user_id,api_key_id,group_id,subscription_id,account_id,platform,endpoint,model,upstream_request_id,native_request_id,state,usage_record,billing_command,evidence,parent_case_id,request_snapshot,created_at)
 SELECT $2,request_root,user_id,api_key_id,group_id,subscription_id,account_id,platform,endpoint,model,upstream_request_id,$3,'review',$4,$5,$6,id,request_snapshot,created_at
 FROM sub2api_custom.billing_recovery WHERE id=$1`, id, child, u.RequestID, record, command, evidence)
	if err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sub2api_custom.billing_recovery SET version=version+1,updated_at=NOW() WHERE id=$1`, id); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sub2api_custom.billing_recovery_events(case_id,action,actor,detail) VALUES($1,'supplement_created',$2,$3),($4,'usage_restored',$2,$5)`, id, actor, child, child, evidence); err != nil {
		return "", err
	}
	return child, tx.Commit()
}

// Monetary amounts use the original command. Rolling counters only receive usage
// whose original timestamp belongs to the currently stored window.
func (r *usageBillingRepository) applyHistoricalRecoveryEffects(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand, result *service.UsageBillingApplyResult) error {
	if cmd.RecoveryUsage == nil || cmd.RecoveryUsage.CreatedAt.IsZero() {
		return errors.New("historical usage timestamp required")
	}
	at := cmd.RecoveryUsage.CreatedAt
	if cmd.SubscriptionCost > 0 && cmd.SubscriptionID != nil {
		sub := &service.UserSubscription{}
		if err := tx.QueryRowContext(ctx, `SELECT starts_at,expires_at,daily_window_start,weekly_window_start,monthly_window_start
          FROM user_subscriptions WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, *cmd.SubscriptionID).Scan(&sub.StartsAt, &sub.ExpiresAt, &sub.DailyWindowStart, &sub.WeeklyWindowStart, &sub.MonthlyWindowStart); err != nil {
			return err
		}
		daily, weekly, monthly := recoverySubscriptionWindows(sub, at)
		res, err := tx.ExecContext(ctx, `UPDATE user_subscriptions SET
   daily_usage_usd=daily_usage_usd+CASE WHEN $3 THEN $1::numeric ELSE 0 END,
   weekly_usage_usd=weekly_usage_usd+CASE WHEN $4 THEN $1::numeric ELSE 0 END,
   monthly_usage_usd=monthly_usage_usd+CASE WHEN $5 THEN $1::numeric ELSE 0 END,
   updated_at=NOW() WHERE id=$2 AND deleted_at IS NULL`, cmd.SubscriptionCost, *cmd.SubscriptionID, daily, weekly, monthly)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return service.ErrSubscriptionNotFound
		}
	}
	if cmd.BalanceCost > 0 {
		value, sufficient, err := deductUsageBillingBalance(ctx, tx, cmd.UserID, cmd.BalanceCost)
		if err != nil {
			return err
		}
		result.NewBalance = &value
		result.BalanceOverdrafted = !sufficient
	}
	if cmd.APIKeyQuotaCost > 0 {
		exhausted, err := incrementUsageBillingAPIKeyQuota(ctx, tx, cmd.APIKeyID, cmd.APIKeyQuotaCost)
		if err != nil && !errors.Is(err, service.ErrAPIKeyNotFound) {
			return err
		}
		result.APIKeyQuotaExhausted = exhausted
	}
	if cmd.APIKeyRateLimitCost > 0 {
		_, err := tx.ExecContext(ctx, `UPDATE api_keys SET
   usage_5h=usage_5h+CASE WHEN $3>=window_5h_start AND $3<window_5h_start+INTERVAL '5 hours' THEN $1::numeric ELSE 0 END,
   usage_1d=usage_1d+CASE WHEN $3>=window_1d_start AND $3<window_1d_start+INTERVAL '24 hours' THEN $1::numeric ELSE 0 END,
   usage_7d=usage_7d+CASE WHEN $3>=window_7d_start AND $3<window_7d_start+INTERVAL '7 days' THEN $1::numeric ELSE 0 END,
   updated_at=NOW() WHERE id=$2 AND deleted_at IS NULL`, cmd.APIKeyRateLimitCost, cmd.APIKeyID, at)
		if err != nil {
			return err
		}
	}
	if cmd.AccountQuotaCost > 0 && service.AccountTypeSupportsQuota(cmd.AccountType) {
		_, err := tx.ExecContext(ctx, `UPDATE accounts SET extra=COALESCE(extra,'{}'::jsonb)||jsonb_build_object(
   'quota_used',COALESCE((extra->>'quota_used')::numeric,0)+$1,
   'quota_daily_used',COALESCE((extra->>'quota_daily_used')::numeric,0)+CASE WHEN
    $3::timestamptz>=(extra->>'quota_daily_start')::timestamptz AND $3::timestamptz<COALESCE((extra->>'quota_daily_reset_at')::timestamptz,(extra->>'quota_daily_start')::timestamptz+INTERVAL '1 day') THEN $1::numeric ELSE 0 END,
   'quota_weekly_used',COALESCE((extra->>'quota_weekly_used')::numeric,0)+CASE WHEN
    $3::timestamptz>=(extra->>'quota_weekly_start')::timestamptz AND $3::timestamptz<COALESCE((extra->>'quota_weekly_reset_at')::timestamptz,(extra->>'quota_weekly_start')::timestamptz+INTERVAL '7 days') THEN $1::numeric ELSE 0 END
   ),updated_at=NOW() WHERE id=$2 AND deleted_at IS NULL`, cmd.AccountQuotaCost, cmd.AccountID, at)
		if err != nil {
			return err
		}
	}
	return nil
}

func recoverySubscriptionWindows(sub *service.UserSubscription, at time.Time) (bool, bool, bool) {
	belongs := func(start, end *time.Time) bool {
		return start != nil && end != nil && !at.Before(*start) && at.Before(*end)
	}
	return belongs(sub.DailyWindowStart, sub.DailyResetTime()), belongs(sub.WeeklyWindowStart, sub.WeeklyResetTime()), belongs(sub.MonthlyWindowStart, sub.MonthlyResetTime())
}
