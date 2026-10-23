-- Fork-owned lookup indexes; never rewrite the already applied recovery migration.
CREATE INDEX IF NOT EXISTS custom_billing_recovery_user_lookup
    ON sub2api_custom.billing_recovery (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS custom_billing_recovery_account_lookup
    ON sub2api_custom.billing_recovery (account_id, created_at DESC);
