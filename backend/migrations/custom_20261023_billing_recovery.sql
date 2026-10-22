-- Fork-owned durable billing handoff. See CUSTOMIZATIONS.md before merging upstream.
CREATE SCHEMA IF NOT EXISTS sub2api_custom;
CREATE TABLE IF NOT EXISTS sub2api_custom.billing_recovery (
    id TEXT PRIMARY KEY,
    request_root TEXT NOT NULL,
    user_id BIGINT NOT NULL,
    api_key_id BIGINT NOT NULL,
    group_id BIGINT,
    subscription_id BIGINT,
    account_id BIGINT NOT NULL,
    platform TEXT NOT NULL DEFAULT '',
    endpoint TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    upstream_request_id TEXT NOT NULL DEFAULT '',
    native_request_id TEXT,
    state TEXT NOT NULL DEFAULT 'unknown' CHECK (state IN ('unknown','prepared','settled','review','ignored')),
    http_status INTEGER,
    observed_usage JSONB,
    usage_record JSONB,
    billing_command JSONB,
    evidence TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at TIMESTAMPTZ,
    cache_pending BOOLEAN NOT NULL DEFAULT TRUE,
    parent_case_id TEXT REFERENCES sub2api_custom.billing_recovery(id),
    request_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (api_key_id, native_request_id)
);
CREATE INDEX IF NOT EXISTS custom_billing_recovery_pending ON sub2api_custom.billing_recovery (state, next_retry_at, created_at);
CREATE TABLE IF NOT EXISTS sub2api_custom.billing_recovery_events (
    id BIGSERIAL PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES sub2api_custom.billing_recovery(id),
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
