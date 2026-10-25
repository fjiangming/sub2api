-- Freeze the entire allocation before any member may settle. Individual bill
-- and source uniqueness still lives in billing_import_claims.
CREATE TABLE IF NOT EXISTS sub2api_custom.billing_import_groups (
    account_id BIGINT NOT NULL,
    identity TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    receipt JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, identity)
);
