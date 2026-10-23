-- A supplier bill and a local attempt may each be claimed only once, across
-- exports, service restarts and concurrent operators. This is not a money table.
CREATE TABLE IF NOT EXISTS sub2api_custom.billing_import_claims (
    account_id BIGINT NOT NULL,
    bill_identity TEXT NOT NULL,
    local_reference TEXT NOT NULL UNIQUE,
    payload_digest TEXT NOT NULL,
    case_id TEXT NOT NULL REFERENCES sub2api_custom.billing_recovery(id),
    actor TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, bill_identity)
);
