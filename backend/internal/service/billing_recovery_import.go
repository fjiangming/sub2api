package service

import (
	"context"
	"time"
)

// The operator service owns CSV parsing and attribution. Pricing and settlement
// reuse the core; log sources are explicit administrator attestations.
type BillingRecoveryImport struct {
	AccountID     int64                     `json:"account_id" binding:"required,min=1"`
	BillIdentity  string                    `json:"bill_identity" binding:"required,len=64,hexadecimal"`
	PayloadDigest string                    `json:"payload_digest" binding:"required,len=64,hexadecimal"`
	CaseID        string                    `json:"case_id"`
	Version       int                       `json:"version"`
	ErrorID       int64                     `json:"error_id"`
	EventIndex    int                       `json:"event_index"`
	UserID        int64                     `json:"user_id" binding:"required,min=1"`
	APIKeyID      int64                     `json:"api_key_id" binding:"required,min=1"`
	StartedAt     time.Time                 `json:"started_at" binding:"required"`
	Model         string                    `json:"model" binding:"required,max=128"`
	UpstreamID    string                    `json:"upstream_request_id" binding:"max=128"`
	Evidence      string                    `json:"evidence" binding:"required,min=8,max=4000"`
	LogSource     *BillingRecoveryLogSource `json:"log_source,omitempty"`
}

type BillingRecoveryLogSource struct {
	RequestID  string    `json:"request_id"`
	ClientID   string    `json:"client_request_id"`
	GroupID    int64     `json:"group_id"`
	Model      string    `json:"model"`
	EndedAt    time.Time `json:"ended_at"`
	LatencyMS  int64     `json:"latency_ms"`
	StatusCode int       `json:"status_code"`
	Stream     bool      `json:"stream"`
	Digest     string    `json:"digest"`
}

type BillingRecoveryImportStore interface {
	ReserveImport(context.Context, *BillingRecoveryImport, string) (string, error)
}

type BillingRecoveryImportSourceStore interface {
	ImportSource(context.Context, *BillingRecoveryImport) (*BillingRecoveryCase, error)
}
