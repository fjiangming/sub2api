package service

import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

// Group allocation proves the financial owner, not an exact historical pairing.
// Matching and quote-equivalence checks remain in operations-center.
type BillingRecoveryImportGroupMember struct {
	Source BillingRecoveryImport `json:"source"`
	Usage  *UsageLog             `json:"usage_record"`
}

type BillingRecoveryImportGroup struct {
	Identity      string                             `json:"identity" binding:"required,len=64,hexadecimal"`
	PayloadDigest string                             `json:"payload_digest" binding:"required,len=64,hexadecimal"`
	Members       []BillingRecoveryImportGroupMember `json:"members" binding:"required,min=2,max=20"`
}

type BillingRecoveryImportClaim struct {
	AccountID      int64  `json:"account_id"`
	BillIdentity   string `json:"bill_identity"`
	LocalReference string `json:"local_reference"`
	CaseID         string `json:"case_id"`
	State          string `json:"state"`
	UserID         int64  `json:"user_id"`
	APIKeyID       int64  `json:"api_key_id"`
	GroupID        *int64 `json:"group_id"`
}

type BillingRecoveryImportGroupStore interface {
	ReserveImportGroup(context.Context, *BillingRecoveryImportGroup, string) ([]BillingRecoveryImportClaim, error)
	ImportClaims(context.Context, int64, time.Time, time.Time) ([]BillingRecoveryImportClaim, error)
}

func ValidBillingImportGroupUsage(usage *UsageLog) bool {
	return usage != nil && validateRecoveryUsage(usage) == nil && usage.TotalCost > 0 && usage.RateMultiplier > 0 &&
		usage.ActualCost == QuantizeUsageBillingAmount(usage.TotalCost*usage.RateMultiplier) &&
		usage.ImageInputTokens == 0 && usage.ImageOutputTokens == 0 && usage.ImageCount == 0 && usage.VideoCount == 0
}

func verifyFrozenImportGroupUsage(entry *BillingRecoveryCase, usage *UsageLog) error {
	var snapshot struct {
		Group string    `json:"csv_group_identity"`
		Usage *UsageLog `json:"csv_group_usage"`
	}
	if len(entry.Snapshot) == 0 {
		return nil
	}
	if json.Unmarshal(entry.Snapshot, &snapshot) != nil {
		return ErrBillingRecoveryConflict
	}
	if snapshot.Group == "" {
		return nil
	}
	if snapshot.Usage == nil || usage == nil {
		return ErrBillingRecoveryConflict
	}
	normalize := func(value *UsageLog) []byte {
		clean := *value
		clean.ID, clean.RequestID, clean.CreatedAt = 0, "", time.Time{}
		clean.User, clean.APIKey, clean.Account, clean.Group, clean.Subscription = nil, nil, nil, nil, nil
		encoded, _ := json.Marshal(clean)
		return encoded
	}
	if !bytes.Equal(normalize(snapshot.Usage), normalize(usage)) {
		return ErrBillingRecoveryConflict
	}
	return nil
}
