package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type BillingRecoveryImportQuote struct {
	Usage             *UsageLog `json:"usage_record"`
	PricingSource     string    `json:"pricing_source"`
	PricingAt         time.Time `json:"pricing_at"`
	GroupAuditBlocked string    `json:"group_audit_blocked,omitempty"`
}

// A current quote is an explicit alternative for old requests without a price
// snapshot. It must never replace an existing historical price or imply that
// today's prices were in force when the request ran.
func (s *GatewayService) QuoteBillingImportUsage(ctx context.Context, in *BillingRecoveryImport, key *APIKey, tokens UsageTokens) (*BillingRecoveryImportQuote, error) {
	reader, ok := BillingRecovery().(BillingRecoveryImportSourceStore)
	if !ok {
		return nil, fmt.Errorf("historical import pricing is unavailable; update the main site")
	}
	entry, err := reader.ImportSource(ctx, in)
	if err != nil {
		return nil, err
	}
	if key == nil || key.User == nil || key.ID != entry.APIKeyID || key.User.ID != entry.UserID {
		return nil, fmt.Errorf("usage ownership mismatch")
	}
	var snapshot map[string]json.RawMessage
	if err = json.Unmarshal(entry.Snapshot, &snapshot); err != nil {
		return nil, err
	}
	if snapshot == nil {
		snapshot = make(map[string]json.RawMessage)
	}
	source := "immutable_dispatch_snapshot"
	groupBlocked := ""
	var quotedAt time.Time
	if price := snapshot["pricing"]; len(price) > 0 && string(price) != "null" {
		var plan recoveryPricePlan
		if json.Unmarshal(price, &plan) != nil || !plan.Eligible {
			return nil, fmt.Errorf("this request requires native billing dimensions; token-only pricing is unavailable")
		}
		quotedAt = plan.PricingAt
	} else {
		if s.accountRepo == nil {
			return nil, fmt.Errorf("account pricing is unavailable")
		}
		account, readErr := s.accountRepo.GetByID(ctx, entry.AccountID)
		if readErr != nil {
			return nil, readErr
		}
		if key.HasRateLimits() || account.HasAnyQuotaLimit() {
			groupBlocked = "Key rolling limits or account quotas require exact historical request attribution"
		}
		pricingKey := *key
		pricingKey.GroupID = entry.GroupID
		if entry.GroupID != nil && (key.Group == nil || key.Group.ID != *entry.GroupID) {
			if s.groupRepo == nil {
				return nil, fmt.Errorf("original group pricing is unavailable")
			}
			pricingKey.Group, err = s.groupRepo.GetByID(ctx, *entry.GroupID)
			if err != nil {
				return nil, err
			}
		}
		if entry.GroupID == nil {
			pricingKey.Group = nil
		}
		quotedAt = time.Now().UTC()
		state := &billingRequestState{Key: &pricingKey, GroupID: entry.GroupID, StartedAt: quotedAt}
		state.Ingress.Store(&recoveryIngress{Model: entry.Model})
		path := entry.Endpoint
		if path == "" {
			path = "/v1/messages"
		}
		req := (&http.Request{URL: &url.URL{Path: path}}).WithContext(ctx)
		plan := s.recoveryPricePlan(ctx, state, entry.Model, req)
		if plan == nil || !plan.Eligible {
			return nil, fmt.Errorf("the main site has no supported token price for this model")
		}
		values := map[string]any{"pricing": plan, "account_rate_multiplier": account.BillingRateMultiplier(),
			"account_type": account.Type, "account_cache_ttl_override": account.IsCacheTTLOverrideEnabled(),
			"account_cache_ttl_target": account.GetCacheTTLOverrideTarget()}
		for name, value := range values {
			encoded, encodeErr := json.Marshal(value)
			if encodeErr != nil {
				return nil, encodeErr
			}
			snapshot[name] = encoded
		}
		entry.Snapshot, err = json.Marshal(snapshot)
		if err != nil {
			return nil, err
		}
		source = "current_sub2api_prices"
	}
	usage, err := s.quoteRecoveryEntry(entry, tokens)
	if err != nil {
		return nil, err
	}
	usage.ActualCost = QuantizeUsageBillingAmount(usage.TotalCost * usage.RateMultiplier)
	return &BillingRecoveryImportQuote{Usage: usage, PricingSource: source, PricingAt: quotedAt, GroupAuditBlocked: groupBlocked}, nil
}
