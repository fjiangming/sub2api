package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// Operators submit historically priced cumulative usage, never balance deltas.
func (s *GatewayService) RestoreBillingRecoveryUsage(ctx context.Context, id string, version int, key *APIKey, u *UsageLog, evidence, actor string) (string, error) {
	store := BillingRecovery()
	if store == nil {
		return "", fmt.Errorf("billing recovery is disabled")
	}
	entry, err := store.Get(ctx, id)
	if err != nil {
		return "", err
	}
	if entry.Version != version || (entry.State != "unknown" && entry.State != "review" && entry.State != "settled") {
		return "", ErrBillingRecoveryConflict
	}
	// Imported legacy failures have already passed their dispatch deadline. Their
	// fresh staging timestamp must not impose another minute before confirmation.
	imported := (strings.HasPrefix(entry.RequestRoot, "historical-error:") || strings.HasPrefix(entry.RequestRoot, "historical-log:")) && entry.State == "review" && entry.Usage == nil
	if (!imported && time.Since(entry.UpdatedAt) < time.Minute) || entry.Command == nil && time.Since(entry.CreatedAt) < BillingRecoveryTimeout()+time.Minute {
		return "", fmt.Errorf("request may still be running; wait for the recovery deadline before restoring usage")
	}
	if key == nil || key.User == nil || key.ID != entry.APIKeyID || key.User.ID != entry.UserID {
		return "", fmt.Errorf("usage ownership mismatch")
	}
	if u == nil || strings.TrimSpace(evidence) == "" || strings.TrimSpace(u.Model) == "" {
		return "", fmt.Errorf("historical usage and pricing evidence are required")
	}
	if err = validateRecoveryUsage(u); err != nil {
		return "", err
	}
	if u.TotalCost <= 0 || u.RateMultiplier <= 0 {
		return "", fmt.Errorf("a positive historical price and multiplier are required")
	}
	if u.UserID != 0 && u.UserID != entry.UserID || u.APIKeyID != 0 && u.APIKeyID != entry.APIKeyID || u.AccountID != 0 && u.AccountID != entry.AccountID {
		return "", fmt.Errorf("usage identity mismatch")
	}
	account, err := s.accountRepo.GetByID(ctx, entry.AccountID)
	if err != nil {
		return "", err
	}
	clean := *u
	clean.User = nil
	clean.APIKey = nil
	clean.Account = nil
	clean.Group = nil
	clean.Subscription = nil
	u = &clean
	u.ID = 0
	u.UserID = entry.UserID
	u.APIKeyID = entry.APIKeyID
	u.AccountID = entry.AccountID
	u.GroupID = entry.GroupID
	u.SubscriptionID = entry.SubscriptionID
	u.CreatedAt = entry.CreatedAt
	u.RequestID = "recovery:" + entry.ID
	u.ActualCost = QuantizeUsageBillingAmount(u.TotalCost * u.RateMultiplier)
	if entry.State == "settled" {
		previous := &UsageLog{}
		cursor := entry
		for depth := 0; ; depth++ {
			if depth >= 100 || cursor.Usage == nil {
				return "", ErrBillingRecoveryConflict
			}
			if cursor.Usage.Model != u.Model || cursor.Usage.RateMultiplier != u.RateMultiplier {
				return "", fmt.Errorf("correction must retain the original model and multiplier")
			}
			addRecoveryUsage(previous, cursor.Usage)
			if cursor.ParentCaseID == nil {
				break
			}
			cursor, err = store.Get(ctx, *cursor.ParentCaseID)
			if err != nil {
				return "", err
			}
		}
		subtractRecoveryUsage(u, previous)
		if err = validateRecoveryUsage(u); err != nil {
			return "", err
		}
		if u.ActualCost <= 0 {
			return "", fmt.Errorf("verified cumulative usage must exceed the amount already billed")
		}
	}
	u.BillingType = BillingTypeBalance
	var subscription *UserSubscription
	if entry.SubscriptionID != nil {
		u.BillingType = BillingTypeSubscription
		subscription = &UserSubscription{ID: *entry.SubscriptionID}
	}
	multiplier := account.BillingRateMultiplier()
	if u.AccountRateMultiplier != nil {
		multiplier = *u.AccountRateMultiplier
	}
	if multiplier < 0 || math.IsNaN(multiplier) || math.IsInf(multiplier, 0) || multiplier > 1e6 {
		return "", fmt.Errorf("invalid account multiplier")
	}
	u.AccountRateMultiplier = &multiplier
	cmd := buildUsageBillingCommand(u.RequestID, u, &postUsageBillingParams{User: key.User, APIKey: key, Account: account, Subscription: subscription, IsSubscriptionBill: subscription != nil, AccountRateMultiplier: multiplier, Cost: &CostBreakdown{TotalCost: u.TotalCost, ActualCost: u.ActualCost}})
	if cmd == nil {
		return "", fmt.Errorf("unable to prepare usage settlement")
	}
	var snapshot struct {
		KeyQuota      bool   `json:"key_quota"`
		KeyRateLimits bool   `json:"key_rate_limits"`
		AccountQuota  bool   `json:"account_quota"`
		AccountType   string `json:"account_type"`
	}
	_ = json.Unmarshal(entry.Snapshot, &snapshot)
	if snapshot.KeyQuota {
		cmd.APIKeyQuotaCost = u.ActualCost
	}
	if snapshot.KeyRateLimits {
		cmd.APIKeyRateLimitCost = u.ActualCost
	}
	if snapshot.AccountType != "" {
		cmd.AccountType = snapshot.AccountType
		cmd.AccountQuotaCost = 0
		if snapshot.AccountQuota {
			cmd.AccountQuotaCost = QuantizeUsageBillingAmount(u.TotalCost * multiplier)
		}
	}
	cmd.RequestFingerprint = ""
	cmd.Normalize()
	if entry.State == "settled" {
		return store.Supplement(ctx, id, version, u, cmd, evidence, actor)
	}
	return id, store.Restore(ctx, id, version, u, cmd, evidence, actor)
}

// A single list covers native token, image and video dimensions.
var recoveryCountFields = []string{"InputTokens", "OutputTokens", "CacheCreationTokens", "CacheReadTokens", "CacheCreation5mTokens", "CacheCreation1hTokens", "ImageCount", "VideoCount", "ImageInputTokens", "ImageOutputTokens"}
var recoveryCostFields = []string{"InputCost", "OutputCost", "CacheCreationCost", "CacheReadCost", "ImageInputCost", "ImageOutputCost", "TotalCost", "ActualCost"}

func validateRecoveryUsage(u *UsageLog) error {
	v := reflect.ValueOf(u).Elem()
	for _, name := range recoveryCountFields {
		n := v.FieldByName(name).Int()
		if n < 0 || n > 2147483647 {
			return fmt.Errorf("invalid usage: %s", name)
		}
	}
	for _, name := range append(append([]string{}, recoveryCostFields...), "RateMultiplier") {
		n := v.FieldByName(name).Float()
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1e8 {
			return fmt.Errorf("invalid pricing: %s", name)
		}
	}
	if u.VideoDurationSeconds != nil && *u.VideoDurationSeconds < 0 {
		return fmt.Errorf("invalid video duration")
	}
	component := u.InputCost + u.OutputCost + u.CacheCreationCost + u.CacheReadCost + u.ImageInputCost + u.ImageOutputCost
	if QuantizeUsageBillingAmount(component) > QuantizeUsageBillingAmount(u.TotalCost) {
		return fmt.Errorf("usage components exceed total cost")
	}
	return nil
}
func addRecoveryUsage(dst, src *UsageLog)      { changeRecoveryUsage(dst, src, 1) }
func subtractRecoveryUsage(dst, src *UsageLog) { changeRecoveryUsage(dst, src, -1) }
func changeRecoveryUsage(dst, src *UsageLog, sign int) {
	d, s := reflect.ValueOf(dst).Elem(), reflect.ValueOf(src).Elem()
	for _, name := range recoveryCountFields {
		f := d.FieldByName(name)
		f.SetInt(f.Int() + int64(sign)*s.FieldByName(name).Int())
	}
	for _, name := range recoveryCostFields {
		f := d.FieldByName(name)
		f.SetFloat(QuantizeUsageBillingAmount(f.Float() + float64(sign)*s.FieldByName(name).Float()))
	}
}
