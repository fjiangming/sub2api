package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBillingRecoveryImportGroupRestoreRechecksPriceAndChangedQuotaConfiguration(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	for _, scenario := range []string{"valid", "changed_price", "enabled_key_limit"} {
		t.Run(scenario, func(t *testing.T) {
			multiplier := 1.0
			usage := &UsageLog{UserID: 1, APIKeyID: 2, AccountID: 3, Model: "test", InputTokens: 100,
				TotalCost: 0.01, RateMultiplier: 0.9, ActualCost: 0.009, AccountRateMultiplier: &multiplier}
			snapshot, err := json.Marshal(map[string]any{"csv_group_identity": "original", "csv_group_usage": usage})
			require.NoError(t, err)
			at := time.Now().Add(-4 * time.Hour)
			store := &restoreTestStore{entry: &BillingRecoveryCase{ID: "case", RequestRoot: "historical-error:1:-1", State: "review", Version: 1,
				UserID: 1, APIKeyID: 2, AccountID: 3, CreatedAt: at, UpdatedAt: at, Snapshot: snapshot}}
			ConfigureBillingRecovery(store)
			key := &APIKey{ID: 2, User: &User{ID: 1}}
			if scenario == "changed_price" {
				usage.TotalCost = 1
			}
			if scenario == "enabled_key_limit" {
				key.RateLimit5h = 10
			}
			gateway := &GatewayService{accountRepo: recoveryAccountRepo{}}
			_, err = gateway.RestoreBillingRecoveryUsage(context.Background(), "case", 1, key, usage, "group confirmation", "admin")
			if scenario == "valid" {
				require.NoError(t, err)
				require.Equal(t, 0.009, store.command.BalanceCost)
			} else {
				require.ErrorIs(t, err, ErrBillingRecoveryConflict)
				require.Nil(t, store.command)
			}
		})
	}
}

func TestBillingRecoveryImportGroupFreezesAllUsageAndPriceDimensions(t *testing.T) {
	multiplier, group := 0.79, int64(5)
	usage := &UsageLog{UserID: 3, APIKeyID: 4, AccountID: 9, GroupID: &group, Model: "test", InputTokens: 100,
		TotalCost: 0.01, RateMultiplier: 0.9, ActualCost: 0.009, AccountRateMultiplier: &multiplier}
	require.True(t, ValidBillingImportGroupUsage(usage))
	snapshot, err := json.Marshal(map[string]any{"csv_group_identity": "group", "csv_group_usage": usage})
	require.NoError(t, err)
	entry := &BillingRecoveryCase{Snapshot: snapshot}
	for _, change := range []string{"native_id", "tokens", "price", "owner", "account_multiplier", "model", "tier"} {
		t.Run(change, func(t *testing.T) {
			changed := *usage
			switch change {
			case "native_id":
				changed.ID, changed.RequestID, changed.CreatedAt = 123, "recovery:original", time.Now()
			case "tokens":
				changed.InputTokens++
			case "price":
				changed.TotalCost, changed.ActualCost = 0.02, 0.018
			case "owner":
				changed.UserID = 99
			case "account_multiplier":
				value := 1.0
				changed.AccountRateMultiplier = &value
			case "model":
				changed.Model = "other"
			case "tier":
				tier := "priority"
				changed.ServiceTier = &tier
			}
			err := verifyFrozenImportGroupUsage(entry, &changed)
			if change == "native_id" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrBillingRecoveryConflict)
			}
		})
	}
	require.NoError(t, verifyFrozenImportGroupUsage(&BillingRecoveryCase{Snapshot: []byte(`{}`)}, usage), "ordinary recovery remains compatible")
	require.ErrorIs(t, verifyFrozenImportGroupUsage(&BillingRecoveryCase{Snapshot: []byte(`{"csv_group_identity":"group"}`)}, usage), ErrBillingRecoveryConflict)
	require.ErrorIs(t, verifyFrozenImportGroupUsage(&BillingRecoveryCase{Snapshot: []byte(`{"csv_group_identity":"group","csv_group_usage":"invalid"}`)}, usage), ErrBillingRecoveryConflict)
}
