package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type importQuoteStore struct {
	BillingRecoveryStore
	entry *BillingRecoveryCase
}

func (s *importQuoteStore) ImportSource(context.Context, *BillingRecoveryImport) (*BillingRecoveryCase, error) {
	copy := *s.entry
	return &copy, nil
}

type importQuoteAccounts struct {
	AccountRepository
	account *Account
}

func (r *importQuoteAccounts) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

type importQuoteRates struct {
	UserGroupRateRepository
	rate *float64
}

func (r *importQuoteRates) GetByUserAndGroup(context.Context, int64, int64) (*float64, error) {
	return r.rate, nil
}

type importQuoteGroups struct {
	GroupRepository
	group *Group
	read  int64
}

func (r *importQuoteGroups) GetByID(_ context.Context, id int64) (*Group, error) {
	r.read = id
	return r.group, nil
}

func TestBillingRecoveryImportQuoteUsesNativeModelAndUserGroupPrices(t *testing.T) {
	for _, model := range []string{"claude-opus-4-6", "gpt-5.4", "gemini-3.1-pro"} {
		for _, override := range []bool{false, true} {
			t.Run(model+map[bool]string{false: "/group", true: "/user"}[override], func(t *testing.T) {
				groupID, accountRate := int64(7), 0.79
				key := &APIKey{ID: 2, User: &User{ID: 1}, GroupID: &groupID, Group: &Group{ID: groupID, RateMultiplier: 0.9}}
				entry := &BillingRecoveryCase{UserID: 1, APIKeyID: 2, GroupID: &groupID, AccountID: 3, Model: model,
					Platform: PlatformAnthropic, Endpoint: "/v1/messages", Snapshot: json.RawMessage(`{}`), CreatedAt: time.Now().Add(-24 * time.Hour)}
				store := &importQuoteStore{entry: entry}
				ConfigureBillingRecovery(store)
				t.Cleanup(func() { ConfigureBillingRecovery(nil) })
				billing := NewBillingService(nil, nil)
				svc := &GatewayService{billingService: billing, resolver: NewModelPricingResolver(nil, billing),
					accountRepo: &importQuoteAccounts{account: &Account{ID: 3, RateMultiplier: &accountRate}}}
				multiplier := 0.9
				if override {
					multiplier = 0.6
					svc.userGroupRateResolver = newUserGroupRateResolver(&importQuoteRates{rate: &multiplier}, nil, time.Minute, nil, "test")
				}
				tokens := UsageTokens{InputTokens: 1000, OutputTokens: 300, CacheReadTokens: 200}
				quote, err := svc.QuoteBillingImportUsage(context.Background(), &BillingRecoveryImport{}, key, tokens)
				require.NoError(t, err)
				require.Equal(t, "current_sub2api_prices", quote.PricingSource)
				require.Equal(t, multiplier, quote.Usage.RateMultiplier)
				require.Equal(t, accountRate, *quote.Usage.AccountRateMultiplier)
				want, err := billing.CalculateTokenCostForRequest(TokenCostRequest{Ctx: context.Background(), Model: model, Group: key.Group,
					Tokens: tokens, RateMultiplier: multiplier, PricingAt: quote.PricingAt, Resolver: svc.resolver})
				require.NoError(t, err)
				require.InDelta(t, want.TotalCost, quote.Usage.TotalCost, 1e-9)
				require.Equal(t, QuantizeUsageBillingAmount(want.TotalCost*multiplier), quote.Usage.ActualCost)
				require.Equal(t, entry.CreatedAt, quote.Usage.CreatedAt)
				require.Equal(t, `{}`, string(entry.Snapshot), "read-only quotes must not rewrite a stored snapshot")
			})
		}
	}
}

func TestBillingRecoveryImportQuotePreservesSnapshotsAndRejectsUnsupportedPrices(t *testing.T) {
	groupID, rate := int64(7), 0.79
	plan := &recoveryPricePlan{Version: 1, Eligible: true, Model: "historical-model", RateMultiplier: 0.4, TimeMultiplier: 1,
		PricingAt: time.Now().Add(-48 * time.Hour), BasePricing: &ModelPricing{InputPricePerToken: 0.000001, OutputPricePerToken: 0.000002}}
	snapshot, err := json.Marshal(map[string]any{"pricing": plan, "account_rate_multiplier": rate})
	require.NoError(t, err)
	store := &importQuoteStore{entry: &BillingRecoveryCase{UserID: 1, APIKeyID: 2, AccountID: 3, GroupID: &groupID, Snapshot: snapshot}}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	svc := &GatewayService{billingService: NewBillingService(nil, nil)}
	key := &APIKey{ID: 2, User: &User{ID: 1}, Group: &Group{ID: groupID, RateMultiplier: 99}}
	quote, err := svc.QuoteBillingImportUsage(context.Background(), &BillingRecoveryImport{}, key, UsageTokens{InputTokens: 1000})
	require.NoError(t, err)
	require.Equal(t, "immutable_dispatch_snapshot", quote.PricingSource)
	require.Equal(t, 0.0004, quote.Usage.ActualCost)
	require.Equal(t, plan.PricingAt.Unix(), quote.PricingAt.Unix())
	plan.Eligible = false
	store.entry.Snapshot, err = json.Marshal(map[string]any{"pricing": plan, "account_rate_multiplier": rate})
	require.NoError(t, err)
	_, err = svc.QuoteBillingImportUsage(context.Background(), &BillingRecoveryImport{}, key, UsageTokens{InputTokens: 1000})
	require.ErrorContains(t, err, "native billing dimensions")
	_, err = svc.QuoteBillingImportUsage(context.Background(), &BillingRecoveryImport{}, &APIKey{ID: 9, User: &User{ID: 1}}, UsageTokens{})
	require.ErrorContains(t, err, "ownership")
}

func TestBillingRecoveryImportQuoteUsesOriginalGroupAfterKeyMoves(t *testing.T) {
	originalID, currentID := int64(7), int64(8)
	key := &APIKey{ID: 2, User: &User{ID: 1}, GroupID: &currentID, Group: &Group{ID: currentID, RateMultiplier: 9}}
	groups := &importQuoteGroups{group: &Group{ID: originalID, RateMultiplier: 0.9}}
	store := &importQuoteStore{entry: &BillingRecoveryCase{UserID: 1, APIKeyID: 2, AccountID: 3, GroupID: &originalID,
		Model: "claude-opus-4-6", Platform: PlatformAnthropic, Endpoint: "/v1/messages", Snapshot: json.RawMessage(`{}`)}}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	svc := &GatewayService{billingService: NewBillingService(nil, nil), groupRepo: groups,
		accountRepo: &importQuoteAccounts{account: &Account{ID: 3}}}
	quote, err := svc.QuoteBillingImportUsage(context.Background(), &BillingRecoveryImport{}, key, UsageTokens{InputTokens: 1000})
	require.NoError(t, err)
	require.Equal(t, originalID, groups.read)
	require.Equal(t, &originalID, quote.Usage.GroupID)
	require.Equal(t, 0.9, quote.Usage.RateMultiplier)
	require.Equal(t, currentID, key.Group.ID)
	require.Equal(t, &currentID, key.GroupID)
}
