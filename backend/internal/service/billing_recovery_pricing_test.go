package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBillingRecoveryFreezesHistoricalPricingAndOwnership(t *testing.T) {
	for _, model := range []string{"claude-opus-4-6", "gpt-5.4", "gemini-3.1-pro", "deepseek-v4-flash"} {
		t.Run(model, func(t *testing.T) {
			store := &recoveryTestStore{}
			ConfigureBillingRecovery(store)
			t.Cleanup(func() { ConfigureBillingRecovery(nil); ConfigureBillingRecoveryPricing(nil) })
			billing := NewBillingService(nil, nil)
			svc := &GatewayService{billingService: billing}
			svc.resolver = NewModelPricingResolver(nil, billing)
			ConfigureBillingRecoveryPricing(svc)
			groupID := int64(7)
			key := &APIKey{ID: 2, User: &User{ID: 1}, GroupID: &groupID, Group: &Group{ID: 7, RateMultiplier: 1.8, LongContextPricingEnabled: true}}
			ctx, stop := BillingRecoveryRequest(context.Background(), key, nil, "/v1/messages", true)
			defer stop()
			data := `{"model":"` + model + `","messages":[{"content":"private prompt"}]}`
			ingress, _ := http.NewRequestWithContext(ctx, "POST", "https://local.invalid/v1/messages", strings.NewReader(data))
			_, err := io.Copy(io.Discard, CaptureBillingRecoveryIngress(ingress).Body)
			require.NoError(t, err)
			req, _ := http.NewRequestWithContext(ctx, "POST", "https://provider.invalid/v1/messages", strings.NewReader(data))
			_, err = BeginBillingRecoveryAttempt(req, 3)
			require.NoError(t, err)
			require.NotContains(t, string(store.entry.Snapshot), "private")
			var snap map[string]any
			require.NoError(t, json.Unmarshal(store.entry.Snapshot, &snap))
			snap["account_rate_multiplier"] = 0.7
			store.entry.Snapshot, _ = json.Marshal(snap)
			store.entry.Version, store.entry.CreatedAt = 3, time.Now().Add(-time.Hour)
			readStore := &restoreTestStore{entry: store.entry}
			ConfigureBillingRecovery(readStore)
			tokens := UsageTokens{InputTokens: 300000, OutputTokens: 81, CacheReadTokens: 1000, CacheCreationTokens: 10, CacheCreation5mTokens: 10}
			var plan recoveryPricePlan
			encoded, _ := json.Marshal(snap["pricing"])
			require.NoError(t, json.Unmarshal(encoded, &plan))
			want, err := billing.CalculateTokenCostForRequest(TokenCostRequest{Ctx: ctx, Model: model, Group: key.Group,
				Tokens: tokens, RateMultiplier: plan.RateMultiplier, PricingAt: plan.PricingAt, Resolver: svc.resolver})
			require.NoError(t, err)
			// Neither a live group edit nor a changed catalog may rewrite old bills.
			key.Group.RateMultiplier = 99
			billing.fallbackPrices = map[string]*ModelPricing{}
			usage, err := svc.QuoteBillingRecoveryUsage(ctx, store.entry.ID, 3, tokens)
			require.NoError(t, err)
			require.InDelta(t, want.TotalCost, usage.TotalCost, 1e-9)
			require.InDelta(t, want.ActualCost, usage.ActualCost, 1e-9)
			require.EqualValues(t, 1, usage.UserID)
			require.EqualValues(t, 2, usage.APIKeyID)
			require.EqualValues(t, 3, usage.AccountID)
			require.Equal(t, model, usage.RequestedModel)
			require.Equal(t, RequestTypeSync, usage.RequestType)
			require.Equal(t, store.entry.CreatedAt, usage.CreatedAt)
			_, err = svc.QuoteBillingRecoveryUsage(ctx, store.entry.ID, 2, tokens)
			require.ErrorIs(t, err, ErrBillingRecoveryConflict)
			_, err = svc.QuoteBillingRecoveryUsage(ctx, store.entry.ID, 3, UsageTokens{InputTokens: -1})
			require.Error(t, err)
			_, err = svc.QuoteBillingRecoveryUsage(ctx, store.entry.ID, 3, UsageTokens{InputTokens: 1, ImageOutputTokens: 5})
			require.Error(t, err)
		})
	}
}

func TestBillingRecoveryCorrelationIsDistinctPerDispatchAndPreservesCallerHeaders(t *testing.T) {
	store := &recoveryTestStore{}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	ctx, stop := BillingRecoveryRequest(context.Background(), &APIKey{ID: 2, User: &User{ID: 1}}, nil, "/v1/messages", true)
	defer stop()
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://provider.invalid/v1/messages", strings.NewReader(`{"model":"test"}`))
	req.Header["x-request-id"] = []string{"caller-id"}
	first, err := BeginBillingRecoveryAttempt(req, 3)
	require.NoError(t, err)
	require.Equal(t, "caller-id", recoveryRequestHeader(req.Header, "x-request-id"))
	require.Equal(t, first, req.Header.Get("X-Client-Request-ID"))
	second, err := BeginBillingRecoveryAttempt(req, 3)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	require.Equal(t, second, req.Header.Get("X-Client-Request-ID"))
	require.Equal(t, "caller-id", recoveryRequestHeader(req.Header, "x-request-id"))
}

func TestBillingRecoveryRecognizesNativeGeminiStreamWithoutBodyFlag(t *testing.T) {
	store := &recoveryTestStore{}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	path := "/v1beta/models/gemini-test:streamGenerateContent"
	ctx, stop := BillingRecoveryRequest(context.Background(), &APIKey{ID: 2, User: &User{ID: 1}}, nil, path, true)
	defer stop()
	ingress, _ := http.NewRequestWithContext(ctx, "POST", "https://local.invalid"+path, strings.NewReader(`{"contents":[]}`))
	_, err := io.Copy(io.Discard, CaptureBillingRecoveryIngress(ingress).Body)
	require.NoError(t, err)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://provider.invalid"+path, strings.NewReader(`{"contents":[]}`))
	_, err = BeginBillingRecoveryAttempt(req, 3)
	require.NoError(t, err)
	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(store.entry.Snapshot, &snapshot))
	require.Equal(t, true, snapshot["downstream_stream"])
	require.Equal(t, true, snapshot["upstream_stream"])
	require.Equal(t, "gemini-test", store.entry.Model)
}
