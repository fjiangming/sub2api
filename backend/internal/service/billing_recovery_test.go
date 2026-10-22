package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recoveryTestStore struct {
	BillingRecoveryStore
	began      int
	beginError error
	entry      *BillingRecoveryCase
	prepared   bool
}

func (s *recoveryTestStore) Begin(_ context.Context, c *BillingRecoveryCase) error {
	s.began++
	s.entry = c
	return s.beginError
}
func (s *recoveryTestStore) Prepare(_ context.Context, _ string, c *UsageBillingCommand, u *UsageLog) (string, error) {
	s.prepared = true
	return "persisted", nil
}

func TestBillingRecoveryDisconnectRetainsDeadlineAndBlocksNewDispatch(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &recoveryTestStore{}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	original, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, stop := BillingRecoveryRequest(original, &APIKey{ID: 2, User: &User{ID: 1}}, nil, "/v1/messages", true)
	defer stop()
	req, err := http.NewRequestWithContext(ctx, "POST", "https://upstream.invalid/messages", strings.NewReader(`{"model":"claude-test","messages":[{"content":"secret"}]}`))
	require.NoError(t, err)
	id, err := BeginBillingRecoveryAttempt(req, 3)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Equal(t, "claude-test", store.entry.Model)
	require.Equal(t, "dispatch:"+id, resolveUsageBillingRequestID(ctx, "upstream-id"))
	encoded, _ := json.Marshal(store.entry)
	require.NotContains(t, string(encoded), "secret")
	cancel() // Simulates the Cloudflare/client timeout after the request was dispatched.
	require.NoError(t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(30*time.Minute), deadline, time.Second)
	upstream, release := detachStreamUpstreamContext(ctx, false)
	defer release()
	require.NoError(t, upstream.Err())
	copied := CopyBillingRecoveryContext(ctx, context.Background())
	require.Equal(t, id, billingRecoveryAttemptID(copied))
	_, err = BeginBillingRecoveryAttempt(req, 3)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.began)
}

func TestBillingRecoveryJournalFailurePreventsDispatch(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &recoveryTestStore{beginError: errors.New("database unavailable")}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	ctx, cancel := BillingRecoveryRequest(context.Background(), &APIKey{ID: 2, User: &User{ID: 1}}, nil, "/v1/chat/completions", true)
	defer cancel()
	err := BeginBillingRecoveryOperation(ctx, 3, "gpt-test")
	require.ErrorContains(t, err, "persistence unavailable")
	require.Empty(t, billingRecoveryAttemptID(ctx))
}

func TestBillingRecoverySynchronousSubmissionEvenAfterPoolStop(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	ConfigureBillingRecovery(&recoveryTestStore{})
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	pool := &UsageRecordWorkerPool{taskTimeout: time.Millisecond}
	done := false
	mode := pool.Submit(func(ctx context.Context) {
		deadline, _ := ctx.Deadline()
		require.Greater(t, time.Until(deadline), 20*time.Second)
		done = true
	})
	require.True(t, done)
	require.Equal(t, UsageRecordSubmitModeSync, mode)
}

func TestBillingRecoveryCorrectionChargesOnlyVerifiedDifference(t *testing.T) {
	prior := &UsageLog{InputTokens: 100, OutputTokens: 20, TotalCost: 0.1, ActualCost: 0.2}
	target := &UsageLog{InputTokens: 100, OutputTokens: 80, TotalCost: 0.4, ActualCost: 0.8, RateMultiplier: 2}
	subtractRecoveryUsage(target, prior)
	require.NoError(t, validateRecoveryUsage(target))
	require.Equal(t, 60, target.OutputTokens)
	require.Equal(t, 0, target.InputTokens)
	require.Equal(t, 0.6, target.ActualCost)
	subtractRecoveryUsage(target, prior)
	require.Error(t, validateRecoveryUsage(target))
}

func TestBillingRecoveryPrepareDoesNotAcknowledgeBeforeCommit(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &recoveryTestStore{}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	cmd := &UsageBillingCommand{}
	require.NoError(t, prepareBillingRecovery(context.Background(), cmd, &UsageLog{}))
	require.True(t, store.prepared)
	require.Equal(t, "persisted", cmd.RecoveryCaseID)
	// CacheComplete is intentionally not implemented by this fake: calling it here would panic.
}

func TestBillingRecoveryPricingFailureRetainsUsageWithoutBilling(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &recoveryTestStore{}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	params := &postUsageBillingParams{Cost: &CostBreakdown{recoveryPricingMissing: true},
		User: &User{ID: 1}, APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3}}
	// The embedded nil interface panics if Apply is reached before pricing review.
	repo := &struct{ UsageBillingRepository }{}
	_, err := applyUsageBilling(context.Background(), "missing-price", &UsageLog{InputTokens: 100}, params, &billingDeps{}, repo)
	require.ErrorContains(t, err, "pricing unavailable")
	require.True(t, store.prepared, "the usage must be durable even though settlement is blocked")
	cmd := &UsageBillingCommand{}
	require.NoError(t, prepareBillingRecovery(context.Background(), cmd, &UsageLog{TotalCost: 0}), "an explicitly free price is not a pricing error")
}

type recoveryAccountRepo struct{ AccountRepository }

func (r recoveryAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return &Account{ID: 3, Type: AccountTypeAPIKey}, nil
}

type restoreTestStore struct {
	BillingRecoveryStore
	entry      *BillingRecoveryCase
	usage      *UsageLog
	command    *UsageBillingCommand
	correction bool
}

func (s *restoreTestStore) Get(context.Context, string) (*BillingRecoveryCase, error) {
	return s.entry, nil
}
func (s *restoreTestStore) Restore(_ context.Context, _ string, _ int, u *UsageLog, c *UsageBillingCommand, _ string, _ string) error {
	s.usage = u
	s.command = c
	return nil
}
func (s *restoreTestStore) Supplement(_ context.Context, _ string, _ int, u *UsageLog, c *UsageBillingCommand, _ string, _ string) (string, error) {
	s.usage = u
	s.command = c
	s.correction = true
	return "child", nil
}

func TestBillingRecoveryManualRecordPreservesOwnershipAndChargesDifference(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	at := time.Now().Add(-3 * time.Hour)
	store := &restoreTestStore{entry: &BillingRecoveryCase{ID: "case", Version: 2, State: "settled", UserID: 1, APIKeyID: 2, AccountID: 3, CreatedAt: at, UpdatedAt: at,
		Usage: &UsageLog{Model: "test", InputTokens: 100, OutputTokens: 10, TotalCost: 0.1, ActualCost: 0.2, RateMultiplier: 2}, Snapshot: json.RawMessage(`{"key_quota":true,"key_rate_limits":true}`)}}
	ConfigureBillingRecovery(store)
	t.Cleanup(func() { ConfigureBillingRecovery(nil) })
	gateway := &GatewayService{accountRepo: recoveryAccountRepo{}}
	record := &UsageLog{Model: "test", InputTokens: 100, OutputTokens: 50, TotalCost: 0.3, RateMultiplier: 2, UserID: 99}
	key := &APIKey{ID: 2, User: &User{ID: 1}}
	_, err := gateway.RestoreBillingRecoveryUsage(context.Background(), "case", 2, key, record, "historical price evidence", "admin:1")
	require.ErrorContains(t, err, "identity mismatch")
	record.UserID = 0
	id, err := gateway.RestoreBillingRecoveryUsage(context.Background(), "case", 2, key, record, "historical price evidence", "admin:1")
	require.NoError(t, err)
	require.Equal(t, "child", id)
	require.True(t, store.correction)
	require.Equal(t, 40, store.usage.OutputTokens)
	require.Equal(t, 0.4, store.command.BalanceCost)
	require.Equal(t, 0.4, store.command.APIKeyQuotaCost)
	require.Equal(t, 0.4, store.command.APIKeyRateLimitCost)
	require.Equal(t, int64(1), store.usage.UserID)
	require.Equal(t, at, store.usage.CreatedAt)
}
