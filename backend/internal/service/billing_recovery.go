package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/google/uuid"
)

// Fork-owned contract; recovery scheduling and reconciliation live in operations-center.
type BillingRecoveryCase struct {
	ID                string               `json:"id"`
	RequestRoot       string               `json:"request_root"`
	UserID            int64                `json:"user_id"`
	APIKeyID          int64                `json:"api_key_id"`
	GroupID           *int64               `json:"group_id"`
	SubscriptionID    *int64               `json:"subscription_id"`
	AccountID         int64                `json:"account_id"`
	Platform          string               `json:"platform"`
	Endpoint          string               `json:"endpoint"`
	Model             string               `json:"model"`
	UpstreamRequestID string               `json:"upstream_request_id"`
	State             string               `json:"state"`
	HTTPStatus        *int                 `json:"http_status"`
	ObservedUsage     json.RawMessage      `json:"observed_usage"`
	Usage             *UsageLog            `json:"usage_record"`
	Command           *UsageBillingCommand `json:"-"`
	Evidence          string               `json:"evidence"`
	LastError         string               `json:"last_error"`
	Attempts          int                  `json:"attempts"`
	Version           int                  `json:"version"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
	NextRetryAt       time.Time            `json:"next_retry_at"`
	CachePending      bool                 `json:"cache_pending"`
	ParentCaseID      *string              `json:"parent_case_id"`
	Snapshot          json.RawMessage      `json:"request_snapshot"`
}

type BillingRecoveryStore interface {
	Begin(context.Context, *BillingRecoveryCase) error
	Observe(context.Context, string, int, string, json.RawMessage, string) error
	Prepare(context.Context, string, *UsageBillingCommand, *UsageLog) (string, error)
	List(context.Context, string, int, int) ([]BillingRecoveryCase, int64, error)
	Get(context.Context, string) (*BillingRecoveryCase, error)
	Review(context.Context, string, int, string, string, string) error
	Restore(context.Context, string, int, *UsageLog, *UsageBillingCommand, string, string) error
	Failure(context.Context, string, string) error
	Events(context.Context, string) ([]map[string]any, error)
	CacheComplete(context.Context, string) error
	Supplement(context.Context, string, int, *UsageLog, *UsageBillingCommand, string, string) (string, error)
}

type billingRecoveryRuntime struct{ Store BillingRecoveryStore }

var billingRecovery atomic.Pointer[billingRecoveryRuntime]

func ConfigureBillingRecovery(store BillingRecoveryStore) {
	billingRecovery.Store(nil)
	if store != nil && !strings.EqualFold(os.Getenv("BILLING_RECOVERY_ENABLED"), "false") {
		billingRecovery.Store(&billingRecoveryRuntime{Store: store})
	}
}

func BillingRecovery() BillingRecoveryStore {
	if r := billingRecovery.Load(); r != nil {
		return r.Store
	}
	return nil
}

func BillingRecoveryEnabled() bool { return BillingRecovery() != nil }

type billingRequestKey struct{}
type billingRecoveryStreamKey struct{}
type billingRecoveryFilterKey struct{}

type BillingRecoveryListFilter struct {
	UserID, APIKeyID, AccountID int64
	TimeoutOnly                 bool
	RequestID                   string
	From, To                    *time.Time
}

func WithBillingRecoveryFilter(ctx context.Context, filter BillingRecoveryListFilter) context.Context {
	return context.WithValue(ctx, billingRecoveryFilterKey{}, filter)
}

func BillingRecoveryFilter(ctx context.Context) BillingRecoveryListFilter {
	filter, _ := ctx.Value(billingRecoveryFilterKey{}).(BillingRecoveryListFilter)
	return filter
}

func HasBillingRecoveryRequest(ctx context.Context) bool {
	_, ok := ctx.Value(billingRequestKey{}).(*billingRequestState)
	return ok
}

func BillingRecoveryClientDisconnected(ctx context.Context) bool {
	state, _ := ctx.Value(billingRequestKey{}).(*billingRequestState)
	return state != nil && state.Original.Err() != nil
}

func WithBillingRecoveryStream(ctx context.Context, protocol string) context.Context {
	return context.WithValue(ctx, billingRecoveryStreamKey{}, protocol)
}

type billingRequestState struct {
	Original                context.Context
	Root                    string
	UserID, KeyID           int64
	GroupID, SubscriptionID *int64
	Platform, Endpoint      string
	Snapshot                json.RawMessage
	LastAttempt             atomic.Pointer[string]
	Ingress                 atomic.Pointer[recoveryIngress]
	Key                     *APIKey
	StartedAt               time.Time
}

func BillingRecoveryTimeout() time.Duration {
	seconds, err := strconv.Atoi(os.Getenv("BILLING_RECOVERY_TIMEOUT_SECONDS"))
	if err != nil || seconds < 30 || seconds > 7200 {
		seconds = 1800
	}
	return time.Duration(seconds) * time.Second
}

// Protect the whole response-body lifecycle; retain the original context to reject new dispatches.
func BillingRecoveryRequest(ctx context.Context, key *APIKey, subscription *UserSubscription, endpoint string, detach bool) (context.Context, context.CancelFunc) {
	if !BillingRecoveryEnabled() || key == nil || key.User == nil {
		return ctx, func() {}
	}
	state := &billingRequestState{Original: ctx, Root: uuid.NewString(), UserID: key.User.ID, KeyID: key.ID,
		GroupID: key.GroupID, Platform: PlatformFromAPIKey(key), Endpoint: endpoint, Key: key, StartedAt: time.Now()}
	state.Snapshot, _ = json.Marshal(map[string]any{"key_quota": key.Quota > 0, "key_rate_limits": key.HasRateLimits(),
		"request_id": ctx.Value(ctxkey.RequestID), "client_request_id": ctx.Value(ctxkey.ClientRequestID),
		"recovery_timeout_seconds": int(BillingRecoveryTimeout().Seconds())})
	if subscription != nil {
		state.SubscriptionID = &subscription.ID
	}
	base := ctx
	cancel := func() {}
	if detach {
		base, cancel = context.WithTimeout(context.WithoutCancel(ctx), BillingRecoveryTimeout())
	}
	return context.WithValue(base, billingRequestKey{}, state), cancel
}

func CopyBillingRecoveryContext(parent, target context.Context) context.Context {
	if parent == nil {
		return target
	}
	if state, ok := parent.Value(billingRequestKey{}).(*billingRequestState); ok {
		return context.WithValue(target, billingRequestKey{}, state)
	}
	return target
}

func BeginBillingRecoveryAttempt(req *http.Request, accountID int64) (string, error) {
	store := BillingRecovery()
	if store == nil || req == nil {
		return "", nil
	}
	state, _ := req.Context().Value(billingRequestKey{}).(*billingRequestState)
	if state == nil {
		return "", nil
	}
	if err := state.Original.Err(); err != nil {
		return "", err
	}
	if err := req.Context().Err(); err != nil {
		return "", err
	}
	id := uuid.NewString()
	if req.URL != nil && !strings.HasPrefix(recoveryRequestHeader(req.Header, "authorization"), "AWS4-") {
		req.Header = req.Header.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		for _, name := range []string{"X-Request-ID", "X-Client-Request-ID"} {
			value := recoveryRequestHeader(req.Header, name)
			last := state.LastAttempt.Load()
			if value == "" || last != nil && value == *last {
				for key := range req.Header {
					if strings.EqualFold(key, name) {
						delete(req.Header, key)
					}
				}
				req.Header.Set(name, id)
			}
		}
	}
	model := ""
	stream := false
	if req.GetBody != nil {
		if body, err := req.GetBody(); err == nil {
			var fields struct {
				Model  string `json:"model"`
				Stream bool   `json:"stream"`
			}
			// The decoder only retains model; no prompt or credentials enter the journal.
			_ = json.NewDecoder(http.MaxBytesReader(nil, body, 4<<20)).Decode(&fields)
			_ = body.Close()
			model = fields.Model
			stream = fields.Stream
		}
	}
	if model == "" && req.URL != nil {
		if index := strings.LastIndex(req.URL.Path, "/models/"); index >= 0 {
			model = strings.SplitN(req.URL.Path[index+len("/models/"):], ":", 2)[0]
		}
	}
	if req.URL != nil && strings.HasSuffix(req.URL.Path, ":streamGenerateContent") {
		stream = true
	}
	ctx, cancel := detachedBillingContext(req.Context())
	defer cancel()
	entry := &BillingRecoveryCase{ID: id, RequestRoot: state.Root, UserID: state.UserID, APIKeyID: state.KeyID,
		GroupID: state.GroupID, SubscriptionID: state.SubscriptionID, AccountID: accountID,
		Platform: state.Platform, Endpoint: state.Endpoint, Model: model}
	var snapshot map[string]any
	_ = json.Unmarshal(state.Snapshot, &snapshot)
	if snapshot == nil {
		snapshot = map[string]any{}
	}
	snapshot["upstream_stream"] = stream
	if req.URL != nil {
		snapshot["upstream_path"] = req.URL.Path
	}
	snapshot["downstream_stream"] = stream
	if ingress := state.Ingress.Load(); ingress != nil {
		snapshot["downstream_stream"] = ingress.Stream || strings.HasSuffix(state.Endpoint, ":streamGenerateContent")
	}
	if protocol, _ := req.Context().Value(billingRecoveryStreamKey{}).(string); protocol != "" {
		snapshot["sync_stream_protocol"] = protocol
		snapshot["downstream_stream"] = false
		snapshot["upstream_stream"] = true
	}
	// Restrict correlation metadata to bounded identifiers; never persist auth headers.
	ids := map[string]string{}
	for _, name := range []string{"x-request-id", "x-client-request-id", "request-id"} {
		if value := strings.TrimSpace(recoveryRequestHeader(req.Header, name)); value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\r\n") {
			ids[name] = value
		}
	}
	snapshot["sent_request_ids"] = ids
	snapshot["dispatch_correlation_id"] = id
	if pricing := snapshotRecoveryPrice(ctx, state, model, req); pricing != nil {
		snapshot["pricing"] = pricing
	}
	entry.Snapshot, _ = json.Marshal(snapshot)
	if err := store.Begin(ctx, entry); err != nil {
		return "", fmt.Errorf("billing recovery persistence unavailable: %w", err)
	}
	state.LastAttempt.Store(&id)
	if err := state.Original.Err(); err != nil {
		return "", err
	}
	return id, nil
}

func recoveryRequestHeader(header http.Header, name string) string {
	for key, values := range header {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func billingRecoveryAttemptID(ctx context.Context) string {
	if state, ok := ctx.Value(billingRequestKey{}).(*billingRequestState); ok {
		if id := state.LastAttempt.Load(); id != nil {
			return *id
		}
	}
	return ""
}

// Non-HTTP transports persist the same dispatch intent before sending a billable turn.
func BeginBillingRecoveryOperation(ctx context.Context, accountID int64, model string) error {
	data, _ := json.Marshal(map[string]string{"model": model})
	req := &http.Request{GetBody: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(string(data))), nil }}
	_, err := BeginBillingRecoveryAttempt(req.WithContext(ctx), accountID)
	return err
}

func prepareBillingRecovery(ctx context.Context, cmd *UsageBillingCommand, usage *UsageLog) error {
	store := BillingRecovery()
	if store == nil {
		return nil
	}
	id, err := store.Prepare(ctx, billingRecoveryAttemptID(ctx), cmd, usage)
	if err != nil {
		return err
	}
	cmd.RecoveryCaseID, cmd.RecoveryUsage = id, usage
	if cmd.RecoveryPricingMissing {
		return errors.New("historical pricing unavailable; usage retained for billing review")
	}
	return nil
}

var ErrBillingRecoveryConflict = errors.New("billing recovery version or state conflict")

// Execute only a persisted, priced usage record. Operators never submit a naked balance delta.
func (s *GatewayService) SettleBillingRecovery(ctx context.Context, id string, version int) error {
	store := BillingRecovery()
	if store == nil {
		return errors.New("billing recovery is disabled")
	}
	entry, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	if entry.Command == nil || entry.Usage == nil {
		return ErrBillingRecoveryConflict
	}
	if entry.State != "settled" && (entry.Version != version || entry.State != "prepared") {
		return ErrBillingRecoveryConflict
	}
	cmd := entry.Command
	cmd.RecoveryCaseID, cmd.RecoveryUsage, cmd.RecoveryReplay = id, entry.Usage, true
	if entry.State != "settled" {
		_, err = s.usageBillingRepo.Apply(ctx, cmd)
		if err != nil {
			failCtx, cancel := detachedBillingContext(ctx)
			defer cancel()
			_ = store.Failure(failCtx, id, "SETTLEMENT_FAILED")
			return err
		}
	}
	// Invalidations are repeatable, including after a committed transaction lost its receipt.
	if err := invalidateRecoveryCaches(ctx, s.billingCacheService, entry.Usage); err != nil {
		_ = store.Failure(ctx, id, "CACHE_INVALIDATION_FAILED")
		return err
	}
	return store.CacheComplete(ctx, id)
}

func invalidateRecoveryCaches(ctx context.Context, cache *BillingCacheService, usage *UsageLog) error {
	if cache == nil || usage == nil {
		return nil
	}
	if err := cache.InvalidateUserBalance(ctx, usage.UserID); err != nil {
		return err
	}
	if err := cache.InvalidateAPIKeyRateLimit(ctx, usage.APIKeyID); err != nil {
		return err
	}
	if usage.GroupID != nil && usage.SubscriptionID != nil {
		return cache.InvalidateSubscription(ctx, usage.UserID, *usage.GroupID)
	}
	return nil
}
