package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func recoveryCommand() *service.UsageBillingCommand {
	cmd := &service.UsageBillingCommand{RequestID: "recover-1", UserID: 1, APIKeyID: 2, AccountID: 3, BalanceCost: 0.25, RecoveryCaseID: "case-1", RecoveryUsage: &service.UsageLog{UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: "recover-1", Model: "test", ActualCost: 0.25, TotalCost: 0.25, RateMultiplier: 1, CreatedAt: time.Now()}}
	cmd.Normalize()
	return cmd
}
func expectRecoveryStage(mock sqlmock.Sqlmock, cmd *service.UsageBillingCommand, fail bool) {
	mock.ExpectQuery("SELECT state,billing_command").WithArgs(cmd.RecoveryCaseID).WillReturnRows(sqlmock.NewRows([]string{"state", "fingerprint"}).AddRow("prepared", cmd.RequestFingerprint))
	query := mock.ExpectQuery("INSERT INTO usage_logs")
	if fail {
		query.WillReturnError(errors.New("usage insert failed"))
		return
	}
	query.WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(10, time.Now()))
	mock.ExpectExec("UPDATE usage_logs SET actual_cost").WillReturnResult(sqlmock.NewResult(0, 1))
}
func TestBillingRecoveryRecordFailureRollsBackDedupAndMoney(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	cmd := recoveryCommand()
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO usage_billing_dedup").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery("SELECT request_fingerprint.*usage_billing_dedup_archive").WillReturnError(sql.ErrNoRows)
	expectRecoveryStage(mock, cmd, true)
	mock.ExpectRollback()
	_, err = (&usageBillingRepository{db: db}).Apply(context.Background(), cmd)
	require.ErrorContains(t, err, "usage insert failed")
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestBillingRecoveryDuplicateCompletesRecordWithoutSecondCharge(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	cmd := recoveryCommand()
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO usage_billing_dedup").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT request_fingerprint").WillReturnRows(sqlmock.NewRows([]string{"fingerprint"}).AddRow(cmd.RequestFingerprint))
	expectRecoveryStage(mock, cmd, false)
	mock.ExpectExec("UPDATE sub2api_custom.billing_recovery SET state='settled'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO auth_cache_invalidation_outbox").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	result, err := (&usageBillingRepository{db: db}).Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingRecoveryMissingPricingIsPersistedForReview(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	cmd := recoveryCommand()
	cmd.RecoveryPricingMissing = true
	mock.ExpectQuery("SELECT id,billing_command").WithArgs(cmd.APIKeyID, cmd.RequestID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("UPDATE sub2api_custom.billing_recovery SET native_request_id").
		WithArgs("case-1", cmd.RequestID, sqlmock.AnyArg(), sqlmock.AnyArg(), cmd.Model, cmd.UserID, cmd.APIKeyID, cmd.AccountID, nil, nil, nil, "review", "PRICING_UNAVAILABLE").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id,billing_command").WithArgs(cmd.APIKeyID, cmd.RequestID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "fingerprint"}).AddRow("case-1", cmd.RequestFingerprint))
	id, err := (&billingRecoveryRepository{db: db}).Prepare(context.Background(), "case-1", cmd, cmd.RecoveryUsage)
	require.NoError(t, err)
	require.Equal(t, "case-1", id)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingRecoveryReadsNullableJSON(t *testing.T) {
	at := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	snapshot := `{"account_type":"apikey","account_quota":false}`
	for _, fixture := range []struct {
		name, state, filter, observedJSON string
		observed, usage, command          driver.Value
	}{
		{name: "awaiting response", state: "unknown", filter: "unknown"},
		{name: "prepared without HTTP observation", state: "prepared", filter: "pending",
			usage: []byte(`{"Model":"test","ActualCost":0.25}`), command: []byte(`{"RequestID":"recover-1"}`)},
		{name: "observed usage", state: "unknown", filter: "",
			observed: []byte(`{"input_tokens":13}`), observedJSON: `{"input_tokens":13}`},
		{name: "JSON null observation", state: "unknown", filter: "unknown",
			observed: []byte(`null`), observedJSON: `null`},
	} {
		for _, operation := range []string{"get", "list"} {
			t.Run(fixture.name+"/"+operation, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				rows := sqlmock.NewRows(strings.Split(recoveryColumns, ",")).AddRow(
					"case-1", "root-1", int64(1), int64(2), nil, nil, int64(3), "anthropic", "/v1/messages", "test", "", fixture.state,
					nil, fixture.observed, fixture.usage, fixture.command, "", "", 0, 1, at, at, at, true, nil, []byte(snapshot))
				repo := &billingRecoveryRepository{db: db}
				var result *service.BillingRecoveryCase
				if operation == "get" {
					mock.ExpectQuery("SELECT id,request_root").WithArgs("case-1").WillReturnRows(rows)
					result, err = repo.Get(context.Background(), "case-1")
					require.NoError(t, err)
				} else {
					mock.ExpectQuery("SELECT count").WithArgs(fixture.filter).
						WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
					mock.ExpectQuery("SELECT id,request_root").WithArgs(fixture.filter, 20, 0).WillReturnRows(rows)
					items, total, listErr := repo.List(context.Background(), fixture.filter, 20, 0)
					require.NoError(t, listErr)
					require.EqualValues(t, 1, total)
					require.Len(t, items, 1)
					result = &items[0]
				}
				require.Equal(t, fixture.state, result.State)
				require.Nil(t, result.HTTPStatus)
				if fixture.observedJSON == "" {
					require.Nil(t, result.ObservedUsage)
				} else {
					require.JSONEq(t, fixture.observedJSON, string(result.ObservedUsage))
				}
				require.JSONEq(t, snapshot, string(result.Snapshot))
				if fixture.usage == nil {
					require.Nil(t, result.Usage)
					require.Nil(t, result.Command)
				} else {
					require.NotNil(t, result.Usage)
					require.Equal(t, "test", result.Usage.Model)
					require.InDelta(t, 0.25, result.Usage.ActualCost, 0)
					require.NotNil(t, result.Command)
					require.Equal(t, "recover-1", result.Command.RequestID)
				}
				_, err = json.Marshal(result)
				require.NoError(t, err)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

type observationStore struct {
	service.BillingRecoveryStore
	data json.RawMessage
	code string
}

type unavailableRecoveryStore struct{ service.BillingRecoveryStore }

func (s unavailableRecoveryStore) Begin(context.Context, *service.BillingRecoveryCase) error {
	return errors.New("journal offline")
}
func TestBillingRecoveryTransportCannotSendWithoutDurableIntent(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	service.ConfigureBillingRecovery(unavailableRecoveryStore{})
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	sent := 0
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sent++ }))
	defer target.Close()
	ctx, cancel := service.BillingRecoveryRequest(context.Background(), &service.APIKey{ID: 2, User: &service.User{ID: 1}}, nil, "/v1/messages", true)
	defer cancel()
	for _, tls := range []bool{false, true} {
		req, err := http.NewRequestWithContext(ctx, "POST", target.URL, strings.NewReader(`{"model":"test"}`))
		require.NoError(t, err)
		upstream := NewHTTPUpstream(nil)
		if tls {
			_, err = upstream.DoWithTLS(req, "", 3, 1, nil)
		} else {
			_, err = upstream.Do(req, "", 3, 1)
		}
		require.ErrorContains(t, err, "persistence unavailable")
	}
	require.Zero(t, sent)
}

func (s *observationStore) Observe(_ context.Context, _ string, _ int, _ string, data json.RawMessage, code string) error {
	s.data = data
	s.code = code
	return nil
}
func TestBillingRecoveryObserverCapturesProviderUsageWithoutContent(t *testing.T) {
	for _, fixture := range []struct{ name, body, content string }{
		{"claude", `data: {"message":{"usage":{"input_tokens":13}},"content":"secret"}` + "\n" + `data: {"usage":{"output_tokens":21}}` + "\n", "text/event-stream"},
		{"sse-final-line", `data: {"usage":{"input_tokens":13,"output_tokens":21}}`, "text/event-stream"},
		{"openai", `{"choices":[{"text":"secret"}],"usage":{"prompt_tokens":13,"completion_tokens":21,"details":{"cached_tokens":3,"secret":"secret"}}}`, "application/json"},
		{"gemini", `{"text":"secret","usageMetadata":{"promptTokenCount":13,"candidatesTokenCount":21}}`, "application/json"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Setenv("BILLING_RECOVERY_ENABLED", "true")
			store := &observationStore{}
			service.ConfigureBillingRecovery(store)
			defer service.ConfigureBillingRecovery(nil)
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{fixture.content}}, Body: io.NopCloser(strings.NewReader(fixture.body))}
			observeBillingRecoveryResponse("case", resp, nil)
			_, err := io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Contains(t, string(store.data), "13")
			require.Contains(t, string(store.data), "21")
			require.NotContains(t, string(store.data), "secret")
			require.Empty(t, store.code)
		})
	}
}

func TestBillingRecoverySubscriptionUsesNativeCalendarAndOneTimeWindows(t *testing.T) {
	start := time.Date(2026, 10, 22, 23, 0, 0, 0, timezone.Location())
	sub := &service.UserSubscription{StartsAt: start, ExpiresAt: start.Add(30 * 24 * time.Hour), DailyWindowStart: &start, WeeklyWindowStart: &start, MonthlyWindowStart: &start}
	daily, weekly, monthly := recoverySubscriptionWindows(sub, start.Add(2*time.Hour))
	require.False(t, daily)
	require.True(t, weekly)
	require.True(t, monthly)
	sub.ExpiresAt = start.Add(24 * time.Hour)
	daily, _, _ = recoverySubscriptionWindows(sub, start.Add(2*time.Hour))
	require.True(t, daily, "one-time daily quota lasts until native expiry")
}
