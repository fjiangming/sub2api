package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type importHandlerStore struct {
	service.BillingRecoveryStore
	input  *service.BillingRecoveryImport
	filter service.BillingRecoveryListFilter
}

func (s *importHandlerStore) ReserveImport(_ context.Context, in *service.BillingRecoveryImport, _ string) (string, error) {
	s.input = in
	return "reserved", nil
}
func (s *importHandlerStore) List(ctx context.Context, _ string, _, _ int) ([]service.BillingRecoveryCase, int64, error) {
	s.filter = service.BillingRecoveryFilter(ctx)
	return []service.BillingRecoveryCase{}, 0, nil
}

type importGroupHandlerStore struct {
	importHandlerStore
	group *service.BillingRecoveryImportGroup
}

func (s *importGroupHandlerStore) ReserveImportGroup(_ context.Context, in *service.BillingRecoveryImportGroup, _ string) ([]service.BillingRecoveryImportClaim, error) {
	s.group = in
	return []service.BillingRecoveryImportClaim{{CaseID: "original-1"}, {CaseID: "original-2"}}, nil
}

func (s *importGroupHandlerStore) ImportClaims(_ context.Context, _ int64, from, to time.Time) ([]service.BillingRecoveryImportClaim, error) {
	if !to.After(from) {
		return nil, service.ErrBillingRecoveryConflict
	}
	return []service.BillingRecoveryImportClaim{{CaseID: "original-1"}}, nil
}

func TestBillingRecoveryImportGroupHTTPValidationAndUnavailableStore(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	store := &importGroupHandlerStore{}
	service.ConfigureBillingRecovery(store)
	router := gin.New()
	RegisterBillingRecoveryRoutes(router.Group("/admin"), nil)
	in := service.BillingRecoveryImportGroup{Identity: strings.Repeat("a", 64), PayloadDigest: strings.Repeat("b", 64),
		Members: make([]service.BillingRecoveryImportGroupMember, 2)}
	valid, err := json.Marshal(in)
	require.NoError(t, err)
	for _, body := range []string{`{}`, strings.Replace(string(valid), strings.Repeat("a", 64), "invalid", 1), string(valid)} {
		response := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/billing-recovery/imports/reserve-group", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(response, req)
		if body == string(valid) {
			require.Equal(t, 200, response.Code)
			require.Equal(t, in.Identity, store.group.Identity)
		} else {
			require.Equal(t, 400, response.Code)
			require.Nil(t, store.group)
		}
	}
	for _, query := range []string{"account_id=bad", "account_id=3&from=invalid&to=invalid", "account_id=3&from=2026-10-22T00:00:00Z&to=2026-10-23T00:00:00Z"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/billing-recovery/imports/claims?"+query, nil))
		if strings.Contains(query, "2026-10-22") {
			require.Equal(t, 200, response.Code)
			require.Contains(t, response.Body.String(), "original-1")
		} else {
			require.Equal(t, 400, response.Code)
		}
	}
	service.ConfigureBillingRecovery(&importHandlerStore{})
	for _, route := range []struct{ method, path string }{{http.MethodPost, "/imports/reserve-group"}, {http.MethodGet, "/imports/claims"}} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(route.method, "/admin/billing-recovery"+route.path, nil))
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
	}
}

func TestBillingRecoveryHTTPRegistrationWithoutGateway(t *testing.T) {
	service.ConfigureBillingRecovery(nil)
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	router := gin.New()
	require.NotPanics(t, func() {
		RegisterBillingRecoveryRoutes(router.Group("/admin"), nil)
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/billing-recovery", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}

func TestBillingRecoveryImportHTTPValidatesBeforeReservation(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &importHandlerStore{}
	service.ConfigureBillingRecovery(store)
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	router := gin.New()
	RegisterBillingRecoveryRoutes(router.Group("/admin"), &GatewayHandler{})
	in := service.BillingRecoveryImport{AccountID: 3, BillIdentity: strings.Repeat("a", 64), PayloadDigest: strings.Repeat("b", 64),
		UserID: 1, APIKeyID: 2, ErrorID: 10, EventIndex: -1, StartedAt: time.Now().Add(-4 * time.Hour), Model: "test", Evidence: "verified historical CSV"}
	valid, err := json.Marshal(in)
	require.NoError(t, err)
	for _, body := range []string{`{}`, strings.Replace(string(valid), strings.Repeat("a", 64), "invalid", 1), string(valid)} {
		response := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/admin/billing-recovery/imports/reserve", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(response, req)
		if body == string(valid) {
			require.Equal(t, 200, response.Code)
			require.NotNil(t, store.input)
			require.Equal(t, in.BillIdentity, store.input.BillIdentity)
			require.Equal(t, -1, store.input.EventIndex)
		} else {
			require.Equal(t, 400, response.Code)
			require.Nil(t, store.input)
		}
	}
}

func TestBillingRecoveryImportQuoteHTTPRejectsInvalidInputAndMissingGateway(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &importHandlerStore{}
	service.ConfigureBillingRecovery(store)
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	in := service.BillingRecoveryImport{AccountID: 3, BillIdentity: strings.Repeat("a", 64), PayloadDigest: strings.Repeat("b", 64),
		UserID: 1, APIKeyID: 2, ErrorID: 10, EventIndex: -1, StartedAt: time.Now().Add(-4 * time.Hour), Model: "test", Evidence: "verified historical CSV"}
	valid, err := json.Marshal(map[string]any{"source": in, "tokens": service.UsageTokens{InputTokens: 1000}})
	require.NoError(t, err)
	for _, gateway := range []*GatewayHandler{nil, {}} {
		router := gin.New()
		RegisterBillingRecoveryRoutes(router.Group("/admin"), gateway)
		for _, body := range []string{`{}`, `{"source":{"api_key_id":2}}`, string(valid)} {
			response := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/admin/billing-recovery/imports/quote", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(response, req)
			if body == string(valid) {
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
			} else {
				require.Equal(t, http.StatusBadRequest, response.Code)
			}
			require.Nil(t, store.input, "quotes must never reserve a source")
		}
	}
}

func TestBillingRecoveryHTTPDateFilterPreservesNanoseconds(t *testing.T) {
	t.Setenv("BILLING_RECOVERY_ENABLED", "true")
	store := &importHandlerStore{}
	service.ConfigureBillingRecovery(store)
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	router := gin.New()
	RegisterBillingRecoveryRoutes(router.Group("/admin"), &GatewayHandler{})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/admin/billing-recovery?from=2026-10-22T01:08:08.123456789Z&to=2026-10-23T00:00:00Z&account_id=356", nil))
	require.Equal(t, 200, response.Code)
	require.EqualValues(t, 356, store.filter.AccountID)
	require.NotNil(t, store.filter.From)
	require.Equal(t, 123456789, store.filter.From.Nanosecond())
	require.Equal(t, "2026-10-23T00:00:00Z", store.filter.To.Format(time.RFC3339))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/billing-recovery?from=invalid", nil))
	require.Equal(t, 400, response.Code)
}
