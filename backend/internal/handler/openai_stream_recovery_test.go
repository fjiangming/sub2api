package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func recoveryContext(body, previous string, optIn bool) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	if optIn {
		c.Request.Header.Set(openAIRecoveryHeader, "1")
	}
	c.Request.Header.Set(openAIRecoveryFromHeader, previous)
	return c, w
}

func TestOpenAIStreamRecoveryScopesHintsToAPIKeyGroupAndExactBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &OpenAIGatewayHandler{streamRecovery: newOpenAIStreamRecoveryCache(16, time.Now)}
	groupID := int64(79)
	apiKey := &service.APIKey{ID: 527, GroupID: &groupID}
	body := `{"model":"unchanged","input":"private prompt","reasoning":{"effort":"max"},"max_output_tokens":131072,"stream":true}`
	c, w := recoveryContext(body, "", true)
	first := h.beginOpenAIStreamRecovery(c, apiKey, []byte(body))
	first.recordFailure(360)
	token := w.Header().Get(openAIRecoveryTokenHeader)
	require.NoError(t, uuid.Validate(token))
	require.Empty(t, c.Request.Header.Get(openAIRecoveryHeader))
	require.Empty(t, c.Request.Header.Get(openAIRecoveryFromHeader))

	for _, change := range []string{"none", "key", "group", "prompt", "model", "reasoning", "tokens"} {
		t.Run(change, func(t *testing.T) {
			key := *apiKey
			otherGroup := groupID + 1
			requestBody := body
			switch change {
			case "key":
				key.ID++
			case "group":
				key.GroupID = &otherGroup
			case "prompt":
				requestBody = strings.Replace(body, "private prompt", "other prompt", 1)
			case "model":
				requestBody = strings.Replace(body, "unchanged", "other-model", 1)
			case "reasoning":
				requestBody = strings.Replace(body, "max", "high", 1)
			case "tokens":
				requestBody = strings.Replace(body, "131072", "512", 1)
			}
			c, w := recoveryContext(requestBody, token, true)
			r := h.beginOpenAIStreamRecovery(c, &key, []byte(requestBody))
			if change == "none" {
				require.Equal(t, map[int64]struct{}{360: {}}, r.excludedAccounts())
				require.Equal(t, "1", w.Header().Get(openAIRecoveryAppliedHeader))
			} else {
				require.Empty(t, r.excludedAccounts())
				require.Equal(t, "0", w.Header().Get(openAIRecoveryAppliedHeader))
			}
		})
	}
}

func TestOpenAIStreamRecoveryAccumulatesFailuresAndClearsSuccessfulRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &OpenAIGatewayHandler{streamRecovery: newOpenAIStreamRecoveryCache(16, time.Now)}
	groupID := int64(79)
	key := &service.APIKey{ID: 527, GroupID: &groupID}
	var tokens []string
	for _, accountID := range []int64{360, 391} {
		c, w := recoveryContext("same-body", strings.Join(tokens, ","), true)
		r := h.beginOpenAIStreamRecovery(c, key, []byte("same-body"))
		r.recordFailure(accountID)
		tokens = append(tokens, w.Header().Get(openAIRecoveryTokenHeader))
	}
	c, w := recoveryContext("same-body", strings.Join(tokens, ","), true)
	r := h.beginOpenAIStreamRecovery(c, key, []byte("same-body"))
	require.Equal(t, map[int64]struct{}{360: {}, 391: {}}, r.excludedAccounts())
	r.recordFailure(999)
	r.forgetFailure()
	c, _ = recoveryContext("same-body", w.Header().Get(openAIRecoveryTokenHeader), true)
	require.Empty(t, h.beginOpenAIStreamRecovery(c, key, []byte("same-body")).excludedAccounts())
}

func TestOpenAIStreamRecoveryIgnoresMissingOptInInvalidAndOversizedHints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &OpenAIGatewayHandler{streamRecovery: newOpenAIStreamRecoveryCache(16, time.Now)}
	groupID := int64(79)
	key := &service.APIKey{ID: 527, GroupID: &groupID}
	for _, previous := range []string{"", "360", "bad-token", uuid.NewString(), strings.Repeat("x", 200), strings.Repeat(uuid.NewString()+",", 4)} {
		c, _ := recoveryContext("same-body", previous, true)
		require.Empty(t, h.beginOpenAIStreamRecovery(c, key, []byte("same-body")).excludedAccounts())
	}
	c, w := recoveryContext("same-body", "360", false)
	r := h.beginOpenAIStreamRecovery(c, key, []byte("same-body"))
	require.Nil(t, r)
	require.Empty(t, r.excludedAccounts())
	require.Empty(t, w.Header().Get(openAIRecoveryTokenHeader))
	require.Empty(t, c.Request.Header.Get(openAIRecoveryFromHeader))
	r.recordFailure(360)
	require.Empty(t, h.streamRecovery.entries)
}

func TestOpenAIStreamRecoveryCacheExpiresAndRemainsBounded(t *testing.T) {
	now := time.Unix(100, 0)
	s := newOpenAIStreamRecoveryCache(2, func() time.Time { return now })
	a := openAIRecoveryKey{token: "a"}
	b := openAIRecoveryKey{token: "b"}
	c := openAIRecoveryKey{token: "c"}
	s.record(a, 360)
	s.record(b, 391)
	require.EqualValues(t, 360, s.account(a))
	s.record(c, 999)
	require.Zero(t, s.account(b))
	require.Len(t, s.entries, 2)
	now = now.Add(openAIRecoveryTTL)
	require.Zero(t, s.account(a))
	require.Zero(t, s.account(c))
	require.Empty(t, s.entries)
}

func TestOpenAIStreamRecoveryCacheConcurrentAccess(t *testing.T) {
	s := newOpenAIStreamRecoveryCache(16, time.Now)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := openAIRecoveryKey{token: fmt.Sprint(i)}
			s.record(key, int64(i+1))
			s.account(key)
		}(i)
	}
	wg.Wait()
	require.LessOrEqual(t, len(s.entries), 16)
}

type streamRecoveryUpstream struct {
	service.HTTPUpstream
	accountIDs    []int64
	headers       []http.Header
	failSecond    bool
	cancelOnClose context.CancelFunc
}

type streamRecoveryBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (b streamRecoveryBody) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return nil
}

func (s *streamRecoveryUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	s.accountIDs = append(s.accountIDs, accountID)
	s.headers = append(s.headers, req.Header.Clone())
	events := []string{
		`{"type":"response.created","response":{"id":"resp_recovery"}}`,
		`{"type":"response.output_text.delta","delta":"partial"}`,
		`{"type":"error","code":"stream_timeout","message":"Upstream response exceeded the maximum stream duration"}`,
	}
	if accountID == 391 && !s.failSecond {
		events = []string{
			`{"type":"response.output_text.delta","delta":"complete answer"}`,
			`{"type":"response.completed","response":{"id":"resp_complete","status":"completed","output_text":"complete answer"}}`,
		}
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}},
		Body: streamRecoveryBody{Reader: strings.NewReader("data: " + strings.Join(events, "\n\ndata: ") + "\n\n"), cancel: s.cancelOnClose}}, nil
}

func TestOpenAIStreamRecoveryResponsesUsesNativeSchedulerToAvoidFailedAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(79)
	accounts := []service.Account{}
	for i, id := range []int64{360, 391} {
		accounts = append(accounts, service.Account{
			ID: id, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Status: service.StatusActive, Schedulable: true, Priority: i + 1,
			Credentials: map[string]any{"api_key": "sk-synthetic", "base_url": "https://synthetic.invalid"},
			Extra:       map[string]any{"openai_passthrough": true, "openai_responses_supported": true},
		})
	}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	upstream := &streamRecoveryUpstream{}
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	gateway := service.NewOpenAIGatewayService(
		&openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
	)
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	key := &service.APIKey{ID: 527, GroupID: &groupID, User: &service.User{ID: 1, Status: service.StatusActive},
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive}}
	body := `{"model":"gpt-5.2","input":"same question","reasoning":{"effort":"high"},"max_output_tokens":512,"stream":true}`
	call := func(previous string) *httptest.ResponseRecorder {
		c, w := recoveryContext(body, previous, true)
		// The detector closes its stream as soon as the error event is accepted.
		// Keep the failed-account hint even if that cancellation wins the race.
		upstream.cancelOnClose = nil
		if len(upstream.accountIDs) == 0 {
			ctx, cancel := context.WithCancel(c.Request.Context())
			defer cancel()
			c.Request = c.Request.WithContext(ctx)
			upstream.cancelOnClose = cancel
		}
		c.Request.Header.Set("Content-Type", "application/json")
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 0})
		h.Responses(c)
		return w
	}
	first := call("")
	require.Contains(t, first.Body.String(), "maximum stream duration")
	require.Equal(t, []int64{360}, upstream.accountIDs)
	second := call(first.Header().Get(openAIRecoveryTokenHeader))
	require.Equal(t, "1", second.Header().Get(openAIRecoveryAppliedHeader))
	require.Contains(t, second.Body.String(), "resp_complete")
	require.NotContains(t, second.Body.String(), "maximum stream duration")
	require.Equal(t, []int64{360, 391}, upstream.accountIDs)
	for _, headers := range upstream.headers {
		for _, name := range []string{openAIRecoveryHeader, openAIRecoveryFromHeader, openAIRecoveryTokenHeader} {
			require.Empty(t, headers.Get(name))
		}
	}
	// A hint can never make a previously successful request exclude its account.
	c, _ := recoveryContext(body, second.Header().Get(openAIRecoveryTokenHeader), true)
	require.Empty(t, h.beginOpenAIStreamRecovery(c, key, []byte(body)).excludedAccounts())
	upstream.failSecond = true
	failedSecond := call(first.Header().Get(openAIRecoveryTokenHeader))
	require.Contains(t, failedSecond.Body.String(), "maximum stream duration")
	exhausted := call(first.Header().Get(openAIRecoveryTokenHeader) + "," + failedSecond.Header().Get(openAIRecoveryTokenHeader))
	require.GreaterOrEqual(t, exhausted.Code, 400)
	require.Equal(t, []int64{360, 391, 391}, upstream.accountIDs)
}
