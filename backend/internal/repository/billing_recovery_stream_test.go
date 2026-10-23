package repository

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func recoverySSE(events ...string) string {
	return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
}

type streamRecoveryStore struct {
	observationStore
	entry *service.BillingRecoveryCase
}

func (s *streamRecoveryStore) Begin(_ context.Context, entry *service.BillingRecoveryCase) error {
	s.entry = entry
	return nil
}

func TestBillingRecoverySyncBridgeDrainsAllTextProtocolsAfterDisconnect(t *testing.T) {
	for _, fixture := range []struct{ name, path, events, tokenPath, contentPath, content string }{
		{"anthropic", "/v1/messages", recoverySSE(
			`{"type":"message_start","message":{"id":"msg-1","type":"message","role":"assistant","model":"test","usage":{"input_tokens":13,"output_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":5}}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"secret answer"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{"type":"char_location","start_char_index":1}}}`,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":21,"cache_creation":{"ephemeral_1h_input_tokens":2}}}`,
			`{"type":"message_stop"}`), "usage.output_tokens", "content.0.text", "secret answer"},
		{"chat", "/v1/chat/completions", recoverySSE(
			`{"id":"chat-1","model":"test","choices":[{"index":0,"delta":{"role":"assistant","content":"secret "}}]}`,
			`{"choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":13,"completion_tokens":21,"prompt_tokens_details":{"cached_tokens":3}}}`,
			`[DONE]`), "usage.completion_tokens", "choices.0.message.content", "secret answer"},
		{"responses", "/v1/responses", recoverySSE(
			`{"type":"response.completed","response":{"id":"resp-1","status":"completed","model":"test","output":[{"type":"message","custom_field":"keep","content":[{"type":"output_text","text":"secret answer"}]}],"usage":{"input_tokens":13,"output_tokens":21}}}`),
			"usage.output_tokens", "output.0.custom_field", "keep"},
		{"gemini", "/v1beta/models/test:generateContent", recoverySSE(
			`{"responseId":"g-1","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"secret ","thought":true}]}}]}`,
			`{"candidates":[{"index":0,"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":13,"candidatesTokenCount":21,"thoughtsTokenCount":5,"cachedContentTokenCount":3}}`),
			"usageMetadata.candidatesTokenCount", "candidates.0.content.parts.1.text", "answer"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Setenv("BILLING_RECOVERY_ENABLED", "true")
			store := &streamRecoveryStore{}
			service.ConfigureBillingRecovery(store)
			t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
			original, disconnect := context.WithCancel(context.Background())
			defer disconnect()
			ctx, stop := service.BillingRecoveryRequest(original, &service.APIKey{ID: 2, User: &service.User{ID: 1}}, nil, fixture.path, true)
			defer stop()
			var sends atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sends.Add(1)
				data, _ := io.ReadAll(r.Body)
				if fixture.name == "gemini" {
					require.True(t, strings.HasSuffix(r.URL.Path, ":streamGenerateContent"))
					require.Equal(t, "sse", r.URL.Query().Get("alt"))
				} else {
					require.True(t, gjson.GetBytes(data, "stream").Bool())
					if fixture.name == "chat" {
						require.True(t, gjson.GetBytes(data, "stream_options.include_usage").Bool())
					}
				}
				require.NotNil(t, store.entry, "intent must exist before upstream accepts the request")
				require.Equal(t, store.entry.ID, r.Header.Get("X-Client-Request-ID"))
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Request-ID", "provider-id")
				w.Header().Set("X-Client-Request-ID", "provider-client-id")
				w.WriteHeader(200)
				require.NoError(t, http.NewResponseController(w).Flush())
				disconnect()
				_, _ = io.WriteString(w, ": heartbeat\n\n"+fixture.events)
			}))
			defer server.Close()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+fixture.path,
				strings.NewReader(`{"model":"test","stream":false,"messages":[{"content":"secret prompt"}]}`))
			require.NoError(t, err)
			resp, err := NewHTTPUpstream(nil).Do(req, "", 3, 1)
			require.NoError(t, err)
			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
			require.EqualValues(t, 21, gjson.GetBytes(data, fixture.tokenPath).Int())
			require.Equal(t, fixture.content, gjson.GetBytes(data, fixture.contentPath).String())
			require.True(t, gjson.GetBytes(store.data, "_recovery.complete").Bool())
			require.True(t, gjson.GetBytes(store.data, "_recovery.client_disconnected").Bool())
			require.Equal(t, "provider-client-id", gjson.GetBytes(store.data, "_recovery.response_request_ids.x-client-request-id").String())
			require.NotContains(t, string(store.data), "secret")
			require.NotContains(t, string(store.entry.Snapshot), "secret")
			if fixture.name == "anthropic" {
				require.EqualValues(t, 5, gjson.GetBytes(store.data, "cache_creation.ephemeral_5m_input_tokens").Int())
			}
			_, err = NewHTTPUpstream(nil).Do(req, "", 3, 1)
			require.ErrorIs(t, err, context.Canceled)
			require.EqualValues(t, 1, sends.Load(), "disconnection cannot start a second inference")
		})
	}
}

func TestBillingRecoverySyncBridgeRejectsIncompleteAndErrorResponses(t *testing.T) {
	for _, body := range []string{
		recoverySSE(`{"type":"message_start","message":{"id":"m","usage":{"input_tokens":13,"output_tokens":0}}}`, `{"type":"message_stop"}`),
		recoverySSE(`{"type":"message_start","message":{"id":"m","usage":{"input_tokens":13}}}`, `{"type":"error","error":{"type":"overloaded_error"}}`),
		recoverySSE(`{"type":"message_start","message":{"id":"m","usage":{"input_tokens":13}}}`, `{"type":"message_delta","usage":{"output_tokens":21}}`),
	} {
		_, err := collectRecoveryStream(strings.NewReader(body), "anthropic")
		require.Error(t, err)
	}
	_, err := collectRecoveryStream(strings.NewReader(recoverySSE(`{"choices":[{"index":-1,"delta":{"content":"x"}}]}`)), "chat")
	require.ErrorContains(t, err, "index")
}

func TestBillingRecoverySyncBridgePreservesToolsThinkingAndMultilineFrames(t *testing.T) {
	body := recoverySSE(
		`{"type":"message_start","message":{"id":"m","usage":{"input_tokens":13}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reason"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool","name":"run","input":{}}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"a\":"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"1}"}}`,
		`{"type":"message_delta","usage":{"output_tokens":21}}`, `{"type":"message_stop"}`)
	data, err := collectRecoveryStream(strings.NewReader(body), "anthropic")
	require.NoError(t, err)
	require.Equal(t, "reason", gjson.GetBytes(data, "content.0.thinking").String())
	require.Equal(t, "sig", gjson.GetBytes(data, "content.0.signature").String())
	require.EqualValues(t, 1, gjson.GetBytes(data, "content.1.input.a").Int())
	data, err = collectRecoveryStream(strings.NewReader("data: {\"type\":\"response.completed\",\ndata: \"response\":{\"output\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}"), "responses")
	require.NoError(t, err)
	require.True(t, json.Valid(data))
}

func TestBillingRecoverySyncBridgeSkipsNativeStreamingAndSignedRequests(t *testing.T) {
	service.ConfigureBillingRecovery(&streamRecoveryStore{})
	t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
	ctx, stop := service.BillingRecoveryRequest(context.Background(), &service.APIKey{ID: 2, User: &service.User{ID: 1}}, nil, "/v1/messages", true)
	defer stop()
	nilURL := &http.Request{Method: http.MethodPost}
	result, protocol, err := prepareRecoveryStream(nilURL.WithContext(ctx))
	require.NoError(t, err)
	require.Nil(t, result.URL)
	require.Empty(t, protocol)
	for _, fixture := range []struct{ path, body, auth string }{
		{"/v1/messages", `{"stream":true}`, ""},
		{"/v1/responses", `{"background":true}`, ""},
		{"/v1/messages", `{"stream":false}`, "AWS4-HMAC-SHA256 signed"},
		{"/v1/images/generations", `{"stream":false}`, ""},
	} {
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://provider.invalid"+fixture.path, strings.NewReader(fixture.body))
		req.Header["authorization"] = []string{fixture.auth}
		result, protocol, err := prepareRecoveryStream(req)
		require.NoError(t, err)
		require.Same(t, req, result)
		require.Empty(t, protocol)
	}
}

func TestBillingRecoveryObserverNeverConfirmsInitialOrInterruptedMultiChoiceUsage(t *testing.T) {
	for _, fixture := range []struct {
		name, body string
		complete   bool
	}{
		{"initial zero only", recoverySSE(`{"type":"message_start","message":{"usage":{"input_tokens":13,"output_tokens":0}}}`, `{"type":"message_stop"}`), false},
		{"missing second choice end", recoverySSE(`{"choices":[{"index":0,"delta":{}},{"index":1,"delta":{}}]}`, `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":13,"completion_tokens":21}}`, `[DONE]`), false},
		{"invalid final token type", recoverySSE(`{"type":"message_start","message":{"usage":{"input_tokens":13,"output_tokens":0}}}`, `{"type":"message_delta","usage":{"output_tokens":"unknown"}}`, `{"type":"message_stop"}`), false},
		{"invalid final input retains only partial usage", recoverySSE(`{"type":"message_start","message":{"usage":{"input_tokens":13,"output_tokens":0}}}`, `{"type":"message_delta","usage":{"input_tokens":"unknown","output_tokens":21}}`, `{"type":"message_stop"}`), false},
		{"invalid nested cache type", recoverySSE(`{"type":"message_start","message":{"usage":{"input_tokens":13,"cache_creation":{"ephemeral_5m_input_tokens":5}}}}`, `{"type":"message_delta","usage":{"output_tokens":21,"cache_creation":{"ephemeral_5m_input_tokens":"unknown"}}}`, `{"type":"message_stop"}`), false},
		{"final zero", recoverySSE(`{"type":"message_start","message":{"usage":{"input_tokens":0,"output_tokens":0}}}`, `{"type":"message_delta","usage":{"output_tokens":0}}`, `{"type":"message_stop"}`), true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			store := &observationStore{}
			service.ConfigureBillingRecovery(store)
			t.Cleanup(func() { service.ConfigureBillingRecovery(nil) })
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(fixture.body))}
			observeBillingRecoveryResponse("case", resp, nil)
			_, err := io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, fixture.complete, gjson.GetBytes(store.data, "_recovery.complete").Bool())
		})
	}
}

func TestBillingRecoveryResponsesSupplementRetainsTerminalExtensions(t *testing.T) {
	for _, terminal := range []struct{ text, want string }{{"", "streamed answer"}, {"authoritative answer", "authoritative answer"}} {
		body := recoverySSE(
			`{"type":"response.output_text.delta","delta":"streamed answer"}`,
			`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call-1","name":"run"}}`,
			`{"type":"response.function_call_arguments.done","output_index":1,"arguments":"{\"a\":1}"}`,
			`{"type":"response.completed","response":{"custom_top":"keep","output":[{"type":"message","custom_item":"keep","content":[{"type":"output_text","text":"`+terminal.text+`","annotations":[{"type":"url_citation","url":"https://example.com"}]}]},{"type":"function_call","call_id":"call-1","name":"run","custom_tool":"keep","arguments":""}],"usage":{"input_tokens":13,"output_tokens":21}}}`)
		data, err := collectRecoveryStream(strings.NewReader(body), "responses")
		require.NoError(t, err)
		require.Equal(t, terminal.want, gjson.GetBytes(data, "output.0.content.0.text").String())
		require.Equal(t, "https://example.com", gjson.GetBytes(data, "output.0.content.0.annotations.0.url").String())
		require.Equal(t, "keep", gjson.GetBytes(data, "custom_top").String())
		require.Equal(t, "keep", gjson.GetBytes(data, "output.0.custom_item").String())
		require.Equal(t, "keep", gjson.GetBytes(data, "output.1.custom_tool").String())
		require.JSONEq(t, `{"a":1}`, gjson.GetBytes(data, "output.1.arguments").String())
	}
}

func TestBillingRecoveryChatRetainsToolArgumentsAndLongTextFragments(t *testing.T) {
	var body strings.Builder
	_, _ = body.WriteString(recoverySSE(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"run","arguments":""}}]}}]}`))
	for index := 0; index < 2000; index++ {
		_, _ = body.WriteString(recoverySSE(`{"choices":[{"index":0,"delta":{"content":"text ","reasoning_content":"thought "}}]}`))
	}
	_, _ = body.WriteString(recoverySSE(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`,
		`{"choices":[],"usage":{"prompt_tokens":13,"completion_tokens":2000}}`, `[DONE]`))
	data, err := collectRecoveryStream(strings.NewReader(body.String()), "chat")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("text ", 2000), gjson.GetBytes(data, "choices.0.message.content").String())
	require.Equal(t, strings.Repeat("thought ", 2000), gjson.GetBytes(data, "choices.0.message.reasoning_content").String())
	require.Equal(t, "call-1", gjson.GetBytes(data, "choices.0.message.tool_calls.0.id").String())
	require.JSONEq(t, `{"a":1}`, gjson.GetBytes(data, "choices.0.message.tool_calls.0.function.arguments").String())
}
