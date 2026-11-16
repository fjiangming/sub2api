package conversationlog

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func filterPolicy() policy {
	return policy{Version: 1, Enabled: true, Limit: 4 * 1024 * 1024, ModelFilterVersion: 1, ModelFilterEnabled: true, KimiK3Plus: true,
		AllowedModels: []string{"gpt-6-astra", "gpt-6-sol", "gpt-6.1-sol", "gpt-5.6-sol", "claude-fable-5", "claude-fable-5-1", "claude-opus-5", "claude-opus-5-5", "gemini-carbon"}}
}

func TestModelWhitelistCanonicalAliasesAndBoundaries(t *testing.T) {
	p := filterPolicy()
	for _, model := range append(p.AllowedModels, "GPT-6.1-SOL", "Fable5", "Fable5.1", "Claude Fable 5.1", "Opus5", "Opus5.5", "claude-opus-5.5", "Gemini carbon", "Kimik3", "kimi-k3.1", "kimi-k3-0905-preview", "kimi-k4-thinking", "kimi-k10") {
		if !p.allowsModel(model) {
			t.Errorf("selected model rejected: %s", model)
		}
	}
	for _, model := range []string{"", "claude-fable-5-2", "claude-opus-5-50", "gpt-6.1-terra", "gpt-6-sol-extra", "deepseek-v4.1", "kimi-k2.9", "kimi-k3fake", "not-kimi-k4", "Gemini carbon-image"} {
		if p.allowsModel(model) {
			t.Errorf("unselected model accepted: %s", model)
		}
	}
	p.AllowedModels, p.KimiK3Plus = nil, false
	if p.allowsModel("gpt-6.1-sol") || p.allowsModel("kimi-k3") {
		t.Fatal("empty filter must not allow all models")
	}
	p.AllowedModels = []string{"Kimik3"}
	if !p.allowsModel("kimi-k3") || p.allowsModel("kimi-k3.1") {
		t.Fatal("explicit Kimi model must match only that canonical version")
	}
}

func TestHTTPModelFilterPreservesInferenceAndIdentifiesLargeRequestsAndURLModels(t *testing.T) {
	c, _, _ := fixture(t, filterPolicy())
	for _, sample := range []struct {
		name, path, body, model string
		allowed                 bool
	}{
		{"selected", "/v1/responses", `{"model":"gpt-6.1-sol","input":"safe"}`, "gpt-6.1-sol", true},
		{"large", "/v1/responses", `{"input":"` + strings.Repeat("a", 2*1024*1024) + `","model":"gpt-6.1-sol"}`, "gpt-6.1-sol", true},
		{"url", "/v1beta/models/gemini-carbon:streamGenerateContent", `{"contents":[]}`, "gemini-carbon", true},
		{"other", "/v1/responses", `{"model":"gpt-6.1-terra","input":"safe"}`, "", false},
		{"duplicate", "/v1/responses", `{"model":"gpt-6.1-sol","Model":"other"}`, "", false},
		{"unknown", "/v1/responses", `{"input":"safe"}`, "", false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example"+sample.path, strings.NewReader(sample.body))
			req.Header.Set("Content-Type", "application/json")
			base := roundTripFunc(func(actual *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(actual.Body)
				_ = actual.Body.Close()
				if string(body) != sample.body {
					t.Fatal("model filtering changed inference bytes")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("response"))}, nil
			})
			resp, err := WrapTransport(base).RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if sample.allowed {
				row := next(t, c)
				if row.Model != sample.model || string(row.Request) != sample.body || !row.Complete {
					t.Fatal("selected model archive was incomplete or changed")
				}
			} else if len(c.queue) != 0 {
				t.Fatal("unselected model created an archive event")
			}
			if c.used.Load() != 0 || c.dropped.Load() != 0 {
				t.Fatal("model exclusion leaked memory or created a gap")
			}
		})
	}
}

func TestWebSocketNestedModelInheritanceAndLaterSelectedSegments(t *testing.T) {
	c, _, _ := fixture(t, filterPolicy())
	ctx := bind(context.Background(), c, 1, 2, "")
	for _, sample := range []struct {
		create string
		allow  bool
	}{
		{`{"type":"response.create","response":{"model":"gpt-6.1-sol"}}`, true},
		{`{"type":"response.create","response":{"input":[]}}`, true},
		{`{"type":"response.create","response":{"model":"other"}}`, false},
		{`{"type":"response.create","response":{"input":[]}}`, false},
		{`{"type":"response.create","response":{"model":"gpt-6-sol"}}`, true},
		{`{"type":"response.create","response":{"model":"gpt-6-sol","Model":"other"}}`, false},
		{`{"type":"response.create","response":{"input":[]}}`, false},
	} {
		RecordFrame(ctx, 3, "out", []byte(sample.create), false)
		RecordFrame(ctx, 3, "in", []byte(`{"type":"response.completed"}`), false)
		if sample.allow {
			row := next(t, c)
			if len(row.Frames) != 2 || row.Model == "" || !row.Complete {
				t.Fatal("selected WebSocket segment lost model or frames")
			}
		} else if len(c.queue) != 0 || c.used.Load() != 0 {
			t.Fatal("unselected WebSocket segment was retained")
		}
	}
}

func TestModelFilterRechecksQueuedEventsBeforeSending(t *testing.T) {
	c, p, mu := fixture(t, filterPolicy())
	ctx := bind(context.Background(), c, 1, 2, "")
	RecordFrame(ctx, 3, "out", []byte(`{"model":"gpt-6.1-sol","Model":"gpt-6-sol"}`), false)
	Finish(ctx)
	if len(c.queue) != 1 {
		t.Fatal("selected event was not queued")
	}
	mu.Lock()
	p.AllowedModels = []string{"gpt-6.1-sol"}
	mu.Unlock()
	base := c.http.Transport
	c.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			t.Fatal("queued excluded model was transmitted")
		}
		return base.RoundTrip(req)
	})
	close(c.queue)
	c.run()
	if c.used.Load() != 0 || c.dropped.Load() != 0 {
		t.Fatal("model exclusion created a gap or retained bytes")
	}
}

func TestUnsupportedModelPolicyDisablesOnlyCapture(t *testing.T) {
	p := filterPolicy()
	p.ModelFilterVersion = 2
	c, _, _ := fixture(t, p)
	if _, ok := c.getPolicy(); ok {
		t.Fatal("unknown model filter contract must fail closed")
	}
	body := `{"model":"gpt-6.1-sol"}`
	req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/v1/responses", strings.NewReader(body))
	called := false
	base := roundTripFunc(func(actual *http.Request) (*http.Response, error) {
		called = true
		data, _ := io.ReadAll(actual.Body)
		_ = actual.Body.Close()
		if string(data) != body {
			t.Fatal("unsupported archive policy affected inference bytes")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	resp, err := WrapTransport(base).RoundTrip(req)
	if err != nil || !called {
		t.Fatal("unsupported archive policy blocked inference")
	}
	_ = resp.Body.Close()
	if len(c.queue) != 0 || c.used.Load() != 0 {
		t.Fatal("unsupported policy collected data")
	}
}
