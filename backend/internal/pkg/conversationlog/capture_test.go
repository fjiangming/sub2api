package conversationlog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fixture(t *testing.T, p policy) (*collector, *policy, *sync.Mutex) {
	t.Helper()
	mu := &sync.Mutex{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-collector-token" {
			t.Error("missing collector authentication")
		}
		mu.Lock()
		defer mu.Unlock()
		if p.Version == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p)
	}))
	t.Cleanup(server.Close)
	c := &collector{base: server.URL, token: "test-collector-token", http: server.Client(), queue: make(chan *attempt, 32)}
	return c, &p, mu
}

func next(t *testing.T, c *collector) event {
	t.Helper()
	select {
	case a := <-c.queue:
		a.mu.Lock()
		defer a.mu.Unlock()
		row := a.row
		a.clearLocked()
		return row
	case <-time.After(time.Second):
		t.Fatal("archive was not finalized")
		return event{}
	}
}

func TestHTTPPreservesActualBytesAndRetryIdentity(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 3, Enabled: true, Limit: 65536})
	raw := []byte("{\n  \"model\": \"test-model\", \"big\":9007199254740993, \"decimal\":1.2300, \"unknown\":true\n}")
	sse := []byte("event: response.output_text.delta\r\ndata: {\"delta\":\"hello\"}\r\n\r\ndata: [DONE]\r\n\r\n")
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write(sse)
	_ = gz.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, raw) || r.URL.RawQuery != "secret=not-archived" {
			t.Error("inference request changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer upstream.Close()
	client := &http.Client{Transport: WrapTransport(&http.Transport{DisableCompression: true})}
	defer client.CloseIdleConnections()
	ctx := WithAccount(bind(context.Background(), c, 12, 34, "session"), 56)
	var previous event
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, upstream.URL+"/v1/responses?secret=not-archived", bytes.NewReader(raw))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if !bytes.Equal(body, compressed.Bytes()) {
			t.Fatal("upstream response changed")
		}
		row := next(t, c)
		if !bytes.Equal(row.Request, raw) || !bytes.Equal(row.Response, compressed.Bytes()) {
			t.Fatal("raw body was reformatted")
		}
		if !row.Complete || row.Protocol != "SSE" || row.Encoding != "gzip" || row.Path != "/v1/responses" || row.AccountID != 56 || row.Model != "test-model" {
			t.Fatalf("incorrect metadata: %+v", row)
		}
		if i > 0 && (previous.RequestID != row.RequestID || previous.ID == row.ID) {
			t.Fatal("retry association is incorrect")
		}
		previous = row
	}
	if c.used.Load() != 0 {
		t.Fatal("capture memory was not released")
	}
}

func TestExclusionAndPolicyFailureLeaveInferenceUntouched(t *testing.T) {
	c, p, mu := fixture(t, policy{Version: 1, Enabled: true, Excluded: []int64{12}, Limit: 65536})
	for i := 0; i < 2; i++ {
		ctx := bind(context.Background(), c, 12, 34, "session")
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://upstream.example/v1/responses", strings.NewReader(`{"secret":"value"}`))
		base := roundTripFunc(func(actual *http.Request) (*http.Response, error) {
			if actual != req {
				t.Error("excluded request should never be wrapped or copied")
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: http.Header{}}, nil
		})
		resp, err := WrapTransport(base).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if len(c.queue) != 0 || c.used.Load() != 0 {
			t.Fatal("excluded/unknown-policy user retained data")
		}
		mu.Lock()
		p.Version = 0
		mu.Unlock()
	}
	if c.dropped.Load() != 1 {
		t.Fatal("policy failure should be counted without user metadata")
	}
}

func TestEarlyCloseAndBodyLimitAreExplicit(t *testing.T) {
	for _, limit := range []int{65536, 131072} {
		c, _, _ := fixture(t, policy{Version: 1, Enabled: true, Limit: limit})
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			return &http.Response{StatusCode: 200, Header: http.Header{}, ContentLength: 100000, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 100000)))}, nil
		})
		req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/v1/responses", strings.NewReader("{}"))
		resp, _ := WrapTransport(base).RoundTrip(req)
		_, _ = io.CopyN(io.Discard, resp.Body, 70000)
		_ = resp.Body.Close()
		row := next(t, c)
		if row.Complete {
			t.Fatal("early close was reported as complete")
		}
		if limit == 65536 {
			if row.Reason != "body_limit" || len(row.Request)+len(row.Response) != 0 {
				t.Fatal("oversized raw body must be discarded")
			}
		} else if row.Reason != "closed_early" || len(row.Response) != 70000 {
			t.Fatal("partial bytes must be retained explicitly")
		}
		if c.used.Load() != 0 {
			t.Fatal("memory leaked")
		}
	}
}

func TestWebSocketIdentityIsPerLeaseAndContextCancellationIsPreserved(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, Limit: 65536})
	var wg sync.WaitGroup
	for user := int64(1); user <= 8; user++ {
		wg.Add(1)
		go func(user int64) {
			defer wg.Done()
			owner, cancel := context.WithCancel(bind(context.Background(), c, user, user+10, "same-client-session"))
			cancel()
			parent, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			ctx := CaptureContext(parent, owner, user+20)
			if ctx.Err() != nil {
				t.Error("archive replaced the caller cancellation semantics")
			}
			RecordFrame(ctx, Account(ctx), "out", []byte(`{"type":"response.create","model":"test-model"}`), false)
			RecordFrame(ctx, Account(ctx), "in", []byte(`{"type":"response.completed"}`), false)
			Finish(owner)
		}(user)
	}
	wg.Wait()
	sessions := map[string]bool{}
	for i := 0; i < 8; i++ {
		row := next(t, c)
		if row.KeyID != row.UserID+10 || row.AccountID != row.UserID+20 || len(row.Frames) != 2 || !row.Complete {
			t.Fatal("pooled identity leaked across leases")
		}
		if sessions[row.Session] {
			t.Fatal("session was shared across users/keys")
		}
		sessions[row.Session] = true
	}
}

func TestSendingRechecksExclusionAndNeverSpools(t *testing.T) {
	c, p, mu := fixture(t, policy{Version: 1, Enabled: true, Limit: 65536})
	ctx := bind(context.Background(), c, 1, 2, "")
	RecordFrame(ctx, 3, "out", []byte(`{"type":"response.create"}`), false)
	Finish(ctx)
	mu.Lock()
	p.Excluded = []int64{1}
	mu.Unlock()
	close(c.queue)
	c.run()
	if c.used.Load() != 0 {
		t.Fatal("excluded in-flight body was retained")
	}
	if c.dropped.Load() != 0 {
		t.Fatal("excluded user must not create a loss record")
	}
}

func TestCapturePressureDiscardsBodiesAndReleasesTheAttemptBudget(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, Limit: 65536})
	s := bind(context.Background(), c, 1, 2, "").Value(scopeKey).(*scope)
	a := s.begin(3, "upstream.example", "/v1/responses", "POST", "JSON")
	c.used.Store(64 * 1024 * 1024)
	a.append("request", []byte("secret"), nil)
	a.finish(false, "")
	row := next(t, c)
	if row.Reason != "memory_limit" || len(row.Request) != 0 {
		t.Fatal("memory pressure retained the body")
	}
	c.used.Store(0)
	for i := 0; i < 33; i++ {
		a := s.begin(3, "upstream.example", "/v1/responses", "POST", "JSON")
		a.append("request", []byte("{}"), io.EOF)
		a.finish(true, "")
	}
	if c.dropped.Load() != 1 {
		t.Fatal("full queue was not counted")
	}
	for i := 0; i < 32; i++ {
		next(t, c)
	}
	if c.used.Load() != 0 {
		t.Fatal("queued bodies leaked their budget")
	}
}
