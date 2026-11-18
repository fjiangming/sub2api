package conversationlog

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type tailTestBody struct {
	io.ReadCloser
	tail *ResponseTail
}

func (b tailTestBody) ConversationTail() *ResponseTail { return b.tail }

func newTailAttempt(t *testing.T, c *collector, raw io.ReadCloser, ctx context.Context, status int) (*attempt, *observedBody, *ResponseTail) {
	t.Helper()
	a := reliabilityScope(t, c).begin(3, "", "/responses", "POST", "JSON")
	a.append("request", []byte(`{"model":"test"}`), io.EOF)
	a.row.Status = status
	body := &observedBody{ReadCloser: raw, attempt: a, side: "response"}
	return a, body, NewResponseTail(body, ctx, context.Background())
}

func waitTail(t *testing.T, tail *ResponseTail) {
	t.Helper()
	select {
	case <-tail.job.Load().done:
	case <-time.After(2 * time.Second):
		t.Fatal("tail did not close within its bounded lifetime")
	}
}

func TestResponseTailKeepsExactErrorAndTerminalResponseBytes(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write([]byte(`{"error":{"message":"failure"},"number":9007199254740993}`))
	_ = zw.Close()
	for _, sample := range []struct {
		name, body string
		status     int
		terminal   bool
		declared   bool
	}{
		{"http-error", `{"error":"bad request","extra":1.2300}`, 400, false, true},
		{"large-error", strings.Repeat("unmodified error\r\n", 100000), 503, false, false},
		{"gzip-error", compressed.String(), 500, false, false},
		{"failed-sse", "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"failed\"}}}\n\n: tail\n\n", 200, true, false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a, body, tail := newTailAttempt(t, c, io.NopCloser(strings.NewReader(sample.body)), ctx, sample.status)
			if sample.declared {
				a.responseTransport.expected = int64(len(sample.body))
			}
			_, _ = body.Read(make([]byte, 3))
			if sample.terminal {
				MarkResponseTerminal(&http.Response{Body: tailTestBody{body, tail}})
			}
			closeAt := time.Now()
			if !tail.Start(cancel, func() func() { return func() {} }, func() { cancel(); _ = body.Close() }) {
				t.Fatal("eligible response was not admitted")
			}
			waitTail(t, tail)
			if a.closeAt.After(closeAt.Add(100 * time.Millisecond)) {
				t.Fatal("archive duration included background tail wait")
			}
			row := next(t, c)
			if string(row.Response) != sample.body || !row.Complete || row.Reason != "" || row.Status != sample.status {
				t.Fatal("error response was truncated, changed, or conflated with model success")
			}
			if row.TransportDiagnostics.Response.Bytes != int64(len(sample.body)) || c.tails.Load() != 0 || c.used.Load() != 0 || c.jobs.Load() != 0 {
				t.Fatal("tail lost byte accounting or leaked its shared budget")
			}
		})
	}
}

func TestResponseTailDeclinesUnsafeOrResourceLimitedHandoffs(t *testing.T) {
	for _, scenario := range []string{"ordinary-close", "yielded", "cancelled", "client-gone", "read-error", "request-incomplete", "already-read", "tail-limit", "job-limit", "memory-limit"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a, body, tail := newTailAttempt(t, c, io.NopCloser(strings.NewReader("tail")), ctx, 500)
			switch scenario {
			case "ordinary-close":
				a.row.Status = 200
			case "yielded":
				tail.Yield()
				if ctx.Err() != nil {
					t.Fatal("yield cancelled an active foreground response")
				}
			case "cancelled":
				cancel()
			case "client-gone":
				tail.client = ctx
				tail.ctx = context.Background()
				cancel()
			case "read-error":
				a.row.Reason = "read_error"
			case "request-incomplete":
				a.requestDone = false
			case "already-read":
				a.responseTransport.expected = 0
			case "tail-limit":
				c.tails.Store(2)
			case "job-limit":
				c.jobs.Store(32)
			case "memory-limit":
				c.used.Store(64 * 1024 * 1024)
			}
			used, jobs, tails := c.used.Load(), c.jobs.Load(), c.tails.Load()
			if tail.Start(cancel, func() func() { t.Error("unexpected reader ownership"); return func() {} }, func() { _ = body.Close() }) {
				t.Fatal("unsafe tail was admitted")
			}
			if c.used.Load() != used || c.jobs.Load() != jobs || c.tails.Load() != tails || tail.Closing() {
				t.Fatal("declined handoff changed admission or shared resource accounting")
			}
			a.clearLocked()
		})
	}
}

func TestResponseTailYieldsItsReaderImmediatelyForNewInference(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, body, tail := newTailAttempt(t, c, &tailContextReader{ctx: ctx}, ctx, 500)
	if !tail.Start(cancel, func() func() { return func() {} }, func() { cancel(); _ = body.Close() }) {
		t.Fatal("tail not admitted")
	}
	tail.Yield()
	select {
	case <-tail.job.Load().done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("new inference waited for the tail's idle timeout")
	}
	row := next(t, c)
	if row.Complete || c.tails.Load() != 0 || c.used.Load() != 0 || c.jobs.Load() != 0 {
		t.Fatal("yielded archive was declared complete or retained resources")
	}
}

type tailContextReader struct {
	ctx   context.Context
	delay time.Duration
	reads atomic.Int32
}

func (r *tailContextReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			p[0] = 'x'
			return 1, nil
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}
func (*tailContextReader) Close() error { return nil }

func TestResponseTailIdleAndTotalBudgetsKeepRawFailureAndReleaseOwnership(t *testing.T) {
	for _, delay := range []time.Duration{0, 25 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := &tailContextReader{ctx: ctx, delay: delay}
			_, body, tail := newTailAttempt(t, c, raw, ctx, 500)
			start := time.Now()
			if !tail.Start(cancel, func() func() { return func() {} }, func() { cancel(); _ = body.Close() }) {
				t.Fatal("tail not admitted")
			}
			if time.Since(start) > 100*time.Millisecond {
				t.Fatal("caller waited for a background read")
			}
			waitTail(t, tail)
			elapsed := time.Since(start)
			minimum := tailIdleBudget
			if delay > 0 {
				minimum = tailTotalBudget
			}
			if elapsed < minimum-25*time.Millisecond || elapsed > minimum+500*time.Millisecond {
				t.Fatal("idle or total tail bound was not enforced")
			}
			row := next(t, c)
			if row.Complete || row.Reason != "read_error" || row.TransportDiagnostics.Response.Error != "cancelled" || !row.TransportDiagnostics.Response.ErrorDuringClose {
				t.Fatal("bounded cancellation was hidden or mislabeled complete")
			}
			if c.used.Load() != 0 || c.jobs.Load() != 0 || c.tails.Load() != 0 || raw.reads.Load() == 0 {
				t.Fatal("bounded tail retained memory, jobs or active readers")
			}
		})
	}
}

func TestResponseTailRechecksModelsBeforeReadingAdditionalBytes(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, ModelFilterEnabled: true, ModelFilterVersion: 1, AllowedModels: []string{"different"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, body, tail := newTailAttempt(t, c, io.NopCloser(strings.NewReader("must not read")), ctx, 500)
	if !tail.Start(cancel, func() func() { return func() {} }, func() { cancel(); _ = body.Close() }) {
		t.Fatal("model parsing unexpectedly ran on admission")
	}
	waitTail(t, tail)
	row := next(t, c)
	if len(row.Response) != 0 || row.Complete || c.used.Load() != 0 || c.jobs.Load() != 0 {
		t.Fatal("excluded model received tail reads or leaked archive ownership")
	}
}

func TestResponseTailHandoffRetainsFinishedForegroundReadWithoutReadingAgain(t *testing.T) {
	for _, readErr := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(readErr.Error(), func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := &tailContextReader{ctx: ctx}
			a, body, tail := newTailAttempt(t, c, raw, ctx, 500)
			if !tail.Start(cancel, func() func() {
				a.appendObserved("response", []byte("last foreground bytes"), readErr, true)
				return func() {}
			}, func() { cancel(); _ = body.Close() }) {
				t.Fatal("handoff not admitted before the foreground read finished")
			}
			waitTail(t, tail)
			row := next(t, c)
			if raw.reads.Load() != 0 || string(row.Response) != "last foreground bytes" || row.Complete != (readErr == io.EOF) {
				t.Fatal("handoff reread a finished response, lost its bytes, or hid its error")
			}
			if readErr != io.EOF && row.Reason != "read_error" {
				t.Fatal("late foreground read error was overwritten")
			}
			if c.used.Load() != 0 || c.jobs.Load() != 0 || c.tails.Load() != 0 {
				t.Fatal("finished foreground read leaked handoff ownership")
			}
		})
	}
}
