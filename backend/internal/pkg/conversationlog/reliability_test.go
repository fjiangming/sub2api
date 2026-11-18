package conversationlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

type delayedFinalRead struct {
	started, release chan struct{}
	body             string
	err              error
}

func (r *delayedFinalRead) Read(p []byte) (int, error) {
	close(r.started)
	<-r.release
	return copy(p, r.body), r.err
}
func (*delayedFinalRead) Close() error { return nil }

func reliabilityScope(t *testing.T, c *collector) *scope {
	t.Helper()
	s, ok := bind(context.Background(), c, 1, 2, "").Value(scopeKey).(*scope)
	if !ok {
		t.Fatal("missing capture scope")
	}
	return s
}

func TestCloseRetainsBytesFromAlreadyStartedReadsWithoutWaitingInRelay(t *testing.T) {
	for _, finalError := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(fmt.Sprint(finalError), func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
			s := reliabilityScope(t, c)
			a := s.begin(3, "", "/responses", "POST", "SSE")
			a.append("request", []byte(`{"model":"test"}`), io.EOF)
			raw := &delayedFinalRead{started: make(chan struct{}), release: make(chan struct{}), body: "data: [DONE]\n\n", err: finalError}
			body := &observedBody{ReadCloser: raw, attempt: a, side: "response"}
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				p := make([]byte, 64)
				n, err := body.Read(p)
				if string(p[:n]) != raw.body || err != finalError {
					t.Error("relay bytes or native error changed")
				}
			}()
			<-raw.started
			closed := make(chan struct{})
			go func() { _ = body.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Close waited for the read observation")
			}
			ready := make(chan *attempt, 32)
			close(c.queue)
			go c.finalize(ready)
			select {
			case <-ready:
				t.Fatal("sealed before the pending read returned")
			default:
			}
			close(raw.release)
			<-readDone
			saved := <-ready
			saved.prepareLocked()
			if string(saved.row.Response) != raw.body || saved.row.Complete != (finalError == io.EOF) {
				t.Fatal("lost final n+error bytes or relaxed the raw completeness gate")
			}
			if finalError != io.EOF && (saved.row.Reason != "read_error" || saved.row.TransportDiagnostics.Response.Error != "unexpected_eof") {
				t.Fatal("real read error was hidden")
			}
			saved.clearLocked()
			if _, ok := <-ready; ok || c.used.Load() != 0 || c.jobs.Load() != 0 {
				t.Fatal("finalization leaked ownership or sent more than once")
			}
		})
	}
}

func TestStalledObserverExpiresWithoutReadingMoreOrRewritingSealedBytes(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	s := reliabilityScope(t, c)
	a := s.begin(3, "", "/responses", "POST", "SSE")
	a.append("request", []byte("{}"), io.EOF)
	a.append("response", []byte("prefix"), nil)
	raw := &delayedFinalRead{started: make(chan struct{}), release: make(chan struct{}), body: "late", err: io.EOF}
	body := &observedBody{ReadCloser: raw, attempt: a, side: "response"}
	readDone := make(chan struct{})
	go func() { _, _ = body.Read(make([]byte, 16)); close(readDone) }()
	<-raw.started
	_ = body.Close()
	close(c.queue)
	ready := make(chan *attempt, 32)
	go c.finalize(ready)
	select {
	case saved := <-ready:
		saved.prepareLocked()
		if string(saved.row.Response) != "prefix" || saved.row.Complete || saved.row.Reason != "closed_early" {
			t.Fatal("stalled read was mislabeled complete")
		}
		close(raw.release)
		<-readDone
		if string(saved.row.Response) != "prefix" {
			t.Fatal("late observation changed a sealed archive")
		}
		saved.clearLocked()
	case <-time.After(time.Second):
		close(raw.release)
		<-readDone
		t.Fatal("stalled reader retained the archive indefinitely")
	}
	for range ready {
		t.Fatal("duplicate archive")
	}
	if c.used.Load() != 0 || c.jobs.Load() != 0 {
		t.Fatal("deadline cleanup leaked budget")
	}
}

func TestPendingFinalizationAndReadyDeliveryShareTheThirtyTwoJobBudget(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	s := reliabilityScope(t, c)
	for i := 0; i < 33; i++ {
		a := s.begin(3, "", "/responses", "POST", "SSE")
		a.append("response", []byte("x"), nil)
		a.observers.Store(1)
		a.observers.Add(observerClosed)
		a.closeResponse()
	}
	ready := make(chan *attempt, 32)
	go c.finalize(ready)
	first := <-ready
	if c.jobs.Load() != 32 || c.dropped.Load() != 1 {
		t.Fatal("pending/ready queues multiplied the job budget")
	}
	extra := s.begin(3, "", "/responses", "POST", "JSON")
	extra.finish(true, "")
	if c.jobs.Load() != 32 || c.dropped.Load() != 2 {
		t.Fatal("moving to the ready queue freed a still-owned slot")
	}
	close(c.queue)
	first.clearLocked()
	for a := range ready {
		a.clearLocked()
	}
	if c.jobs.Load() != 0 || c.used.Load() != 0 {
		t.Fatal("pending cleanup leaked memory or job slots")
	}
}

func TestMemoryPressureDuringCloseGraceDropsTheWholeArchive(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	a := reliabilityScope(t, c).begin(3, "", "/responses", "POST", "JSON")
	a.append("request", []byte(`{"model":"test"}`), io.EOF)
	raw := &delayedFinalRead{started: make(chan struct{}), release: make(chan struct{}), body: "last", err: io.EOF}
	body := &observedBody{ReadCloser: raw, attempt: a, side: "response"}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		p := make([]byte, 16)
		n, err := body.Read(p)
		if string(p[:n]) != raw.body || err != io.EOF {
			t.Error("memory pressure changed native response bytes or error")
		}
	}()
	<-raw.started
	_ = body.Close()
	external := int64(64*1024*1024) - c.used.Load()
	if !c.reserveObservation(external) {
		close(raw.release)
		<-readDone
		t.Fatal("could not reserve simulated concurrent observation memory")
	}
	defer c.used.Add(-external)
	close(raw.release)
	<-readDone
	close(c.queue)
	ready := make(chan *attempt, 32)
	c.finalize(ready)
	emitted := 0
	for saved := range ready {
		emitted++
		saved.clearLocked()
	}
	if emitted != 0 || c.dropped.Load() != 1 || c.jobs.Load() != 0 || c.used.Load() != external {
		t.Fatal("grace-period memory failure emitted a partial archive or leaked capture ownership")
	}
}

func TestDelayedRetryLetsAnotherRecordDeliverAndKeepsEnvelopeStable(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	s := reliabilityScope(t, c)
	a, b := s.begin(3, "", "/responses", "POST", "JSON"), s.begin(3, "", "/responses", "POST", "JSON")
	for _, item := range []*attempt{a, b} {
		item.append("request", []byte(`{"model":"test"}`), io.EOF)
		item.append("response", []byte("{}"), io.EOF)
		item.finish(true, "")
	}
	base := c.http.Transport
	var order []string
	var first []byte
	c.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			return base.RoundTrip(req)
		}
		encoded, _ := io.ReadAll(req.Body)
		var row event
		_ = json.Unmarshal(encoded, &row)
		order = append(order, row.ID)
		if row.ID == a.row.ID && first == nil {
			first = encoded
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		}
		if row.ID == a.row.ID && !bytes.Equal(first, encoded) {
			t.Error("retry changed the immutable archive envelope")
		}
		ack, _ := json.Marshal(map[string]any{"accepted": true, "id": row.ID, "body_sha256": row.BodySHA256})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(ack))}, nil
	})
	close(c.queue)
	c.run()
	if !reflect.DeepEqual(order, []string{a.row.ID, b.row.ID, a.row.ID}) || c.jobs.Load() != 0 || c.used.Load() != 0 || c.dropped.Load() != 0 {
		t.Fatal("failed delivery monopolized the sender or leaked its budget")
	}
}

func TestDurableReceiptDeadlineIsNegotiatedOnlyForBackgroundPosts(t *testing.T) {
	for _, version := range []int{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, DurableReceiptVersion: version})
			c.http.Timeout = 2 * time.Second
			base := c.http.Transport
			c.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				deadline, ok := req.Context().Deadline()
				if !ok {
					t.Error("missing archive deadline")
				}
				remaining := time.Until(deadline)
				if req.Method != http.MethodPost {
					if remaining > 201*time.Millisecond {
						t.Error("policy deadline was extended")
					}
					return base.RoundTrip(req)
				}
				want := 2 * time.Second
				if version == 1 {
					want = 5 * time.Second
				}
				if remaining > want || remaining < want-500*time.Millisecond {
					t.Error("unsupported or unbounded receipt deadline")
				}
				var row event
				_ = json.NewDecoder(req.Body).Decode(&row)
				ack, _ := json.Marshal(map[string]any{"accepted": true, "id": row.ID, "body_sha256": row.BodySHA256})
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(ack))}, nil
			})
			s := reliabilityScope(t, c)
			a := s.begin(3, "", "/responses", "POST", "JSON")
			a.append("request", []byte("{}"), io.EOF)
			a.finish(true, "")
			close(c.queue)
			c.run()
			if c.http.Timeout != 2*time.Second || c.dropped.Load() != 0 || c.used.Load() != 0 || c.jobs.Load() != 0 {
				t.Fatal("background negotiation changed the shared client or leaked capture ownership")
			}
		})
	}
}

type closeWindowBody struct{ closing, release chan struct{} }

func (*closeWindowBody) Read([]byte) (int, error) { return 0, net.ErrClosed }
func (b *closeWindowBody) Close() error {
	close(b.closing)
	<-b.release
	return nil
}

func TestReadsStartedAfterResponseCloseCannotInventAnArchiveReadError(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	a := reliabilityScope(t, c).begin(3, "", "/responses", "POST", "JSON")
	a.append("request", []byte("{}"), io.EOF)
	a.append("response", []byte("{}"), io.EOF)
	raw := &closeWindowBody{closing: make(chan struct{}), release: make(chan struct{})}
	body := &observedBody{ReadCloser: raw, attempt: a, side: "response"}
	closed := make(chan struct{})
	go func() { _ = body.Close(); close(closed) }()
	<-raw.closing
	_, err := body.Read(make([]byte, 8))
	close(raw.release)
	<-closed
	if err != net.ErrClosed {
		t.Fatal("native read after close changed")
	}
	row := next(t, c)
	if !row.Complete || row.Reason != "" || string(row.Response) != "{}" {
		t.Fatal("a new read after close modified the previously observed EOF")
	}
}
