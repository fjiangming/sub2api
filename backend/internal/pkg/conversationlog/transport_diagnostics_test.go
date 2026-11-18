package conversationlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type privateTransportError struct{}

func (privateTransportError) Error() string { panic("raw transport errors must never be formatted") }

func TestTransportErrorsUseOnlySafeEnums(t *testing.T) {
	for _, tc := range []struct {
		err  error
		kind string
	}{
		{context.Canceled, "cancelled"}, {fmt.Errorf("wrapped: %w", context.Canceled), "cancelled"},
		{context.DeadlineExceeded, "deadline"}, {os.ErrDeadlineExceeded, "deadline"},
		{fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF), "unexpected_eof"},
		{net.ErrClosed, "closed"}, {os.ErrClosed, "closed"}, {io.ErrClosedPipe, "closed"},
		{&net.DNSError{IsTimeout: true}, "timeout"}, {&net.OpError{Op: "read", Net: "tcp", Err: privateTransportError{}}, "network"},
		{privateTransportError{}, "other"},
	} {
		if got := transportErrorKind(tc.err); got != tc.kind {
			t.Fatalf("expected %s, got %s", tc.kind, got)
		}
	}
}

type dataErrorBody struct {
	data []byte
	err  error
}

func (b *dataErrorBody) Read(p []byte) (int, error) {
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, b.err
}
func (*dataErrorBody) Close() error { return nil }

func TestTransportDiagnosticsPreserveRequestAndResponseFailuresAndBytes(t *testing.T) {
	for _, side := range []string{"request", "response"} {
		t.Run(side, func(t *testing.T) {
			c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
			request, response := []byte(`{"model":"test","input":"unchanged"}`), []byte("partial response")
			errRead := fmt.Errorf("private upstream URL and token: %w", io.ErrUnexpectedEOF)
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				actual, err := io.ReadAll(r.Body)
				if !bytes.Equal(actual, request) || (side == "request" && err != errRead) || (side == "response" && err != nil) {
					t.Fatal("request bytes or original error changed")
				}
				_ = r.Body.Close()
				body := io.ReadCloser(io.NopCloser(bytes.NewReader(response)))
				if side == "response" {
					body = &dataErrorBody{data: response, err: errRead}
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, ContentLength: int64(len(response) + 10), Body: body}, nil
			})
			req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/responses", bytes.NewReader(request))
			if side == "request" {
				req.Body = &dataErrorBody{data: request, err: errRead}
			}
			resp, err := WrapTransport(base).RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := io.ReadAll(resp.Body)
			if !bytes.Equal(actual, response) || (side == "response" && err != errRead) || (side == "request" && err != nil) {
				t.Fatal("response bytes or original error changed")
			}
			_ = resp.Body.Close()
			row := next(t, c)
			if row.Complete || row.Reason != "read_error" || !bytes.Equal(row.Request, request) || !bytes.Equal(row.Response, response) {
				t.Fatal("read failure was cleared or captured bytes changed")
			}
			d := row.TransportDiagnostics
			if d == nil || d.Request.Bytes != int64(len(request)) || d.Response.Bytes != int64(len(response)) || d.Response.ExpectedBytes != int64(len(response)+10) {
				t.Fatal("transport byte counts missing")
			}
			bad, good := d.Request, d.Response
			if side == "response" {
				bad, good = good, bad
			}
			if bad.Error != "unexpected_eof" || bad.EOF || bad.ErrorDuringClose || good.Error != "" || !good.EOF {
				t.Fatal("transport error attributed to the wrong side or close phase")
			}
			encoded, _ := json.Marshal(row)
			if bytes.Contains(encoded, []byte("private upstream")) || c.used.Load() != 0 {
				t.Fatal("error content escaped or observation budget leaked")
			}
		})
	}
}

type closingResponse struct {
	reader       *strings.Reader
	reading      chan struct{}
	closeStarted chan struct{}
	readReturned chan struct{}
	reads        atomic.Int32
}

func (b *closingResponse) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if b.reader.Len() > 0 {
		return b.reader.Read(p)
	}
	close(b.reading)
	<-b.closeStarted
	return 0, context.Canceled
}
func (b *closingResponse) Close() error {
	close(b.closeStarted)
	select {
	case <-b.readReturned:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("observer did not return")
	}
}

func TestTransportDiagnosticsObserveClosePhaseWithoutExtraReadsOrClearingErrors(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
	terminal := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	body := &closingResponse{reader: strings.NewReader(terminal), reading: make(chan struct{}), closeStarted: make(chan struct{}), readReturned: make(chan struct{})}
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, ContentLength: -1, Body: body}, nil
	})
	req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/responses", strings.NewReader(`{"model":"test"}`))
	resp, err := WrapTransport(base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, len(terminal))
	if n, err := resp.Body.Read(first); n != len(first) || err != nil || string(first) != terminal {
		t.Fatal("terminal bytes were changed")
	}
	readErrors := make(chan error, 1)
	go func() {
		_, err := resp.Body.Read(make([]byte, 1))
		readErrors <- err
		close(body.readReturned)
	}()
	<-body.reading
	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if <-readErrors != context.Canceled {
		t.Fatal("close replaced the original read cancellation")
	}
	row := next(t, c)
	d := row.TransportDiagnostics
	if row.Complete || row.Reason != "read_error" || string(row.Response) != terminal || d == nil || d.Response.Error != "cancelled" || !d.Response.ErrorDuringClose || d.Response.EOF {
		t.Fatal("completion bytes or observed close phase incorrectly cleared the raw error")
	}
	if body.reads.Load() != 2 || body.reader.Len() != 0 || c.used.Load() != 0 {
		t.Fatal("diagnostics added an EOF probe or leaked memory")
	}
}

func TestTransportDiagnosticsNegotiateBeforeFirstSendAndKeepRetriesStable(t *testing.T) {
	for _, initial := range []int{0, 1} {
		for _, change := range []bool{false, true} {
			t.Run(fmt.Sprintf("initial%d/change%v", initial, change), func(t *testing.T) {
				c, p, mu := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: initial})
				s, ok := bind(context.Background(), c, 1, 2, "").Value(scopeKey).(*scope)
				if !ok {
					t.Fatal("collector scope was not bound")
				}
				a := s.begin(3, "upstream.example", "/responses", "POST", "JSON")
				a.append("request", []byte(`{"model":"test"}`), io.EOF)
				a.append("response", []byte("unchanged"), io.EOF)
				a.finish(true, "")
				a = <-c.queue
				a.prepareDelivery()
				a.row.BodySHA256 = a.capturedDigest()
				defer a.clearLocked()
				base, posts, first := c.http.Transport, 0, ""
				c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.Method != "POST" {
						return base.RoundTrip(r)
					}
					encoded, err := io.ReadAll(r.Body)
					if err != nil || r.ContentLength != int64(len(encoded)) {
						t.Fatal("stream length or encoding changed")
					}
					var row event
					if err = json.Unmarshal(encoded, &row); err != nil || bodyDigest(row) != row.BodySHA256 {
						t.Fatal("archive body digest changed")
					}
					if (row.TransportDiagnostics != nil) != (initial == 1) {
						t.Fatal("unsupported metadata sent to an old center or changed on retry")
					}
					posts++
					if posts == 1 {
						first = string(encoded)
						if change {
							mu.Lock()
							p.TransportDiagnosticsVersion = 1 - initial
							mu.Unlock()
						}
						return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("{}"))}, nil
					}
					if string(encoded) != first {
						t.Fatal("retry changed event identity, metadata or bytes")
					}
					ack, _ := json.Marshal(map[string]any{"accepted": true, "id": row.ID, "body_sha256": row.BodySHA256})
					return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(ack))}, nil
				})
				done, ok := c.deliver(a)
				for !done && a.deliveryTries < 3 {
					done, ok = c.deliver(a)
				}
				if initial == 1 && change {
					if ok || posts != 1 {
						t.Fatal("downgrade rewrote or resent an incompatible event")
					}
				} else if !ok || posts != 2 {
					t.Fatal("compatible archive retry failed")
				}
			})
		}
	}
}

func TestWebSocketDoesNotEmitHTTPTransportDiagnostics(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, TransportDiagnosticsVersion: 1})
	ctx := bind(context.Background(), c, 1, 2, "")
	RecordFrame(ctx, 3, "out", []byte(`{"model":"test"}`), false)
	RecordFrame(ctx, 3, "in", []byte(`{"type":"response.completed"}`), false)
	row := next(t, c)
	if !row.Complete || row.TransportDiagnostics != nil {
		t.Fatal("HTTP diagnostics altered WebSocket semantics")
	}
}
