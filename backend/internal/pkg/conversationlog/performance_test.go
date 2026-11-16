package conversationlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestmodel"
)

func TestStreamEnvelopePreservesContractAndLength(t *testing.T) {
	for _, websocket := range []bool{false, true} {
		t.Run(fmt.Sprint(websocket), func(t *testing.T) {
			row := event{Schema: 1, CaptureVersion: 2, ID: "safe", Request: []byte{0, 255, 13}, Response: []byte("END"), Frames: []frame{}}
			a := &attempt{row: row, requestChunks: [][]byte{{0}, {255, 13}}, responseChunks: [][]byte{{'E', 'N'}, {'D'}}}
			if websocket {
				row.Protocol = "WebSocket"
				row.Frames = []frame{{Direction: "out", At: 0, Body: row.Request, Binary: true}, {Direction: "in", At: 10, Body: row.Response}}
				a.row = row
			}
			a.row.BodySHA256, row.BodySHA256 = a.capturedDigest(), bodyDigest(row)
			body, length, stop, err := a.streamEnvelope()
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			encoded, err := io.ReadAll(body)
			if err != nil {
				t.Fatal(err)
			}
			var actual event
			if err = json.Unmarshal(encoded, &actual); err != nil {
				t.Fatal(err)
			}
			if int64(len(encoded)) != length || !reflect.DeepEqual(actual, row) {
				t.Fatal("streamed JSON changed bytes, metadata, digest or content length")
			}
		})
	}
}

func TestStreamEnvelopeStopsWhenReceiverRefusesBody(t *testing.T) {
	a := &attempt{row: event{Frames: []frame{}}, requestChunks: [][]byte{bytes.Repeat([]byte("r"), 2*1024*1024)}}
	body, _, stop, err := a.streamEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("encoder was not released after rejection")
	}
}

type countingWriter struct {
	bytes.Buffer
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) { w.writes++; return w.Buffer.Write(p) }

func TestObservedBodyPreservesWriterToAndExactBytes(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	s, ok := bind(context.Background(), c, 1, 2, "").Value(scopeKey).(*scope)
	if !ok {
		t.Fatal("missing scope")
	}
	a := s.begin(3, "", "/v1/responses", "POST", "JSON")
	raw := bytes.Repeat([]byte("x"), 1024*1024)
	body := &observedBody{ReadCloser: io.NopCloser(bytes.NewReader(raw)), attempt: a, side: "request"}
	w := &countingWriter{}
	n, err := io.Copy(w, body)
	if err != nil || n != int64(len(raw)) || w.writes != 1 || !bytes.Equal(w.Bytes(), raw) {
		t.Fatal("body wrapper lost the underlying WriterTo path")
	}
	a.prepareLocked()
	if !bytes.Equal(a.row.Request, raw) || !a.requestDone {
		t.Fatal("fast copy lost archive bytes or completion")
	}
	a.clearLocked()
}

func TestFrameObservationQueuesBytesWithoutWaitingForParsingAndDiscardsOnPressure(t *testing.T) {
	c := &collector{queue: make(chan *attempt, 32), frames: make(chan *frameObservation, 2), cached: policy{Version: 1, Enabled: true}, policyUntil: time.Now().Add(time.Hour)}
	ctx := bind(context.Background(), c, 1, 2, "")
	body := []byte(`{"model":"test","input":"` + strings.Repeat("x", 1024*1024) + `"}`)
	RecordFrame(ctx, 3, "out", body, false)
	body[0] = '!'
	if len(c.frames) != 1 {
		t.Fatal("frame parsing or delivery ran on the observer")
	}
	work := <-c.frames
	if work.frame.Body[0] != '{' {
		t.Fatal("queued frame aliases inference bytes")
	}
	c.frames <- work
	RecordFrame(ctx, 3, "in", []byte(`{"delta":"x"}`), false)
	RecordFrame(ctx, 3, "in", []byte(`{"type":"response.completed"}`), false)
	close(c.frames)
	c.runFrames()
	if c.used.Load() != 0 || len(c.queue) != 0 || c.dropped.Load() != 1 {
		t.Fatal("overflow retained partial bytes, leaked memory or failed to count the dropped archive")
	}
}

func TestCachedPolicyContentionNeverWaitsForRefresh(t *testing.T) {
	c := &collector{cached: policy{Version: 1, Enabled: true}, policyUntil: time.Now().Add(time.Hour)}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	returned := make(chan struct{})
	go func() { _ = bind(context.Background(), c, 1, 2, ""); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("inference waited for a policy refresh")
	}
}

func TestSinglePassModelsMatchesCoreCandidateParser(t *testing.T) {
	for _, text := range []string{`{}`, `null`, `{"model":"gpt-6-sol","Model":"other"}`, `{"model":"","MODEL":null,"session":{"model":"a","MODEL":"b"}}`, `{"session":{"model":"a"},"Session":{"model":"b"}}`, `{"session":[{"model":"a"}],"model":" top "}`, `{"mod\u0065l":"escaped"}`, `{"model":12,"session":true}`} {
		expected := requestmodel.FromBodyCandidates("", "application/json", []byte(text))
		if actual := bodyModels([]byte(text), "application/json", false); !reflect.DeepEqual(expected, actual) {
			t.Errorf("candidate parser differs: %s: %v / %v", text, expected, actual)
		}
	}
}

func BenchmarkHTTPRelay(b *testing.B) {
	for _, size := range []int{16 * 1024, 1024 * 1024, 8 * 1024 * 1024} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("request=%d/capture=%t", size, enabled), func(b *testing.B) {
				request := []byte(`{"model":"test","input":"` + strings.Repeat("r", size) + `"}`)
				response := bytes.Repeat([]byte("s"), 256*1024)
				c := &collector{queue: make(chan *attempt, 32), cached: policy{Version: 1, Enabled: true}, policyUntil: time.Now().Add(time.Hour)}
				buffer := make([]byte, 16384)
				base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					for {
						if _, err := req.Body.Read(buffer); err != nil {
							break
						}
					}
					_ = req.Body.Close()
					return &http.Response{StatusCode: 200, Header: http.Header{}, ContentLength: int64(len(response)), Body: io.NopCloser(bytes.NewReader(response))}, nil
				})
				transport := WrapTransport(base)
				var first, total time.Duration
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					ctx := context.Background()
					if enabled {
						ctx = bind(ctx, c, 1, 2, "")
					}
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://upstream.example/v1/responses", bytes.NewReader(request))
					resp, err := transport.RoundTrip(req)
					if err != nil {
						b.Fatal(err)
					}
					_, _ = resp.Body.Read(buffer)
					first += time.Since(start)
					for {
						if _, err := resp.Body.Read(buffer); err != nil {
							break
						}
					}
					_ = resp.Body.Close()
					total += time.Since(start)
					if enabled {
						a := <-c.queue
						a.clearLocked()
					}
				}
				b.ReportMetric(float64(first.Nanoseconds())/float64(b.N)/1e3, "first-us/op")
				b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/1e3, "total-us/op")
			})
		}
	}
}

func BenchmarkWebSocketObserver(b *testing.B) {
	for _, size := range []int{1024, 1024 * 1024, 8 * 1024 * 1024} {
		for _, direction := range []string{"in", "out"} {
			b.Run(fmt.Sprintf("bytes=%d/direction=%s", size, direction), func(b *testing.B) {
				body := []byte(`{"model":"test","delta":"` + strings.Repeat("x", size) + `"}`)
				c := &collector{frames: make(chan *frameObservation, 1), cached: policy{Version: 1, Enabled: true}, policyUntil: time.Now().Add(time.Hour)}
				ctx := bind(context.Background(), c, 1, 2, "")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					RecordFrame(ctx, 3, direction, body, false)
					work := <-c.frames
					work.scope.pending.Add(-1)
					c.used.Add(-work.used)
				}
			})
		}
	}
}

func BenchmarkBackgroundEnvelope(b *testing.B) {
	request := []byte(`{"model":"test","input":"` + strings.Repeat("r", 32*1024*1024) + `"}`)
	response := bytes.Repeat([]byte("s"), 1024*1024)
	a := &attempt{row: event{Protocol: "JSON", Frames: []frame{}}, contentType: "application/json"}
	for len(request) > 0 {
		n := min(16384, len(request))
		a.requestChunks = append(a.requestChunks, request[:n])
		request = request[n:]
	}
	a.responseChunks = [][]byte{response}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.models = nil
		a.prepareDelivery()
		a.row.BodySHA256 = a.capturedDigest()
		body, _, stop, err := a.streamEnvelope()
		if err != nil {
			b.Fatal(err)
		}
		_, err = io.Copy(io.Discard, body)
		stop()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTPStreaming(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("capture=%t", enabled), func(b *testing.B) {
			p := policy{Version: 1, CaptureVersion: 2, Enabled: true}
			archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(p)
					return
				}
				var row event
				if err := json.NewDecoder(r.Body).Decode(&row); err != nil {
					b.Error(err)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "id": row.ID, "body_sha256": bodyDigest(row)})
			}))
			defer archive.Close()
			c := &collector{base: archive.URL, token: "test", http: archive.Client(), queue: make(chan *attempt, 32), cached: p, policyUntil: time.Now().Add(time.Hour)}
			drained := make(chan struct{})
			go func() { c.run(); close(drained) }()
			defer func() { close(c.queue); <-drained }()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				time.Sleep(2 * time.Millisecond)
				w.Header().Set("Content-Type", "text/event-stream")
				for i := 0; i < 8; i++ {
					_, _ = fmt.Fprintf(w, "data: {\"delta\":\"%s\"}\n\n", strings.Repeat("x", 1024))
					_ = http.NewResponseController(w).Flush()
					time.Sleep(time.Millisecond)
				}
			}))
			defer upstream.Close()
			transport := &http.Transport{Proxy: nil}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: WrapTransport(transport)}
			request := []byte(`{"model":"test","input":"` + strings.Repeat("r", 1024*1024) + `"}`)
			buffer := make([]byte, 16384)
			var first, total []time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				ctx := context.Background()
				if enabled {
					ctx = bind(ctx, c, 1, 2, "")
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, upstream.URL+"/v1/responses", bytes.NewReader(request))
				resp, err := client.Do(req)
				if err != nil {
					b.Fatal(err)
				}
				_, _ = resp.Body.Read(buffer)
				first = append(first, time.Since(start))
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				total = append(total, time.Since(start))
			}
			b.StopTimer()
			slices.Sort(first)
			slices.Sort(total)
			for name, samples := range map[string][]time.Duration{"first": first, "total": total} {
				b.ReportMetric(float64(samples[len(samples)/2].Microseconds())/1000, name+"-p50-ms")
				b.ReportMetric(float64(samples[(len(samples)-1)*95/100].Microseconds())/1000, name+"-p95-ms")
			}
		})
	}
}
