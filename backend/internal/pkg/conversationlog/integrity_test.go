package conversationlog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBodiesLargerThanLegacyLimitsRemainByteExact(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, Limit: 65536})
	request := `{"model":"test-model","input":"` + strings.Repeat("r", 18*1024*1024) + `","n":1.2300}`
	response := []byte(strings.Repeat("s", 18*1024*1024) + "END_OF_RESPONSE")
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != request {
			t.Fatal("inference request was changed")
		}
		_ = r.Body.Close()
		return &http.Response{StatusCode: 200, ContentLength: int64(len(response)), Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(response))}, nil
	})
	req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/v1/responses", strings.NewReader(request))
	resp, err := WrapTransport(base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(actual, response) {
		t.Fatal("inference response was changed")
	}
	_ = resp.Body.Close()
	row := next(t, c)
	if !row.Complete || row.Reason != "" || string(row.Request) != request || !bytes.Equal(row.Response, response) {
		t.Fatal("large body was truncated or reformatted")
	}
	if c.used.Load() != 0 || c.dropped.Load() != 0 {
		t.Fatal("large archive leaked its budget or was dropped")
	}
}

func TestArchiveOutageAndStalePolicyNeverCallNetworkOnInference(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	c.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("archive network was called on the inference path")
		return nil, nil
	})
	for _, fresh := range []bool{true, false} {
		if !fresh {
			c.policyMu.Lock()
			c.policyUntil = time.Now().Add(-time.Second)
			c.policyMu.Unlock()
		}
		req, _ := http.NewRequestWithContext(bind(context.Background(), c, 1, 2, ""), "POST", "https://upstream.example/v1/responses", strings.NewReader("{}"))
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("unchanged"))}, nil
		})
		resp, err := WrapTransport(base).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || string(body) != "unchanged" {
			t.Fatal("archive outage affected inference")
		}
		if fresh {
			next(t, c)
		}
	}
}

func TestMoreThanTenThousandWebSocketFramesAreNotTruncated(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true, Limit: 65536})
	ctx := bind(context.Background(), c, 1, 2, "")
	RecordFrame(ctx, 3, "out", []byte(`{"type":"response.create","model":"test"}`), false)
	for i := 0; i < 10001; i++ {
		RecordFrame(ctx, 3, "in", []byte(`{"type":"response.output_text.delta","delta":"x"}`), false)
	}
	RecordFrame(ctx, 3, "in", []byte(`{"type":"response.completed"}`), false)
	row := next(t, c)
	if !row.Complete || len(row.Frames) != 10003 {
		t.Fatal("WebSocket frame count was truncated")
	}
}

func TestDeliveryRetriesSameEventAndRejectsFalseAcknowledgments(t *testing.T) {
	for _, valid := range []bool{true, false} {
		c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
		base := c.http.Transport
		posts := 0
		var first string
		c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != "POST" {
				return base.RoundTrip(r)
			}
			var row event
			if err := json.NewDecoder(r.Body).Decode(&row); err != nil {
				t.Fatal(err)
			}
			posts++
			if first == "" {
				first = row.ID
			} else if row.ID != first {
				t.Fatal("retry changed the archive ID")
			}
			if posts == 1 {
				return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}
			digest := row.BodySHA256
			if !valid {
				digest = strings.Repeat("0", 64)
			}
			ack, _ := json.Marshal(map[string]any{"accepted": true, "id": row.ID, "body_sha256": digest})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(ack))}, nil
		})
		ctx := bind(context.Background(), c, 1, 2, "")
		RecordFrame(ctx, 3, "out", []byte(`{"model":"test"}`), false)
		RecordFrame(ctx, 3, "in", []byte(`{"type":"response.completed"}`), false)
		flushFrames(c)
		close(c.queue)
		c.run()
		if valid && (posts != 2 || c.dropped.Load() != 0) || !valid && (posts != 3 || c.dropped.Load() != 1) {
			t.Fatal("delivery did not verify acknowledgment or retry bound")
		}
		if c.used.Load() != 0 {
			t.Fatal("delivery leaked captured memory")
		}
	}
}

func TestBodyDigestMatchesArchiveBinaryVector(t *testing.T) {
	row := event{Request: []byte("request\n"), Response: []byte{0, 255, 13, 10}, Frames: []frame{
		{Direction: "out", At: 12, Body: []byte("hi")}, {Direction: "in", At: 20, Binary: true, Body: []byte{0, 255}},
	}}
	if bodyDigest(row) != "3c5432745365d3316265a82af5cf5899adda7fdf53ba8f201d24b4a6d93a1d47" {
		t.Fatal("archive digest contract changed")
	}
}

func TestDeliveryNeverLocksLateObserverReads(t *testing.T) {
	c, _, _ := fixture(t, policy{Version: 1, Enabled: true})
	base := c.http.Transport
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != "POST" {
			return base.RoundTrip(r)
		}
		var row event
		if err := json.NewDecoder(r.Body).Decode(&row); err != nil {
			t.Error(err)
		}
		close(entered)
		<-release
		ack, _ := json.Marshal(map[string]any{"accepted": true, "id": row.ID, "body_sha256": row.BodySHA256})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(ack))}, nil
	})
	s, ok := bind(context.Background(), c, 1, 2, "").Value(scopeKey).(*scope)
	if !ok {
		t.Fatal("missing scope")
	}
	a := s.begin(3, "upstream.example", "/v1/responses", "POST", "JSON")
	a.append("request", []byte("{}"), io.EOF)
	a.finish(true, "")
	close(c.queue)
	go func() { c.run(); close(stopped) }()
	defer func() { close(release); <-stopped }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	returned := make(chan struct{})
	go func() { a.append("request", []byte("late"), nil); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Error("archive delivery blocked an inference observer")
	}
}
