package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/conversationlog"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

type tailArchive struct {
	ID          string `json:"id"`
	BodySHA256  string `json:"body_sha256"`
	Request     []byte `json:"request"`
	Response    []byte `json:"response"`
	Complete    bool   `json:"complete"`
	Reason      string `json:"reason"`
	Encoding    string `json:"encoding"`
	Status      int    `json:"status"`
	Diagnostics struct {
		Response struct {
			Bytes int64  `json:"bytes"`
			Error string `json:"error"`
		} `json:"response"`
	} `json:"transport_diagnostics"`
}

func (a tailArchive) digest() string {
	h := sha256.New()
	writeSize := func(n int) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		_, _ = h.Write(b[:])
	}
	for _, body := range [][]byte{a.Request, a.Response} {
		writeSize(len(body))
		_, _ = h.Write(body)
	}
	writeSize(0)
	return hex.EncodeToString(h.Sum(nil))
}

const tailRequest = "{\n  \"model\": \"gpt-6-sol\", \"input\": [{\"role\":\"user\",\"content\":\"fixture\"}]\n}"

func tailRequestFor(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(tailRequest))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func requireTailCloseReturns(t *testing.T, closeBody func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- closeBody() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("caller waited for a background tail")
	}
}

func requireTailReleased(t *testing.T, s *httpUpstreamService) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, entry := range s.clients {
			if atomic.LoadInt64(&entry.inFlight) != 0 {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond)
}

func requireFollowingResponse(t *testing.T, srv *httptest.Server, do func(*http.Request) (*http.Response, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/following", nil)
	require.NoError(t, err)
	resp, err := do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "following response", string(body), "old read must never consume the next response")
}

func TestHTTPUpstreamConversationTailLifecycle(t *testing.T) {
	// Bind initializes a process-global collector. A child keeps repeated runs
	// independent and leaves other repository tests' environment untouched.
	if os.Getenv("SUB2API_TAIL_TEST_CHILD") != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHTTPUpstreamConversationTailLifecycle$", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), "SUB2API_TAIL_TEST_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	runHTTPUpstreamConversationTailLifecycle(t)
}

func runHTTPUpstreamConversationTailLifecycle(t *testing.T) {
	archives := make(chan tailArchive, 32)
	token := strings.Repeat("fixture-", 8)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, `{"version":1,"enabled":true,"capture_version":2,"transport_diagnostics_version":1}`)
			return
		}
		var archive tailArchive
		if json.NewDecoder(r.Body).Decode(&archive) != nil || archive.digest() != archive.BodySHA256 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		archives <- archive
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "id": archive.ID, "body_sha256": archive.BodySHA256})
	}))
	defer receiver.Close()
	t.Setenv("CONVERSATION_LOGS_URL", receiver.URL)
	t.Setenv("CONVERSATION_LOGS_TOKEN", token)
	bind := func(ctx context.Context) context.Context { return conversationlog.Bind(ctx, 1, 2, "fixture") }
	require.Eventually(t, func() bool { return conversationlog.Enabled(bind(t.Context())) }, 3*time.Second, 5*time.Millisecond)
	nextArchive := func(t *testing.T) tailArchive {
		t.Helper()
		select {
		case archive := <-archives:
			require.Equal(t, tailRequest, string(archive.Request))
			require.Equal(t, int64(len(archive.Response)), archive.Diagnostics.Response.Bytes)
			return archive
		case <-time.After(3 * time.Second):
			t.Fatal("archive not acknowledged by the digest-verifying receiver")
			return tailArchive{}
		}
	}

	errorBody := []byte(`{"error":"failed","extra":"` + strings.Repeat("opaque error bytes ", 30000) + `","number":1.2300}`)
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	compressedSplit := 0
	for i := 0; i < 60000; i++ {
		_, _ = fmt.Fprintf(zw, "%08x-%08x ", i*1664525+1013904223, i*22695477+1)
		if i == 1023 {
			require.NoError(t, zw.Flush())
			compressedSplit = compressed.Len()
		}
	}
	require.NoError(t, zw.Close())
	terminal := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"fixture\"}}}\n\n")
	sseBody := append(append([]byte(nil), terminal...), []byte(": exact trailing bytes\r\n\r\n")...)
	for _, tls := range []bool{false, true} {
		for _, sample := range []struct {
			name, encoding string
			status         int
			body           []byte
			split, read    int
			terminal       bool
		}{
			{"oversized-error", "", 503, errorBody, 1025, 1025, false},
			{"gzip-error", "gzip", 500, compressed.Bytes(), compressedSplit, 256, false},
			{"failed-terminal-in-flight", "", 200, sseBody, len(terminal), len(terminal), true},
		} {
			t.Run(fmt.Sprintf("%s/tls=%v", sample.name, tls), func(t *testing.T) {
				release := make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/following" {
						_, _ = io.WriteString(w, "following response")
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if sample.encoding != "" {
						w.Header().Set("Content-Encoding", sample.encoding)
					}
					if sample.terminal {
						w.Header().Set("Content-Type", "text/event-stream")
					} else {
						w.Header().Set("Content-Length", strconv.Itoa(len(sample.body)))
					}
					w.WriteHeader(sample.status)
					_, _ = w.Write(sample.body[:sample.split])
					_ = http.NewResponseController(w).Flush()
					select {
					case <-release:
						_, _ = w.Write(sample.body[sample.split:])
					case <-r.Context().Done():
					}
				})
				var srv *httptest.Server
				if tls {
					srv = httptest.NewTLSServer(handler)
				} else {
					srv = httptest.NewServer(handler)
				}
				defer srv.Close()
				defer srv.CloseClientConnections()
				s, do := lifecycleClient(t, srv, tls)
				ctx, cancel := context.WithTimeout(bind(t.Context()), 5*time.Second)
				defer cancel()
				req := tailRequestFor(t, ctx, srv.URL)
				req.Header.Set("Accept-Encoding", sample.encoding)
				resp, err := do(req)
				require.NoError(t, err)
				prefix := make([]byte, sample.read)
				_, err = io.ReadFull(resp.Body, prefix)
				require.NoError(t, err)
				var readDone chan error
				if sample.terminal {
					conversationlog.MarkResponseTerminal(resp)
					started := make(chan struct{})
					body := resp.Body.(*trackedBody).ReadCloser.(*cancelOnCloseBody)
					body.ReadCloser = &notifyReadCloser{ReadCloser: body.ReadCloser, started: started}
					readDone = make(chan error, 1)
					go func() { _, err := resp.Body.Read(make([]byte, 1)); readDone <- err }()
					<-started
				}
				var cleaned atomic.Int32
				requireTailCloseReturns(t, func() error {
					return conversationlog.CloseResponse(resp.Body, func() { cleaned.Add(1); cancel() })
				})
				require.True(t, conversationlog.ResponseTailOf(resp.Body).Closing())
				require.NoError(t, resp.Body.Close(), "repeated close must not cancel an accepted tail")
				require.Zero(t, cleaned.Load(), "cleanup must wait until physical close")
				_, err = resp.Body.Read(make([]byte, 1))
				require.ErrorIs(t, err, http.ErrBodyReadAfterClose)
				for _, entry := range s.clients {
					require.EqualValues(t, 1, atomic.LoadInt64(&entry.inFlight))
				}
				once.Do(func() { close(release) })
				if readDone != nil {
					require.NoError(t, <-readDone)
				}
				archive := nextArchive(t)
				require.Equal(t, sample.body, archive.Response)
				require.True(t, archive.Complete, "HTTP/model failure is independent of raw completeness")
				require.Empty(t, archive.Reason)
				require.Equal(t, sample.encoding, archive.Encoding)
				require.Equal(t, sample.status, archive.Status)
				requireTailReleased(t, s)
				require.Eventually(t, func() bool { return cleaned.Load() == 1 }, time.Second, time.Millisecond)
				requireFollowingResponse(t, srv, do)
			})
		}
	}

	for _, scenario := range []string{"idle-budget", "new-request-preempts", "newer-request-disables-tail", "ordinary-close", "client-gone", "short-content-length"} {
		t.Run(scenario, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/following" {
					_, _ = io.WriteString(w, "following response")
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				if scenario == "short-content-length" {
					w.Header().Set("Content-Length", strconv.Itoa(len(terminal)+100))
				}
				_, _ = w.Write(terminal)
				_ = http.NewResponseController(w).Flush()
				if scenario != "short-content-length" {
					<-r.Context().Done()
				}
			}))
			defer srv.Close()
			defer srv.CloseClientConnections()
			s, do := lifecycleClient(t, srv, false)
			ctx, cancel := context.WithCancel(bind(t.Context()))
			defer cancel()
			if scenario == "client-gone" {
				ctx = pkghttputil.DetachUpstreamContext(ctx)
			}
			resp, err := do(tailRequestFor(t, ctx, srv.URL))
			require.NoError(t, err)
			_, err = io.ReadFull(resp.Body, make([]byte, len(terminal)))
			require.NoError(t, err)
			if scenario != "ordinary-close" {
				conversationlog.MarkResponseTerminal(resp)
			}
			if scenario == "client-gone" {
				cancel()
			}
			var followingDone chan error
			if scenario == "newer-request-disables-tail" {
				followingDone = make(chan error, 1)
				go func() {
					req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/following", nil)
					if err == nil {
						var response *http.Response
						response, err = do(req)
						if err == nil {
							var body []byte
							body, err = io.ReadAll(response.Body)
							_ = response.Body.Close()
							if err == nil && string(body) != "following response" {
								err = fmt.Errorf("following response was changed")
							}
						}
					}
					followingDone <- err
				}()
				require.Eventually(t, func() bool {
					for _, entry := range s.clients {
						if atomic.LoadInt64(&entry.inFlight) == 2 {
							return true
						}
					}
					return false
				}, time.Second, time.Millisecond)
			}
			requireTailCloseReturns(t, resp.Body.Close)
			if scenario == "new-request-preempts" {
				require.True(t, conversationlog.ResponseTailOf(resp.Body).Closing())
				started := time.Now()
				requireFollowingResponse(t, srv, do)
				require.Less(t, time.Since(started), 150*time.Millisecond, "new request waited for the old tail's idle deadline")
			}
			if followingDone != nil {
				select {
				case err := <-followingDone:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("an older response blocked a newer inference attempt")
				}
			}
			archive := nextArchive(t)
			require.Equal(t, terminal, archive.Response)
			require.False(t, archive.Complete, "terminal event cannot conceal an unfinished HTTP body")
			if scenario == "idle-budget" || scenario == "short-content-length" || scenario == "new-request-preempts" {
				if scenario == "new-request-preempts" {
					require.Contains(t, []string{"read_error", "closed_early"}, archive.Reason)
				} else {
					require.Equal(t, "read_error", archive.Reason)
				}
				errorKind := "cancelled"
				if scenario == "short-content-length" {
					errorKind = "unexpected_eof"
				}
				if archive.Reason == "read_error" {
					require.Equal(t, errorKind, archive.Diagnostics.Response.Error)
				}
			} else {
				require.Equal(t, "closed_early", archive.Reason)
				require.False(t, conversationlog.ResponseTailOf(resp.Body).Closing())
			}
			requireTailReleased(t, s)
			requireFollowingResponse(t, srv, do)
		})
	}

	t.Run("zstd-async-prefetch-keeps-native-close", func(t *testing.T) {
		var encoded bytes.Buffer
		encoder, err := zstd.NewWriter(&encoded)
		require.NoError(t, err)
		_, err = encoder.Write([]byte(strings.Repeat("native decoder bytes", 1000)))
		require.NoError(t, err)
		require.NoError(t, encoder.Flush())
		split := encoded.Len()
		_, err = encoder.Write([]byte(strings.Repeat("unread remaining bytes", 1000)))
		require.NoError(t, err)
		require.NoError(t, encoder.Close())
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/following" {
				_, _ = io.WriteString(w, "following response")
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Encoding", "zstd")
			w.Header().Set("Content-Length", strconv.Itoa(encoded.Len()))
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write(encoded.Bytes()[:split])
			_ = http.NewResponseController(w).Flush()
			<-r.Context().Done()
		}))
		defer srv.Close()
		defer srv.CloseClientConnections()
		s, do := lifecycleClient(t, srv, false)
		ctx, cancel := context.WithTimeout(bind(t.Context()), 5*time.Second)
		defer cancel()
		req := tailRequestFor(t, ctx, srv.URL)
		req.Header.Set("Accept-Encoding", "zstd")
		resp, err := do(req)
		require.NoError(t, err)
		require.Nil(t, conversationlog.ResponseTailOf(resp.Body), "async decompression must not get a second raw reader")
		prefix := make([]byte, len("native decoder bytes"))
		_, err = io.ReadFull(resp.Body, prefix)
		require.NoError(t, err)
		require.Equal(t, "native decoder bytes", string(prefix))
		requireTailCloseReturns(t, resp.Body.Close)
		archive := nextArchive(t)
		require.False(t, archive.Complete)
		require.Equal(t, "zstd", archive.Encoding)
		require.NotEmpty(t, archive.Response)
		require.True(t, bytes.HasPrefix(encoded.Bytes(), archive.Response))
		requireTailReleased(t, s)
		requireFollowingResponse(t, srv, do)
	})

	t.Run("http2-tail-yield-preserves-active-stream", func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			switch r.URL.Path {
			case "/following":
				_, _ = io.WriteString(w, "following response")
			case "/active":
				_, _ = io.WriteString(w, "active response ")
				_ = http.NewResponseController(w).Flush()
				select {
				case <-release:
					_, _ = io.WriteString(w, "unchanged")
				case <-r.Context().Done():
				}
			default:
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write(terminal)
				_ = http.NewResponseController(w).Flush()
				<-r.Context().Done()
			}
		}))
		srv.EnableHTTP2 = true
		srv.StartTLS()
		defer srv.Close()
		defer srv.CloseClientConnections()
		s, do := lifecycleClient(t, srv, false)
		ctx, cancel := context.WithTimeout(bind(t.Context()), 5*time.Second)
		defer cancel()
		active, err := do(tailRequestFor(t, ctx, srv.URL+"/active"))
		require.NoError(t, err)
		require.Equal(t, 2, active.ProtoMajor)
		_, err = io.ReadFull(active.Body, make([]byte, len("active response ")))
		require.NoError(t, err)
		held, err := do(tailRequestFor(t, ctx, srv.URL+"/tail"))
		require.NoError(t, err)
		_, err = io.ReadFull(held.Body, make([]byte, len(terminal)))
		require.NoError(t, err)
		requireTailCloseReturns(t, held.Body.Close)
		require.True(t, conversationlog.ResponseTailOf(held.Body).Closing())
		requireFollowingResponse(t, srv, do)
		incomplete := nextArchive(t)
		require.False(t, incomplete.Complete)
		require.Equal(t, terminal, incomplete.Response)
		once.Do(func() { close(release) })
		rest, err := io.ReadAll(active.Body)
		require.NoError(t, err, "yielding a tail cancelled a native foreground HTTP/2 stream")
		require.Equal(t, "unchanged", string(rest))
		require.NoError(t, active.Body.Close())
		complete := nextArchive(t)
		require.True(t, complete.Complete)
		require.Equal(t, "active response unchanged", string(complete.Response))
		requireTailReleased(t, s)
	})
}
