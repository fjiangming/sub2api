// Package conversationlog observes upstream application bytes independently of inference and billing.
package conversationlog

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type contextKey uint8

const (
	scopeKey contextKey = iota
	accountKey
)

type policy struct {
	Version  int     `json:"version"`
	Enabled  bool    `json:"enabled"`
	Excluded []int64 `json:"excluded_user_ids"`
	Limit    int     `json:"max_body_bytes"`
}
type frame struct {
	Direction string `json:"direction"`
	At        int64  `json:"at"`
	Body      []byte `json:"body"`
	Binary    bool   `json:"binary"`
}
type event struct {
	Schema        int     `json:"schema"`
	ID            string  `json:"id"`
	RequestID     string  `json:"request_id"`
	UserID        int64   `json:"user_id"`
	KeyID         int64   `json:"key_id"`
	AccountID     int64   `json:"account_id"`
	Session       string  `json:"session_id"`
	At            int64   `json:"at"`
	Duration      int64   `json:"duration_ms"`
	Host          string  `json:"host"`
	Path          string  `json:"path"`
	Method        string  `json:"method"`
	Protocol      string  `json:"protocol"`
	Model         string  `json:"model"`
	Status        int     `json:"status"`
	Complete      bool    `json:"complete"`
	Reason        string  `json:"reason"`
	PolicyVersion int     `json:"policy_version"`
	Encoding      string  `json:"encoding"`
	Request       []byte  `json:"request"`
	Response      []byte  `json:"response"`
	Frames        []frame `json:"frames"`
}
type collector struct {
	base, token string
	http        *http.Client
	queue       chan *attempt
	used        atomic.Int64
	dropped     atomic.Uint64
}
type scope struct {
	client            *collector
	user, key         int64
	id, session, path string
	mu                sync.Mutex
	ws                *attempt
	wsDenied          bool
}
type attempt struct {
	client                    *collector
	mu                        sync.Mutex
	row                       event
	start                     time.Time
	limit                     int
	used                      int64
	finished                  bool
	requestDone               bool
	responseDone              bool
	requestSize, responseSize int64
}

var globalOnce sync.Once
var global *collector
var modelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,119}$`)

func current() *collector {
	globalOnce.Do(func() {
		base, token := strings.TrimRight(os.Getenv("CONVERSATION_LOGS_URL"), "/"), os.Getenv("CONVERSATION_LOGS_TOKEN")
		u, err := url.Parse(base)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(token) < 32 {
			return
		}
		ip := net.ParseIP(u.Hostname())
		local := u.Hostname() == "localhost" || (ip == nil && !strings.ContainsAny(u.Hostname(), ".:")) || (ip != nil && (ip.IsLoopback() || ip.IsPrivate()))
		if u.Scheme != "https" && (u.Scheme != "http" || !local) {
			return
		}
		global = newCollector(base, token)
	})
	return global
}
func newCollector(base, token string) *collector {
	c := &collector{base: base, token: token, queue: make(chan *attempt, 32), http: &http.Client{
		Timeout:       2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{Timeout: 200 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext},
	}}
	go c.run()
	return c
}
func randomID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(id[:])
}

// Bind accepts only IDs obtained by API-key authentication. Client body/header user IDs are never used.
func Bind(ctx context.Context, userID, keyID int64, session string, paths ...string) context.Context {
	result := bind(ctx, current(), userID, keyID, session)
	if s, _ := result.Value(scopeKey).(*scope); s != nil && len(paths) > 0 && strings.HasPrefix(paths[0], "/") && len(paths[0]) <= 1024 {
		s.path = paths[0]
	}
	return result
}
func bind(ctx context.Context, c *collector, userID, keyID int64, session string) context.Context {
	if c == nil || userID <= 0 || keyID <= 0 {
		return ctx
	}
	s := &scope{client: c, user: userID, key: keyID, id: randomID()}
	if s.id == "" {
		return ctx
	}
	if session != "" && len(session) <= 8192 {
		mac := hmac.New(sha256.New, []byte(c.token))
		_, _ = mac.Write([]byte(strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(keyID, 10) + ":" + session))
		s.session = hex.EncodeToString(mac.Sum(nil))
	}
	return context.WithValue(ctx, scopeKey, s)
}
func WithAccount(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, accountKey, id)
}
func Enabled(ctx context.Context) bool { return ctx != nil && ctx.Value(scopeKey) != nil }

// CaptureContext copies identity values without changing the caller's deadline or cancellation behavior.
func CaptureContext(ctx, owner context.Context, account int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if owner == nil || owner.Value(scopeKey) == nil {
		return ctx
	}
	return WithAccount(context.WithValue(ctx, scopeKey, owner.Value(scopeKey)), account)
}
func Account(ctx context.Context) int64 { value, _ := ctx.Value(accountKey).(int64); return value }
func Finish(ctx context.Context) {
	s, _ := ctx.Value(scopeKey).(*scope)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ws != nil {
		s.ws.finish(false, "session_closed")
		s.ws = nil
	}
}
func (c *collector) getPolicy() (policy, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/api/conversation-logs/collector-policy", nil)
	if err != nil {
		return policy{}, false
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Conversation-Dropped", strconv.FormatUint(c.dropped.Load(), 10))
	resp, err := c.http.Do(req)
	if err != nil {
		return policy{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return policy{}, false
	}
	var p policy
	err = json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&p)
	return p, err == nil && p.Version > 0 && p.Limit >= 65536 && p.Limit <= 16777216 && len(p.Excluded) <= 1000
}
func (p policy) allows(user int64) bool {
	if !p.Enabled {
		return false
	}
	for _, id := range p.Excluded {
		if id == user {
			return false
		}
	}
	return true
}
func (s *scope) begin(account int64, host, path, method, protocol string) *attempt {
	p, ok := s.client.getPolicy()
	if !ok || !p.allows(s.user) {
		if !ok {
			s.client.dropped.Add(1)
		}
		return nil
	}
	id := randomID()
	if id == "" {
		return nil
	}
	now := time.Now()
	return &attempt{client: s.client, limit: p.Limit, start: now, row: event{Schema: 1, ID: id, RequestID: s.id,
		UserID: s.user, KeyID: s.key, AccountID: account, Session: s.session, At: now.UnixMilli(), Host: host, Path: path, Method: method,
		Protocol: protocol, PolicyVersion: p.Version, Request: []byte{}, Response: []byte{}, Frames: []frame{}}}
}
func (a *attempt) clearLocked() {
	a.client.used.Add(-a.used)
	a.used = 0
	a.row.Request, a.row.Response, a.row.Frames = []byte{}, []byte{}, []frame{}
}
func (a *attempt) reserveLocked(n int) bool {
	if a.row.Reason == "body_limit" || a.row.Reason == "memory_limit" {
		return false
	}
	if a.used+int64(n) > int64(a.limit) {
		a.row.Reason = "body_limit"
		a.clearLocked()
		return false
	}
	for {
		used := a.client.used.Load()
		if used+int64(n) > 64*1024*1024 {
			a.row.Reason = "memory_limit"
			a.clearLocked()
			return false
		}
		if a.client.used.CompareAndSwap(used, used+int64(n)) {
			a.used += int64(n)
			return true
		}
	}
}
func (a *attempt) append(side string, data []byte, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return
	}
	if side == "request" {
		a.requestSize += int64(len(data))
		if err == io.EOF {
			a.requestDone = true
		}
	} else {
		a.responseSize += int64(len(data))
		if err == io.EOF {
			a.responseDone = true
		}
	}
	if err != nil && err != io.EOF && a.row.Reason == "" {
		a.row.Reason = "read_error"
	}
	if len(data) > 0 && a.reserveLocked(len(data)) {
		if side == "request" {
			a.row.Request = append(a.row.Request, data...)
		} else {
			a.row.Response = append(a.row.Response, data...)
		}
	}
}
func (a *attempt) finish(complete bool, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return
	}
	a.finished = true
	a.row.Duration = min(time.Since(a.start).Milliseconds(), 86400000)
	if a.row.Reason == "" {
		a.row.Reason = reason
	}
	a.row.Complete = complete && a.row.Reason == ""
	if len(a.row.Request) <= 1024*1024 {
		var metadata struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(a.row.Request, &metadata) == nil && modelName.MatchString(metadata.Model) {
			a.row.Model = metadata.Model
		}
	}
	select {
	case a.client.queue <- a:
	default:
		a.client.dropped.Add(1)
		a.clearLocked()
	}
}
func (c *collector) run() {
	for a := range c.queue {
		a.mu.Lock()
		p, ok := c.getPolicy()
		if ok && p.allows(a.row.UserID) {
			body, err := json.Marshal(a.row)
			if err == nil {
				req, buildErr := http.NewRequest(http.MethodPost, c.base+"/api/conversation-logs/ingest", bytes.NewReader(body))
				if buildErr == nil {
					req.Header.Set("Authorization", "Bearer "+c.token)
					req.Header.Set("Content-Type", "application/json")
					resp, sendErr := c.http.Do(req)
					if sendErr != nil {
						c.dropped.Add(1)
					} else {
						if resp.StatusCode != http.StatusOK {
							c.dropped.Add(1)
						}
						_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
						_ = resp.Body.Close()
					}
				}
			}
		} else if !ok {
			c.dropped.Add(1)
		}
		a.clearLocked()
		a.mu.Unlock()
	}
}

type transport struct{ base http.RoundTripper }

// WrapTransport observes each actual RoundTrip, including gateway fallback transports. It never retries inference.
func WrapTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{base}
}
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	s, _ := req.Context().Value(scopeKey).(*scope)
	if s == nil || req.Context().Err() != nil {
		return t.base.RoundTrip(req)
	}
	account, _ := req.Context().Value(accountKey).(int64)
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	a := s.begin(account, req.URL.Hostname(), path, req.Method, "JSON")
	if a == nil {
		return t.base.RoundTrip(req)
	}
	copyReq := req.Clone(req.Context())
	a.requestDone = req.Body == nil || req.Body == http.NoBody
	if req.Body != nil {
		copyReq.Body = &observedBody{ReadCloser: req.Body, attempt: a, side: "request", expected: req.ContentLength}
	}
	resp, err := t.base.RoundTrip(copyReq)
	if err != nil {
		a.finish(false, "transport_error")
		return resp, err
	}
	if resp == nil || resp.Body == nil {
		a.finish(false, "transport_error")
		return resp, err
	}
	a.mu.Lock()
	a.row.Status = resp.StatusCode
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		a.row.Protocol = "SSE"
	}
	a.row.Encoding = strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch a.row.Encoding {
	case "", "identity", "gzip", "br", "deflate", "zstd":
	default:
		a.row.Encoding = "unknown"
	}
	a.mu.Unlock()
	resp.Body = &observedBody{ReadCloser: resp.Body, attempt: a, side: "response", expected: resp.ContentLength}
	return resp, nil
}

type observedBody struct {
	io.ReadCloser
	attempt  *attempt
	side     string
	expected int64
	once     sync.Once
	closeErr error
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.attempt.append(b.side, p[:n], err)
	return n, err
}
func (b *observedBody) Close() error {
	b.once.Do(func() {
		b.closeErr = b.ReadCloser.Close()
		b.attempt.mu.Lock()
		if b.side == "request" && b.expected >= 0 && b.attempt.requestSize == b.expected {
			b.attempt.requestDone = true
		}
		complete := b.attempt.requestDone && (b.attempt.responseDone || (b.expected >= 0 && b.attempt.responseSize == b.expected))
		b.attempt.mu.Unlock()
		if b.side == "response" {
			reason := ""
			if !complete {
				reason = "closed_early"
			}
			b.attempt.finish(complete, reason)
		}
	})
	return b.closeErr
}

// RecordFrame must be called at the lease/relay boundary, never by a pooled connection's background reader.
func RecordFrame(ctx context.Context, account int64, direction string, body []byte, binary bool) {
	s, _ := ctx.Value(scopeKey).(*scope)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wsDenied {
		return
	}
	if s.ws == nil {
		path := s.path
		if path == "" {
			path = "/v1/responses"
		}
		s.ws = s.begin(account, "", path, "WS", "WebSocket")
		if s.ws == nil {
			s.wsDenied = true
			return
		}
		s.ws.row.Status = http.StatusSwitchingProtocols
	}
	a := s.ws
	a.mu.Lock()
	if len(a.row.Frames) >= 10000 {
		a.row.Reason = "body_limit"
		a.clearLocked()
	}
	if a.reserveLocked(len(body)) {
		data := append([]byte{}, body...)
		a.row.Frames = append(a.row.Frames, frame{Direction: direction, At: time.Since(a.start).Milliseconds(), Body: data, Binary: binary})
		if direction == "out" && len(a.row.Request) == 0 {
			a.row.Request = data
		}
		if direction == "in" {
			a.row.Response = data
		}
	}
	a.mu.Unlock()
	if direction == "in" && !binary {
		var message struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(body, &message) == nil && (message.Type == "response.completed" || message.Type == "response.failed" || message.Type == "response.incomplete") {
			a.finish(true, "")
			s.ws = nil
		}
	}
}
