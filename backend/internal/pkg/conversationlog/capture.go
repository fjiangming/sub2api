// Package conversationlog observes upstream application bytes independently of inference and billing.
package conversationlog

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
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
	Version                     int      `json:"version"`
	Enabled                     bool     `json:"enabled"`
	Excluded                    []int64  `json:"excluded_user_ids"`
	Limit                       int      `json:"max_body_bytes"`
	CaptureVersion              int      `json:"capture_version"`
	ModelFilterVersion          int      `json:"model_filter_version"`
	ModelFilterEnabled          bool     `json:"model_filter_enabled"`
	AllowedModels               []string `json:"allowed_models"`
	KimiK3Plus                  bool     `json:"kimi_k3_plus"`
	TransportDiagnosticsVersion int      `json:"transport_diagnostics_version"`
	DurableReceiptVersion       int      `json:"durable_receipt_version"`
}
type frame struct {
	Direction string `json:"direction"`
	At        int64  `json:"at"`
	Body      []byte `json:"body"`
	Binary    bool   `json:"binary"`
}
type event struct {
	Schema               int                   `json:"schema"`
	ID                   string                `json:"id"`
	RequestID            string                `json:"request_id"`
	UserID               int64                 `json:"user_id"`
	KeyID                int64                 `json:"key_id"`
	AccountID            int64                 `json:"account_id"`
	Session              string                `json:"session_id"`
	At                   int64                 `json:"at"`
	Duration             int64                 `json:"duration_ms"`
	Host                 string                `json:"host"`
	Path                 string                `json:"path"`
	Method               string                `json:"method"`
	Protocol             string                `json:"protocol"`
	Model                string                `json:"model"`
	Status               int                   `json:"status"`
	Complete             bool                  `json:"complete"`
	Reason               string                `json:"reason"`
	PolicyVersion        int                   `json:"policy_version"`
	Encoding             string                `json:"encoding"`
	Request              []byte                `json:"request"`
	Response             []byte                `json:"response"`
	Frames               []frame               `json:"frames"`
	CaptureVersion       int                   `json:"capture_version"`
	BodySHA256           string                `json:"body_sha256"`
	TransportDiagnostics *transportDiagnostics `json:"transport_diagnostics,omitempty"`
}
type collector struct {
	base, token string
	http        *http.Client
	queue       chan *attempt
	frames      chan *frameObservation
	used        atomic.Int64
	dropped     atomic.Uint64
	jobs        atomic.Int64
	tails       atomic.Int64
	policyMu    sync.RWMutex
	cached      policy
	policyUntil time.Time
}
type scope struct {
	client            *collector
	user, key         int64
	id, session, path string
	mu                sync.Mutex
	ws                *attempt
	wsModels          []string
	failed            atomic.Bool
	closed            atomic.Bool
	closedAt          atomic.Int64
	pending           atomic.Int64
}
type attempt struct {
	client                              *collector
	mu                                  sync.Mutex
	row                                 event
	start                               time.Time
	used                                int64
	requestChunks, responseChunks       [][]byte
	abandoned                           bool
	finished                            bool
	requestDone                         bool
	responseDone                        bool
	requestSize, responseSize           int64
	policy                              policy
	models                              []string
	contentType                         string
	requestTransport, responseTransport transportState
	observers                           atomic.Int64
	queued, awaiting                    bool
	closeAt                             time.Time
	deliveryTries                       int
	deliveryFrozen                      bool
}

type frameObservation struct {
	scope   *scope
	frame   frame
	account int64
	at      time.Time
	policy  policy
	used    int64
	barrier chan struct{}
}

var globalOnce sync.Once
var global *collector
var modelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._:/\[\]-]{0,199}$`)

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
	c := &collector{base: base, token: token, queue: make(chan *attempt, 32), frames: make(chan *frameObservation, 16384), http: &http.Client{
		Timeout:       2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{Proxy: nil, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{Timeout: 200 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext},
	}}
	go c.run()
	go c.runFrames()
	go c.refreshPolicies()
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
	p, valid := c.cachedPolicy()
	if !valid || !p.allows(userID) {
		if !valid {
			c.dropped.Add(1)
		}
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
	if !s.mu.TryLock() {
		s.failObservation(true)
		s.closed.Store(true)
		return
	}
	s.closedAt.Store(time.Now().UnixNano())
	s.closed.Store(true)
	s.mu.Unlock()
	select {
	case s.client.frames <- &frameObservation{scope: s}:
	default:
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
	req.Header.Set("X-Conversation-Collector-Version", "2")
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
	valid := err == nil && p.Version > 0 && p.CaptureVersion == 2 && len(p.Excluded) <= 1000 && len(p.AllowedModels) <= 100
	if p.ModelFilterEnabled {
		valid = valid && p.ModelFilterVersion == 1
		for _, model := range p.AllowedModels {
			valid = valid && modelName.MatchString(model)
		}
	}
	return p, valid
}

// Policy outages skip observation; request goroutines never perform archive network or disk I/O.
func (c *collector) refreshPolicy() {
	p, ok := c.getPolicy()
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	if ok {
		c.cached, c.policyUntil = p, time.Now().Add(3*time.Second)
	} else {
		c.policyUntil = time.Time{}
	}
}
func (c *collector) refreshPolicies() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		c.refreshPolicy()
		<-ticker.C
	}
}
func (c *collector) cachedPolicy() (policy, bool) {
	if !c.policyMu.TryRLock() {
		return policy{}, false
	}
	defer c.policyMu.RUnlock()
	return c.cached, time.Now().Before(c.policyUntil)
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
	p, ok := s.client.cachedPolicy()
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
	return &attempt{client: s.client, policy: p, start: now,
		requestTransport: transportState{expected: -1}, responseTransport: transportState{expected: -1},
		row: event{Schema: 1, CaptureVersion: 2, ID: id, RequestID: s.id,
			UserID: s.user, KeyID: s.key, AccountID: account, Session: s.session, At: now.UnixMilli(), Host: host, Path: path, Method: method,
			Protocol: protocol, PolicyVersion: p.Version, Request: []byte{}, Response: []byte{}, Frames: []frame{}}}
}
func (a *attempt) clearLocked() {
	if a.queued {
		a.queued = false
		a.client.jobs.Add(-1)
	}
	a.releaseBuffersLocked()
}
func (a *attempt) releaseBuffersLocked() {
	a.client.used.Add(-a.used)
	a.used = 0
	a.row.Request, a.row.Response, a.row.Frames = []byte{}, []byte{}, []frame{}
	a.requestChunks, a.responseChunks = nil, nil
}
func (a *attempt) reserveLocked(n int) bool {
	if a.abandoned {
		return false
	}
	if !a.client.reserveObservation(int64(n)) {
		a.row.Reason = "memory_limit"
		a.abandoned = true
		a.releaseBuffersLocked()
		return false
	}
	a.used += int64(n)
	return true
}
func (c *collector) reserveObservation(n int64) bool {
	for retry := 0; retry < 8; retry++ {
		used := c.used.Load()
		if used+n > 64*1024*1024 {
			return false
		}
		if c.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
	return false
}
func (a *attempt) append(side string, data []byte, err error) {
	a.appendObserved(side, data, err, false)
}
func (a *attempt) appendObserved(side string, data []byte, err error, closing bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return
	}
	state := &a.responseTransport
	if side == "request" {
		state = &a.requestTransport
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
	if err == io.EOF {
		state.eof = true
	} else if err != nil && state.errorKind == "" {
		state.errorKind, state.errorDuringClose = transportErrorKind(err), closing
	}
	if err != nil && err != io.EOF && a.row.Reason == "" {
		a.row.Reason = "read_error"
	}
	// Batch tiny SSE reads without reallocating or repeatedly copying the accumulated body.
	chunks := &a.responseChunks
	if side == "request" {
		chunks = &a.requestChunks
	}
	for len(data) > 0 && !a.abandoned {
		if len(*chunks) == 0 || len((*chunks)[len(*chunks)-1]) == cap((*chunks)[len(*chunks)-1]) {
			if !a.reserveLocked(16384 + 32) {
				break
			}
			*chunks = append(*chunks, make([]byte, 0, 16384))
		}
		last := &(*chunks)[len(*chunks)-1]
		n := min(len(data), cap(*last)-len(*last))
		*last = append(*last, data[:n]...)
		data = data[n:]
	}
}
func (a *attempt) finish(complete bool, reason string) {
	a.finishAt(complete, reason, time.Now())
}
func (a *attempt) finishAt(complete bool, reason string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return
	}
	a.finished = true
	a.row.Duration = max(0, at.Sub(a.start).Milliseconds())
	if a.row.Reason == "" {
		a.row.Reason = reason
	}
	a.row.Complete = complete && a.row.Reason == ""
	a.enqueueLocked()
}
func (a *attempt) enqueueLocked() {
	if a.abandoned {
		a.finished = true
		a.client.dropped.Add(1)
		a.clearLocked()
		return
	}
	if !a.queued {
		if a.client.jobs.Add(1) > 32 {
			a.client.jobs.Add(-1)
			a.finished = true
			a.client.dropped.Add(1)
			a.clearLocked()
			return
		}
		a.queued = true
	}
	select {
	case a.client.queue <- a:
	default:
		a.finished = true
		a.client.dropped.Add(1)
		a.clearLocked()
	}
}

func (a *attempt) prepareLocked() {
	if a.row.Protocol != "WebSocket" {
		a.row.Request, a.row.Response = bytes.Join(a.requestChunks, nil), bytes.Join(a.responseChunks, nil)
		a.requestChunks, a.responseChunks = nil, nil
	}
	a.identifyLocked()
	a.setTransportDiagnostics(a.policy.TransportDiagnosticsVersion == 1)
}

func bodyDigest(row event) string {
	h := sha256.New()
	writeNumber := func(n uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); _, _ = h.Write(b[:]) }
	writeBody := func(b []byte) { writeNumber(uint64(len(b))); _, _ = h.Write(b) }
	writeBody(row.Request)
	writeBody(row.Response)
	writeNumber(uint64(len(row.Frames)))
	for _, f := range row.Frames {
		_, _ = h.Write([]byte(f.Direction))
		writeNumber(uint64(f.At))
		if f.Binary {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
		writeBody(f.Body)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// deliver performs one background attempt. The scheduler owns bounded delayed retries.
func (c *collector) deliver(a *attempt) (done, success bool) {
	a.deliveryTries++
	p, ok := c.getPolicy()
	if !ok {
		return false, false
	}
	if !p.allows(a.row.UserID) || !a.modelAllowedLocked(p) {
		return true, true
	}
	if !a.deliveryFrozen {
		a.setTransportDiagnostics(p.TransportDiagnosticsVersion == 1)
		a.deliveryFrozen = true
	} else if a.row.TransportDiagnostics != nil && p.TransportDiagnosticsVersion != 1 {
		// Do not change an event's fingerprint after a possibly accepted send.
		return true, false
	}
	body, length, stop, err := a.streamEnvelope()
	if err != nil {
		return true, false
	}
	req, err := http.NewRequest(http.MethodPost, c.base+"/api/conversation-logs/ingest", body)
	if err != nil {
		stop()
		return true, false
	}
	req.ContentLength = length
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	client := c.http
	if p.DurableReceiptVersion == 1 {
		copyClient := *c.http
		copyClient.Timeout = 5 * time.Second
		client = &copyClient
	}
	resp, err := client.Do(req)
	stop()
	if err == nil {
		var ack struct {
			Accepted   bool   `json:"accepted"`
			ID         string `json:"id"`
			BodySHA256 string `json:"body_sha256"`
			Reason     string `json:"reason"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&ack)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK && decodeErr == nil {
			if !ack.Accepted {
				return ack.Reason == "" || ack.Reason == "excluded", ack.Reason == "" || ack.Reason == "excluded"
			}
			if ack.ID == a.row.ID && ack.BodySHA256 == a.row.BodySHA256 {
				return true, true
			}
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
			return true, false
		}
	}
	return false, false
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
	a.contentType = req.Header.Get("Content-Type")
	if _, part, found := strings.Cut(path, "/models/"); found {
		part, _, _ = strings.Cut(part, ":")
		if model, err := url.PathUnescape(part); err == nil && modelName.MatchString(model) {
			a.models = []string{model}
		}
	}
	copyReq := req.Clone(req.Context())
	a.requestDone = req.Body == nil || req.Body == http.NoBody
	a.requestTransport.expected = req.ContentLength
	if a.requestDone {
		a.requestTransport.expected = 0
	}
	if req.Body != nil {
		expected := req.ContentLength
		if expected == 0 && req.Body != http.NoBody {
			expected = -1
		}
		a.requestTransport.expected = expected
		copyReq.Body = &observedBody{ReadCloser: req.Body, attempt: a, side: "request"}
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
	a.responseTransport.expected = resp.ContentLength
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
	resp.Body = &observedBody{ReadCloser: resp.Body, attempt: a, side: "response"}
	return resp, nil
}

type observedBody struct {
	io.ReadCloser
	attempt  *attempt
	side     string
	once     sync.Once
	closeErr error
	closing  atomic.Bool
	sealed   atomic.Bool
}

func (b *observedBody) Read(p []byte) (int, error) {
	observe := b.attempt.beginObservation()
	n, err := b.ReadCloser.Read(p)
	if observe {
		b.attempt.appendObserved(b.side, p[:n], err, err != nil && b.closing.Load())
		b.attempt.observers.Add(-1)
	}
	return n, err
}

// Preserve WriterTo (notably bytes.Reader request bodies) instead of forcing many small socket writes.
func (b *observedBody) WriteTo(writer io.Writer) (int64, error) {
	observe := b.attempt.beginObservation()
	if !observe {
		return io.Copy(writer, b.ReadCloser)
	}
	defer b.attempt.observers.Add(-1)
	observed := &observedWriter{Writer: writer, body: b}
	var n int64
	var err error
	if fast, ok := b.ReadCloser.(io.WriterTo); ok {
		n, err = fast.WriteTo(observed)
	} else {
		n, err = io.Copy(observed, b.ReadCloser)
	}
	if err == nil {
		b.attempt.append(b.side, nil, io.EOF)
	} else if !observed.writeFailed {
		b.attempt.appendObserved(b.side, nil, err, b.closing.Load())
	}
	return n, err
}

type observedWriter struct {
	io.Writer
	body        *observedBody
	writeFailed bool
}

func (w *observedWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.body.attempt.append(w.body.side, p[:n], nil)
	w.writeFailed = err != nil || n != len(p)
	return n, err
}

func (b *observedBody) Close() error {
	b.once.Do(func() {
		b.closing.Store(true)
		if b.side == "response" && b.sealed.CompareAndSwap(false, true) {
			b.attempt.observers.Add(observerClosed)
		}
		b.closeErr = b.ReadCloser.Close()
		if b.side == "response" {
			b.attempt.closeResponse()
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
	if s.failed.Load() || s.closed.Load() {
		return
	}
	p, valid := s.client.cachedPolicy()
	if !valid || !p.allows(s.user) {
		s.failObservation(!valid)
		return
	}
	// Never wait behind another direction's observer; pressure sacrifices this archive, not relay progress.
	if !s.mu.TryLock() {
		s.failObservation(true)
		return
	}
	defer s.mu.Unlock()
	if s.failed.Load() || s.closed.Load() {
		return
	}
	n := int64(len(body)) + 256
	if !s.client.reserveObservation(n) {
		s.failObservation(true)
		return
	}
	work := &frameObservation{scope: s, account: account, at: time.Now(), policy: p, used: n,
		frame: frame{Direction: direction, Binary: binary, Body: append([]byte{}, body...)}}
	s.pending.Add(1)
	select {
	case s.client.frames <- work:
	default:
		s.pending.Add(-1)
		s.client.used.Add(-n)
		s.failObservation(true)
	}
}
