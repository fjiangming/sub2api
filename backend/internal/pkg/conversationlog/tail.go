package conversationlog

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

const tailTotalBudget = time.Second
const tailIdleBudget = 200 * time.Millisecond
const tailBufferBytes = 16 * 1024

type tailJob struct {
	done   chan struct{}
	cancel func()
}

// ResponseTail is available only for an actually observed upstream response.
// The repository retains ownership of cancellation, read serialization and closing.
type ResponseTail struct {
	body     *observedBody
	ctx      context.Context
	client   context.Context
	terminal atomic.Bool
	closing  atomic.Bool
	yielded  atomic.Bool
	job      atomic.Pointer[tailJob]
}

func NewResponseTail(body io.ReadCloser, ctx, client context.Context) *ResponseTail {
	observed, ok := body.(*observedBody)
	if !ok || observed.side != "response" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = ctx
	}
	return &ResponseTail{body: observed, ctx: ctx, client: client}
}

func ResponseTailOf(body io.ReadCloser) *ResponseTail {
	if value, ok := body.(interface{ ConversationTail() *ResponseTail }); ok {
		return value.ConversationTail()
	}
	return nil
}

// MarkResponseTerminal receives an observation from the existing protocol parser.
// It permits bounded tail reading; it never declares the archived body complete.
func MarkResponseTerminal(resp *http.Response) {
	if resp != nil {
		if tail := ResponseTailOf(resp.Body); tail != nil {
			tail.terminal.Store(true)
		}
	}
}

func (t *ResponseTail) armed() bool {
	return t != nil && (t.terminal.Load() || t.body.attempt.row.Status >= 400)
}

func (t *ResponseTail) Closing() bool { return t != nil && t.closing.Load() }

// Yield gives a newer inference attempt priority over optional tail reads.
// It never cancels a response which is still being used by the native relay.
func (t *ResponseTail) Yield() {
	t.yielded.Store(true)
	if job := t.job.Load(); job != nil {
		job.cancel()
	}
}

// CloseResponse preserves cleanup-before-close unless bounded archival is armed.
func CloseResponse(body io.ReadCloser, cleanup func()) error {
	tail := ResponseTailOf(body)
	if !tail.armed() {
		cleanup()
		if body != nil {
			return body.Close()
		}
		return nil
	}
	err := body.Close()
	AfterResponseClose(body, cleanup)
	return err
}

// AfterResponseClose keeps transport accounting and cleanup alive until actual close.
func AfterResponseClose(body io.ReadCloser, cleanup func()) {
	if tail := ResponseTailOf(body); tail != nil {
		if job := tail.job.Load(); job != nil {
			go func() { <-job.done; cleanup() }()
			return
		}
	}
	cleanup()
}

// Start never waits for reads or archive delivery. lock is called only in the
// background, before accessing the raw reader or closing its decompressor.
func (t *ResponseTail) Start(cancel func(), lock func() func(), closeBody func()) bool {
	if !t.armed() || t.yielded.Load() || t.ctx.Err() != nil || t.client.Err() != nil {
		return false
	}
	a, c := t.body.attempt, t.body.attempt.client
	p, valid := c.cachedPolicy()
	if !valid || !p.allows(a.row.UserID) {
		return false
	}
	if c.tails.Add(1) > 2 {
		c.tails.Add(-1)
		return false
	}
	if !a.mu.TryLock() {
		c.tails.Add(-1)
		return false
	}
	defer a.mu.Unlock()
	requestDone := a.requestDone || a.requestTransport.expected >= 0 && a.requestSize == a.requestTransport.expected
	responseDone := a.responseDone || a.responseTransport.expected >= 0 && a.responseSize == a.responseTransport.expected
	if a.finished || a.abandoned || a.queued || !requestDone || responseDone || a.row.Reason != "" || !c.reserveObservation(tailBufferBytes) {
		c.tails.Add(-1)
		return false
	}
	if c.jobs.Add(1) > 32 {
		c.jobs.Add(-1)
		c.used.Add(-tailBufferBytes)
		c.tails.Add(-1)
		return false
	}
	a.queued = true
	a.closeAt = time.Now()
	t.body.closing.Store(true)
	t.closing.Store(true)
	job := &tailJob{done: make(chan struct{}), cancel: cancel}
	t.job.Store(job)
	if t.yielded.Load() {
		cancel()
	}
	go t.run(job, cancel, lock, closeBody)
	return true
}

func (t *ResponseTail) run(job *tailJob, cancel func(), lock func() func(), closeBody func()) {
	a, c := t.body.attempt, t.body.attempt.client
	defer close(job.done)
	defer c.tails.Add(-1)
	defer c.used.Add(-tailBufferBytes)
	stopContext := context.AfterFunc(t.ctx, cancel)
	defer stopContext()
	total := time.AfterFunc(max(0, time.Until(a.closeAt.Add(tailTotalBudget))), cancel)
	defer total.Stop()
	idle := time.AfterFunc(max(0, time.Until(a.closeAt.Add(tailIdleBudget))), cancel)
	defer idle.Stop()
	unlock := lock()
	defer unlock()
	defer closeBody()
	// The repository has stopped new reads and joined any read already started.
	// Close admission only now, so that read's final bytes are still observed.
	if t.body.sealed.CompareAndSwap(false, true) {
		a.observers.Add(observerClosed)
	}
	if a.observers.Load() != observerClosed || t.ctx.Err() != nil {
		return
	}
	a.mu.Lock()
	done := a.abandoned || a.row.Reason != "" || a.responseDone || a.responseTransport.expected >= 0 && a.responseSize == a.responseTransport.expected
	a.mu.Unlock()
	if done {
		return
	}
	// Request observations are now quiescent. Model parsing remains off the relay.
	a.prepareDelivery()
	buffer := make([]byte, tailBufferBytes)
	for {
		p, valid := c.cachedPolicy()
		if !valid || !p.allows(a.row.UserID) || !a.modelAllowedLocked(a.policy) || !a.modelAllowedLocked(p) || t.ctx.Err() != nil {
			return
		}
		a.mu.Lock()
		done := a.abandoned || a.row.Reason != "" || a.responseDone || a.responseTransport.expected >= 0 && a.responseSize == a.responseTransport.expected
		a.mu.Unlock()
		if done {
			return
		}
		n, err := t.body.ReadCloser.Read(buffer)
		a.appendObserved("response", buffer[:n], err, t.body.closing.Load())
		if err != nil || n == 0 {
			return
		}
		idle.Reset(tailIdleBudget)
	}
}
