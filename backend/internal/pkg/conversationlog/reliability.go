package conversationlog

import "time"

const observerClosed int64 = -1 << 62
const observationGrace = 250 * time.Millisecond

func (a *attempt) beginObservation() bool {
	if a.observers.Add(1) > 0 {
		return true
	}
	a.observers.Add(-1)
	return false
}

// Close never waits for another read. Only reads already in progress receive a bounded grace period.
func (a *attempt) closeResponse() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished || a.awaiting {
		return
	}
	if a.closeAt.IsZero() {
		a.closeAt = time.Now()
	}
	a.awaiting = true
	if a.observers.Load() == observerClosed {
		a.sealHTTPLocked()
	}
	a.enqueueLocked()
}

func (a *attempt) sealHTTPLocked() {
	a.finished, a.awaiting = true, false
	a.row.Duration = max(0, a.closeAt.Sub(a.start).Milliseconds())
	a.requestDone = a.requestDone || a.requestTransport.expected >= 0 && a.requestSize == a.requestTransport.expected
	a.responseDone = a.responseDone || a.responseTransport.expected >= 0 && a.responseSize == a.responseTransport.expected
	a.row.Complete = a.requestDone && a.responseDone && a.row.Reason == ""
	if !a.row.Complete && a.row.Reason == "" {
		a.row.Reason = "closed_early"
	}
}

func (c *collector) finalize(ready chan<- *attempt) {
	defer close(ready)
	ticker := time.NewTicker(5 * time.Millisecond)
	ticker.Stop()
	defer ticker.Stop()
	queue := c.queue
	pending := make([]*attempt, 0, 32)
	for queue != nil || len(pending) > 0 {
		var tick <-chan time.Time
		if len(pending) > 0 {
			ticker.Reset(5 * time.Millisecond)
			tick = ticker.C
		} else {
			ticker.Stop()
		}
		select {
		case a, ok := <-queue:
			if !ok {
				queue = nil
			} else {
				pending = append(pending, a)
			}
		case <-tick:
		}
		kept := pending[:0]
		for _, a := range pending {
			a.mu.Lock()
			if a.abandoned {
				a.finished, a.awaiting = true, false
				a.client.dropped.Add(1)
				a.clearLocked()
				a.mu.Unlock()
				continue
			}
			if a.awaiting && a.observers.Load() != observerClosed && time.Since(a.closeAt) < observationGrace {
				a.mu.Unlock()
				kept = append(kept, a)
				continue
			}
			if a.awaiting {
				a.sealHTTPLocked()
			}
			a.mu.Unlock()
			ready <- a
		}
		clear(pending[len(kept):])
		pending = kept
	}
}

type scheduledDelivery struct {
	attempt *attempt
	due     time.Time
}

func (c *collector) run() {
	ready := make(chan *attempt, 32)
	go c.finalize(ready)
	jobs := make([]scheduledDelivery, 0, 32)
	for ready != nil || len(jobs) > 0 {
		// Admit fresh records before selecting the oldest due delivery or retry.
		select {
		case a, ok := <-ready:
			if !ok {
				ready = nil
			} else {
				jobs = append(jobs, scheduledDelivery{attempt: a, due: time.Now()})
			}
		default:
		}
		next := -1
		for i := range jobs {
			if next < 0 || jobs[i].due.Before(jobs[next].due) {
				next = i
			}
		}
		if next < 0 || time.Now().Before(jobs[next].due) {
			delay := time.Hour
			if next >= 0 {
				delay = max(0, time.Until(jobs[next].due))
			} else if ready == nil {
				return
			}
			timer := time.NewTimer(delay)
			select {
			case a, ok := <-ready:
				if !ok {
					ready = nil
				} else {
					jobs = append(jobs, scheduledDelivery{attempt: a, due: time.Now()})
				}
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		a := jobs[next].attempt
		if a.deliveryTries == 0 {
			a.prepareDelivery()
			if !a.modelAllowedLocked(a.policy) {
				a.clearLocked()
				jobs = append(jobs[:next], jobs[next+1:]...)
				continue
			}
			a.row.BodySHA256 = a.capturedDigest()
		}
		done, success := c.deliver(a)
		if done || a.deliveryTries >= 3 {
			if !success {
				c.dropped.Add(1)
			}
			a.clearLocked()
			jobs = append(jobs[:next], jobs[next+1:]...)
		} else {
			delay := 500 * time.Millisecond
			if a.deliveryTries > 1 {
				delay = 2 * time.Second
			}
			jobs[next].due = time.Now().Add(delay)
		}
	}
}
