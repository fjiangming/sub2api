package conversationlog

import (
	"encoding/json"
	"net/http"
	"time"
)

func (s *scope) failObservation(count bool) {
	if s.failed.CompareAndSwap(false, true) && count {
		s.client.dropped.Add(1)
	}
}

// Only this worker owns WS assembly and parsing. Relay goroutines never share its locks.
func (c *collector) runFrames() {
	active := make(map[*scope]struct{})
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer func() {
		for s := range active {
			s.ws.clearLocked()
			s.ws = nil
		}
	}()
	for {
		select {
		case work, open := <-c.frames:
			if !open {
				return
			}
			if work.barrier != nil {
				close(work.barrier)
				continue
			}
			c.consumeFrame(work, active)
		case <-ticker.C:
			for s := range active {
				c.finishClosedFrames(s, active)
			}
		}
	}
}

func (c *collector) finishClosedFrames(s *scope, active map[*scope]struct{}) {
	if s.ws == nil {
		return
	}
	if s.failed.Load() {
		s.ws.clearLocked()
		s.ws = nil
		delete(active, s)
	} else if s.closed.Load() && s.pending.Load() == 0 {
		a := s.ws
		s.ws = nil
		delete(active, s)
		a.finishAt(false, "session_closed", time.Unix(0, s.closedAt.Load()))
	}
}

func (c *collector) consumeFrame(work *frameObservation, active map[*scope]struct{}) {
	s := work.scope
	if work.used == 0 {
		c.finishClosedFrames(s, active)
		return
	}
	defer func() { s.pending.Add(-1); c.finishClosedFrames(s, active) }()
	if s.failed.Load() {
		c.used.Add(-work.used)
		return
	}
	if s.ws == nil {
		path := s.path
		if path == "" {
			path = "/v1/responses"
		}
		s.ws = s.begin(work.account, "", path, "WS", "WebSocket")
		if s.ws == nil {
			c.used.Add(-work.used)
			s.failObservation(false)
			return
		}
		s.ws.policy, s.ws.row.PolicyVersion = work.policy, work.policy.Version
		s.ws.start, s.ws.row.At = work.at, work.at.UnixMilli()
		s.ws.row.Status = http.StatusSwitchingProtocols
		s.ws.models = append(s.ws.models, s.wsModels...)
		active[s] = struct{}{}
	}
	a, f := s.ws, work.frame
	if f.Direction == "out" && !f.Binary {
		if models := bodyModels(f.Body, "application/json", true); len(models) > 0 {
			s.wsModels = models
			if len(a.row.Frames) == 0 {
				a.models = nil
			}
			a.models = append(a.models, models...)
		}
	}
	f.At = max(0, work.at.Sub(a.start).Milliseconds())
	a.used += work.used
	a.row.Frames = append(a.row.Frames, f)
	if f.Direction == "out" && len(a.row.Request) == 0 {
		a.row.Request = f.Body
	}
	if f.Direction == "in" {
		a.row.Response = f.Body
	}
	if f.Direction == "in" && !f.Binary {
		var message struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(f.Body, &message) == nil && (message.Type == "response.completed" || message.Type == "response.failed" || message.Type == "response.incomplete") {
			s.ws = nil
			delete(active, s)
			a.finishAt(true, "", work.at)
		}
	}
}
