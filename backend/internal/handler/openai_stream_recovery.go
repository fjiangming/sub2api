package handler

import (
	"container/list"
	"crypto/sha256"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	openAIRecoveryHeader        = "X-Sub2API-Recovery"
	openAIRecoveryFromHeader    = "X-Sub2API-Recovery-From"
	openAIRecoveryTokenHeader   = "X-Sub2API-Recovery-Token"
	openAIRecoveryAppliedHeader = "X-Sub2API-Recovery-Applied"
	openAIRecoveryTTL           = 2 * time.Hour
	openAIRecoveryMaxTokens     = 3
)

type openAIRecoveryKey struct {
	token    string
	apiKeyID int64
	groupID  int64
	bodyHash [32]byte
}

type openAIRecoveryEntry struct {
	key       openAIRecoveryKey
	accountID int64
	expiresAt time.Time
}

// Recovery hints only exclude an account from a new authenticated request.
// They do not replay a dispatch or change the scheduler's eligibility checks.
type openAIStreamRecoveryCache struct {
	mu       sync.Mutex
	entries  map[openAIRecoveryKey]*list.Element
	order    *list.List
	capacity int
	now      func() time.Time
}

func newOpenAIStreamRecoveryCache(capacity int, now func() time.Time) *openAIStreamRecoveryCache {
	return &openAIStreamRecoveryCache{
		entries: make(map[openAIRecoveryKey]*list.Element), order: list.New(), capacity: capacity, now: now,
	}
}

func (s *openAIStreamRecoveryCache) account(key openAIRecoveryKey) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[key]
	if e == nil {
		return 0
	}
	entry := e.Value.(openAIRecoveryEntry)
	if !s.now().Before(entry.expiresAt) {
		delete(s.entries, key)
		s.order.Remove(e)
		return 0
	}
	s.order.MoveToFront(e)
	return entry.accountID
}

func (s *openAIStreamRecoveryCache) record(key openAIRecoveryKey, accountID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[key]; e != nil {
		s.order.Remove(e)
		delete(s.entries, key)
	}
	if accountID <= 0 || s.capacity <= 0 {
		return
	}
	entry := openAIRecoveryEntry{key: key, accountID: accountID, expiresAt: s.now().Add(openAIRecoveryTTL)}
	s.entries[key] = s.order.PushFront(entry)
	for len(s.entries) > s.capacity {
		e := s.order.Back()
		delete(s.entries, e.Value.(openAIRecoveryEntry).key)
		s.order.Remove(e)
	}
}

type openAIStreamRecoveryRequest struct {
	cache    *openAIStreamRecoveryCache
	key      openAIRecoveryKey
	excluded map[int64]struct{}
}

func (h *OpenAIGatewayHandler) beginOpenAIStreamRecovery(c *gin.Context, apiKey *service.APIKey, body []byte) *openAIStreamRecoveryRequest {
	optIn := c.GetHeader(openAIRecoveryHeader) == "1"
	previous := c.GetHeader(openAIRecoveryFromHeader)
	// Consume these locally; routing tokens must never reach the supplier.
	for _, header := range []string{openAIRecoveryHeader, openAIRecoveryFromHeader, openAIRecoveryTokenHeader, openAIRecoveryAppliedHeader} {
		c.Request.Header.Del(header)
	}
	if !optIn || h.streamRecovery == nil || apiKey == nil || apiKey.ID <= 0 || apiKey.GroupID == nil || *apiKey.GroupID <= 0 {
		return nil
	}
	r := &openAIStreamRecoveryRequest{
		cache:    h.streamRecovery,
		key:      openAIRecoveryKey{token: uuid.NewString(), apiKeyID: apiKey.ID, groupID: *apiKey.GroupID, bodyHash: sha256.Sum256(body)},
		excluded: make(map[int64]struct{}),
	}
	// Bound the list and ignore malformed tokens without exposing account IDs.
	if len(previous) <= openAIRecoveryMaxTokens*37 {
		tokens := strings.Split(previous, ",")
		if len(tokens) <= openAIRecoveryMaxTokens {
			for _, token := range tokens {
				token = strings.TrimSpace(token)
				id, err := uuid.Parse(token)
				if err != nil || token != id.String() {
					continue
				}
				key := r.key
				key.token = token
				if accountID := r.cache.account(key); accountID > 0 {
					r.excluded[accountID] = struct{}{}
				}
			}
		}
	}
	c.Header(openAIRecoveryTokenHeader, r.key.token)
	c.Header(openAIRecoveryAppliedHeader, "0")
	if len(r.excluded) > 0 {
		c.Header(openAIRecoveryAppliedHeader, "1")
	}
	return r
}

func (r *openAIStreamRecoveryRequest) excludedAccounts() map[int64]struct{} {
	if r != nil {
		return r.excluded
	}
	return make(map[int64]struct{})
}

func (r *openAIStreamRecoveryRequest) recordFailure(accountID int64) {
	if r != nil {
		r.cache.record(r.key, accountID)
	}
}

func (r *openAIStreamRecoveryRequest) forgetFailure() {
	if r != nil {
		r.cache.record(r.key, 0)
	}
}
