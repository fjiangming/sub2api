package repository

import (
	"io"

	"github.com/Wei-Shaw/sub2api/internal/pkg/conversationlog"
)

func (e *upstreamClientEntry) yieldConversationTail() uint64 {
	epoch := e.tailEpoch.Add(1)
	if tail := e.activeTail.Swap(nil); tail != nil {
		tail.Yield()
	}
	return epoch
}

func (e *upstreamClientEntry) trackConversationTail(body io.ReadCloser, epoch uint64) *conversationlog.ResponseTail {
	tail := conversationlog.ResponseTailOf(body)
	if tail == nil {
		return nil
	}
	if e.tailEpoch.Load() != epoch || !e.activeTail.CompareAndSwap(nil, tail) {
		tail.Yield()
		return tail
	}
	// A newer request may have started between the epoch check and publication.
	if e.tailEpoch.Load() != epoch {
		tail.Yield()
		e.activeTail.CompareAndSwap(tail, nil)
	}
	return tail
}

func (b *cancelOnCloseBody) ConversationTail() *conversationlog.ResponseTail { return b.tail }

func (b *trackedBody) ConversationTail() *conversationlog.ResponseTail {
	return conversationlog.ResponseTailOf(b.ReadCloser)
}
