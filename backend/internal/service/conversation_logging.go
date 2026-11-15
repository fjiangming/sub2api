package service

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/conversationlog"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
)

// The bound identity belongs to this relay, even if its upstream transport is reused later.
type conversationFrameConn struct {
	openaiwsv2.FrameConn
	owner   context.Context
	account int64
}

func (c *conversationFrameConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	kind, data, err := c.FrameConn.ReadFrame(ctx)
	if err == nil {
		conversationlog.RecordFrame(c.owner, c.account, "in", data, kind == coderws.MessageBinary)
	}
	return kind, data, err
}
func (c *conversationFrameConn) WriteFrame(ctx context.Context, kind coderws.MessageType, data []byte) error {
	err := c.FrameConn.WriteFrame(ctx, kind, data)
	if err == nil {
		conversationlog.RecordFrame(c.owner, c.account, "out", data, kind == coderws.MessageBinary)
	}
	return err
}
