package conversationlog

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
)

type transportState struct {
	expected         int64
	eof              bool
	errorKind        string
	errorDuringClose bool
}

type transportSide struct {
	Bytes            int64  `json:"bytes"`
	ExpectedBytes    int64  `json:"expected_bytes"`
	EOF              bool   `json:"eof"`
	Error            string `json:"error"`
	ErrorDuringClose bool   `json:"error_during_close"`
}

type transportDiagnostics struct {
	Version  int           `json:"version"`
	Request  transportSide `json:"request"`
	Response transportSide `json:"response"`
}

func transportErrorKind(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return "deadline"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, net.ErrClosed), errors.Is(err, os.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		return "closed"
	}
	var network net.Error
	if errors.As(err, &network) {
		if network.Timeout() {
			return "timeout"
		}
		return "network"
	}
	return "other"
}

// Called after ownership transfers to the sender; no body parsing or error strings on the relay path.
func (a *attempt) setTransportDiagnostics(enabled bool) {
	a.row.TransportDiagnostics = nil
	if !enabled || a.row.Protocol == "WebSocket" {
		return
	}
	side := func(state transportState, size int64) transportSide {
		return transportSide{size, state.expected, state.eof, state.errorKind, state.errorDuringClose}
	}
	a.row.TransportDiagnostics = &transportDiagnostics{Version: 1,
		Request: side(a.requestTransport, a.requestSize), Response: side(a.responseTransport, a.responseSize)}
}
