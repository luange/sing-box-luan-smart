package inbound

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestInbound struct {
	adapter.Inbound
	closeCount atomic.Int32
	closeErr   error
}

func (*managerTestInbound) Type() string { return "test" }
func (*managerTestInbound) Tag() string  { return "test" }
func (i *managerTestInbound) Close() error {
	i.closeCount.Add(1)
	return i.closeErr
}

func TestManagerCloseBeforeStartReturnsInboundErrors(t *testing.T) {
	expected := errors.New("close failed")
	inbound := &managerTestInbound{closeErr: expected}
	manager := &Manager{
		logger:   log.NewNOPFactory().NewLogger("inbound"),
		inbounds: []adapter.Inbound{inbound},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("inbound close error was lost: %v", err)
	}
	if inbound.closeCount.Load() != 1 {
		t.Fatalf("inbound closed %d times", inbound.closeCount.Load())
	}
}
