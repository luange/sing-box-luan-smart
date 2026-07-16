package provider

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestProvider struct {
	adapter.Provider
	closeCount atomic.Int32
	closeErr   error
}

func (*managerTestProvider) Type() string { return "test" }
func (*managerTestProvider) Tag() string  { return "test" }
func (p *managerTestProvider) Close() error {
	p.closeCount.Add(1)
	return p.closeErr
}

func TestManagerCloseBeforeStartReturnsProviderErrors(t *testing.T) {
	expected := errors.New("close failed")
	provider := &managerTestProvider{closeErr: expected}
	manager := &Manager{
		logger:    log.NewNOPFactory().NewLogger("provider"),
		providers: []adapter.Provider{provider},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("provider close error was lost: %v", err)
	}
	if provider.closeCount.Load() != 1 {
		t.Fatalf("provider closed %d times", provider.closeCount.Load())
	}
}
