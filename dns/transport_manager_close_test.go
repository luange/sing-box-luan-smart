package dns

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestDNSTransport struct {
	adapter.DNSTransport
	closeCount atomic.Int32
	closeErr   error
	tag        string
	typeName   string
}

func (t *managerTestDNSTransport) Type() string {
	if t.typeName == "" {
		return "test"
	}
	return t.typeName
}
func (t *managerTestDNSTransport) Tag() string {
	if t.tag == "" {
		return "test"
	}
	return t.tag
}
func (*managerTestDNSTransport) Dependencies() []string { return nil }
func (t *managerTestDNSTransport) Close() error {
	t.closeCount.Add(1)
	return t.closeErr
}

func TestTransportManagerRemoveDefaultChoosesNormalTransport(t *testing.T) {
	first := &managerTestDNSTransport{tag: "first", typeName: "udp"}
	second := &managerTestDNSTransport{tag: "second", typeName: "https"}
	manager := &TransportManager{
		logger:           log.NewNOPFactory().NewLogger("dns"),
		transports:       []adapter.DNSTransport{first, second},
		transportByTag:   map[string]adapter.DNSTransport{"first": first, "second": second},
		dependByTag:      make(map[string][]string),
		defaultTransport: first,
	}
	if err := manager.Remove("first"); err != nil {
		t.Fatal(err)
	}
	if manager.Default() != second {
		t.Fatal("normal DNS transport was not selected as the next default")
	}
}

func TestTransportManagerRemoveDependentIsTransactional(t *testing.T) {
	base := &managerTestDNSTransport{tag: "base", typeName: "udp"}
	child := &managerTestDNSTransport{tag: "child", typeName: "https"}
	manager := &TransportManager{
		logger:           log.NewNOPFactory().NewLogger("dns"),
		transports:       []adapter.DNSTransport{base, child},
		transportByTag:   map[string]adapter.DNSTransport{"base": base, "child": child},
		dependByTag:      map[string][]string{"base": {"child"}},
		defaultTransport: base,
	}
	if err := manager.Remove("base"); err == nil {
		t.Fatal("dependent DNS transport removal unexpectedly succeeded")
	}
	if transport, loaded := manager.Transport("base"); !loaded || transport != base {
		t.Fatal("failed removal mutated DNS transport lookup")
	}
	if len(manager.Transports()) != 2 || manager.Default() != base {
		t.Fatal("failed removal mutated DNS manager state")
	}
}

func TestTransportManagerCloseBeforeStartReturnsErrors(t *testing.T) {
	expected := errors.New("close failed")
	transport := &managerTestDNSTransport{closeErr: expected}
	manager := &TransportManager{
		logger:     log.NewNOPFactory().NewLogger("dns"),
		transports: []adapter.DNSTransport{transport},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("DNS transport close error was lost: %v", err)
	}
	if transport.closeCount.Load() != 1 {
		t.Fatalf("DNS transport closed %d times", transport.closeCount.Load())
	}
}
