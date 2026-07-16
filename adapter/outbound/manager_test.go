package outbound

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type managerTestOutbound struct {
	Adapter
	started     atomic.Int32
	stageStarts [4]atomic.Int32
	closeErr    error
}

func newManagerTestOutbound(tag string, dependencies []string) *managerTestOutbound {
	return &managerTestOutbound{Adapter: NewAdapter(C.TypeDirect, tag, []string{N.NetworkTCP}, dependencies)}
}

func (o *managerTestOutbound) Start(stage adapter.StartStage) error {
	o.started.Add(1)
	o.stageStarts[stage].Add(1)
	return nil
}

type managerTestEndpointManager struct {
	adapter.EndpointManager
}

func (*managerTestEndpointManager) Endpoints() []adapter.Endpoint       { return nil }
func (*managerTestEndpointManager) Get(string) (adapter.Endpoint, bool) { return nil, false }
func (o *managerTestOutbound) Close() error                             { return o.closeErr }
func (*managerTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}
func (*managerTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestManagerCloseBeforeStartReturnsOutboundErrors(t *testing.T) {
	expected := errors.New("close failed")
	outbound := newManagerTestOutbound("failed", nil)
	outbound.closeErr = expected
	manager := &Manager{
		logger:    log.NewNOPFactory().NewLogger("outbound"),
		outbounds: []adapter.Outbound{outbound},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("outbound close error was lost: %v", err)
	}
}

func TestStartedEndpointSatisfiesDependencyWithoutRestart(t *testing.T) {
	endpoint := newManagerTestOutbound("stable-endpoint", nil)
	dependent := newManagerTestOutbound("candidate", []string{"stable-endpoint"})
	manager := &Manager{logger: log.NewNOPFactory().NewLogger("outbound")}
	if err := manager.startOutbounds(
		[]adapter.Outbound{dependent, endpoint},
		map[string]bool{"stable-endpoint": true},
	); err != nil {
		t.Fatal(err)
	}
	if endpoint.started.Load() != 0 {
		t.Fatalf("stable endpoint restarted %d times", endpoint.started.Load())
	}
	if dependent.started.Load() != 1 {
		t.Fatalf("candidate outbound started %d times", dependent.started.Load())
	}
}

func TestManagerCreateStartsOnlyThroughCurrentStage(t *testing.T) {
	registry := NewRegistry()
	var created *managerTestOutbound
	Register[struct{}](registry, "test", func(context.Context, adapter.Router, log.ContextLogger, string, struct{}) (adapter.Outbound, error) {
		created = newManagerTestOutbound("dynamic", nil)
		return created, nil
	})
	manager := NewManager(log.NewNOPFactory().NewLogger("outbound"), registry, &managerTestEndpointManager{}, "")
	manager.started = true
	manager.stage = adapter.StartStateStart
	if err := manager.Create(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "dynamic", "test", &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if created.stageStarts[adapter.StartStateInitialize].Load() != 1 || created.stageStarts[adapter.StartStateStart].Load() != 1 {
		t.Fatal("dynamic outbound did not start through current stage")
	}
	if created.stageStarts[adapter.StartStatePostStart].Load() != 0 || created.stageStarts[adapter.StartStateStarted].Load() != 0 {
		t.Fatal("dynamic outbound started future lifecycle stages")
	}
}

func TestManagerRemoveDependentOutboundIsTransactional(t *testing.T) {
	base := newManagerTestOutbound("base", nil)
	child := newManagerTestOutbound("child", []string{"base"})
	manager := &Manager{
		logger:        log.NewNOPFactory().NewLogger("outbound"),
		outbounds:     []adapter.Outbound{base, child},
		outboundByTag: map[string]adapter.Outbound{"base": base, "child": child},
		dependByTag:   map[string][]string{"base": {"child"}},
	}
	if err := manager.Remove("base"); err == nil {
		t.Fatal("dependent outbound removal unexpectedly succeeded")
	}
	if outbound, loaded := manager.Outbound("base"); !loaded || outbound != base {
		t.Fatal("failed removal mutated outbound lookup")
	}
	if len(manager.Outbounds()) != 2 {
		t.Fatal("failed removal mutated outbound list")
	}
}
