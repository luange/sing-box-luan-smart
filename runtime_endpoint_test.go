package box

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

type runtimeEndpointTestEndpoint struct {
	adapter.Endpoint
	tag string
}

func (e *runtimeEndpointTestEndpoint) Type() string { return "test" }
func (e *runtimeEndpointTestEndpoint) Tag() string  { return e.tag }
func (e *runtimeEndpointTestEndpoint) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}
func (e *runtimeEndpointTestEndpoint) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type runtimeEndpointTestManager struct {
	adapter.EndpointManager
	endpoints   map[string]adapter.Endpoint
	starts      atomic.Int32
	closes      atomic.Int32
	creates     atomic.Int32
	removes     atomic.Int32
	createError error
}

func newRuntimeEndpointTestManager(tags ...string) *runtimeEndpointTestManager {
	manager := &runtimeEndpointTestManager{endpoints: make(map[string]adapter.Endpoint)}
	for _, tag := range tags {
		manager.endpoints[tag] = &runtimeEndpointTestEndpoint{tag: tag}
	}
	return manager
}

func (m *runtimeEndpointTestManager) Start(adapter.StartStage) error {
	m.starts.Add(1)
	return nil
}

func (m *runtimeEndpointTestManager) Close() error {
	m.closes.Add(1)
	return nil
}

func (m *runtimeEndpointTestManager) Endpoints() []adapter.Endpoint {
	result := make([]adapter.Endpoint, 0, len(m.endpoints))
	for _, endpoint := range m.endpoints {
		result = append(result, endpoint)
	}
	return result
}

func (m *runtimeEndpointTestManager) Get(tag string) (adapter.Endpoint, bool) {
	endpoint, loaded := m.endpoints[tag]
	return endpoint, loaded
}

func (m *runtimeEndpointTestManager) Remove(tag string) error {
	if _, loaded := m.endpoints[tag]; !loaded {
		return os.ErrInvalid
	}
	m.removes.Add(1)
	delete(m.endpoints, tag)
	return nil
}

func (m *runtimeEndpointTestManager) Create(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ string, _ any) error {
	if m.createError != nil {
		return m.createError
	}
	m.creates.Add(1)
	m.endpoints[tag] = &runtimeEndpointTestEndpoint{tag: tag}
	return nil
}

func TestRuntimeEndpointManagerIsolatesStableEndpoints(t *testing.T) {
	stable := newRuntimeEndpointTestManager("stable")
	local := newRuntimeEndpointTestManager()
	manager := newRuntimeEndpointManager(stable, local)

	if endpoint, loaded := manager.Get("stable"); !loaded || endpoint.Tag() != "stable" {
		t.Fatal("stable endpoint is not visible through overlay")
	}
	if err := manager.Create(context.Background(), nil, nil, "candidate", "test", nil); err != nil {
		t.Fatal(err)
	}
	if local.creates.Load() != 1 || stable.creates.Load() != 0 {
		t.Fatalf("candidate endpoint escaped local manager: local=%d stable=%d", local.creates.Load(), stable.creates.Load())
	}
	if endpoint, loaded := manager.Get("candidate"); !loaded || endpoint.Tag() != "candidate" {
		t.Fatal("candidate endpoint is not visible through overlay")
	}
	if err := manager.Create(context.Background(), nil, nil, "stable", "test", nil); err == nil {
		t.Fatal("candidate endpoint replaced a stable endpoint")
	}
	if err := manager.Remove("stable"); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("stable endpoint removal returned %v", err)
	}
	if stable.removes.Load() != 0 {
		t.Fatal("stable endpoint was removed through candidate overlay")
	}
	if err := manager.Remove("candidate"); err != nil {
		t.Fatal(err)
	}
	if local.removes.Load() != 1 {
		t.Fatal("candidate endpoint was not removed from local manager")
	}
}

func TestRuntimeEndpointManagerOwnsOnlyLocalLifecycle(t *testing.T) {
	stable := newRuntimeEndpointTestManager("stable")
	local := newRuntimeEndpointTestManager("candidate")
	manager := newRuntimeEndpointManager(stable, local)

	if err := manager.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if local.starts.Load() != 1 || local.closes.Load() != 1 {
		t.Fatalf("local lifecycle mismatch: starts=%d closes=%d", local.starts.Load(), local.closes.Load())
	}
	if stable.starts.Load() != 0 || stable.closes.Load() != 0 {
		t.Fatalf("stable lifecycle was touched: starts=%d closes=%d", stable.starts.Load(), stable.closes.Load())
	}
}
