package box

import (
	"context"
	"os"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

// runtimeEndpointManager exposes stable top-level endpoints read-only while
// keeping provider-created endpoints private to one runtime generation.
type runtimeEndpointManager struct {
	stable adapter.EndpointManager
	local  adapter.EndpointManager
}

func newRuntimeEndpointManager(stable, local adapter.EndpointManager) *runtimeEndpointManager {
	return &runtimeEndpointManager{stable: stable, local: local}
}

func (m *runtimeEndpointManager) Start(stage adapter.StartStage) error {
	return m.local.Start(stage)
}

func (m *runtimeEndpointManager) Close() error {
	return m.local.Close()
}

func (m *runtimeEndpointManager) Endpoints() []adapter.Endpoint {
	stable := m.stable.Endpoints()
	local := m.local.Endpoints()
	result := make([]adapter.Endpoint, 0, len(stable)+len(local))
	result = append(result, stable...)
	result = append(result, local...)
	return result
}

func (m *runtimeEndpointManager) stableEndpoints() []adapter.Endpoint {
	return append([]adapter.Endpoint(nil), m.stable.Endpoints()...)
}

func (m *runtimeEndpointManager) Get(tag string) (adapter.Endpoint, bool) {
	if endpoint, loaded := m.local.Get(tag); loaded {
		return endpoint, true
	}
	return m.stable.Get(tag)
}

func (m *runtimeEndpointManager) Remove(tag string) error {
	if _, loaded := m.local.Get(tag); !loaded {
		return os.ErrInvalid
	}
	return m.local.Remove(tag)
}

func (m *runtimeEndpointManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, endpointType string, options any) error {
	if _, loaded := m.stable.Get(tag); loaded {
		return E.New("provider endpoint conflicts with stable endpoint: ", tag)
	}
	return m.local.Create(ctx, router, logger, tag, endpointType, options)
}
