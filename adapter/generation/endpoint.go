package generation

import (
	"context"
	"net/netip"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-tun"
)

var _ adapter.EndpointManager = (*EndpointManager)(nil)

type EndpointManager struct {
	manager *Manager
	stable  adapter.EndpointManager
}

func NewEndpointManager(manager *Manager, stable adapter.EndpointManager) *EndpointManager {
	return &EndpointManager{manager: manager, stable: stable}
}

func (*EndpointManager) Start(adapter.StartStage) error { return nil }
func (*EndpointManager) Close() error                   { return nil }

func (m *EndpointManager) Endpoints() []adapter.Endpoint {
	stable := m.stable.Endpoints()
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return append([]adapter.Endpoint(nil), stable...)
	}
	private := runtime.Endpoint.Endpoints()
	result := make([]adapter.Endpoint, 0, len(stable)+len(private))
	result = append(result, stable...)
	for _, endpoint := range private {
		if endpoint != nil {
			result = append(result, newEndpointRef(m.manager, endpoint))
		}
	}
	lease.Release()
	return result
}

func (m *EndpointManager) Get(tag string) (adapter.Endpoint, bool) {
	if endpoint, loaded := m.stable.Get(tag); loaded {
		return endpoint, true
	}
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil, false
	}
	endpoint, loaded := runtime.Endpoint.Get(tag)
	lease.Release()
	if !loaded || endpoint == nil {
		return nil, false
	}
	return newEndpointRef(m.manager, endpoint), true
}

func (m *EndpointManager) Remove(tag string) error {
	return m.stable.Remove(tag)
}

func (m *EndpointManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, endpointType string, options any) error {
	return m.stable.Create(ctx, router, logger, tag, endpointType, options)
}

type endpointRef struct {
	adapter.Outbound
}

func newEndpointRef(manager *Manager, sample adapter.Endpoint) adapter.Endpoint {
	outbound := newEndpointOutboundRef(manager, sample.Tag(), sample)
	base := &endpointRef{Outbound: outbound}
	_, isFlow := outbound.(adapter.FlowOutbound)
	_, hasPreferredRoutes := outbound.(adapter.OutboundWithPreferredRoutes)
	switch {
	case isFlow && hasPreferredRoutes:
		return &preferredFlowEndpointRef{flowEndpointRef: &flowEndpointRef{endpointRef: base}}
	case isFlow:
		return &flowEndpointRef{endpointRef: base}
	case hasPreferredRoutes:
		return &preferredEndpointRef{endpointRef: base}
	default:
		return base
	}
}

func (*endpointRef) Start(adapter.StartStage) error { return nil }
func (*endpointRef) Close() error                   { return nil }

func (r *endpointRef) InterfaceUpdated() {
	if listener, loaded := r.Outbound.(adapter.InterfaceUpdateListener); loaded {
		listener.InterfaceUpdated()
	}
}

type preferredEndpointRef struct {
	*endpointRef
}

func (r *preferredEndpointRef) PreferredDomain(metadata *adapter.InboundContext, domain string) bool {
	preferred, loaded := r.Outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredDomain(metadata, domain)
}

func (r *preferredEndpointRef) PreferredAddress(metadata *adapter.InboundContext, address netip.Addr) bool {
	preferred, loaded := r.Outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredAddress(metadata, address)
}

type flowEndpointRef struct {
	*endpointRef
}

func (r *flowEndpointRef) flow() (adapter.FlowOutbound, bool) {
	flow, loaded := r.Outbound.(adapter.FlowOutbound)
	return flow, loaded
}

func (r *flowEndpointRef) PreMatchFlow(network string, destination netip.Addr) adapter.PreMatchAction {
	if flow, loaded := r.flow(); loaded {
		return flow.PreMatchFlow(network, destination)
	}
	return adapter.PreMatchContinue
}

func (r *flowEndpointRef) PortAddresses() (netip.Addr, netip.Addr) {
	if flow, loaded := r.flow(); loaded {
		return flow.PortAddresses()
	}
	return netip.Addr{}, netip.Addr{}
}

func (r *flowEndpointRef) PortMTU() uint32 {
	if flow, loaded := r.flow(); loaded {
		return flow.PortMTU()
	}
	return 0
}

func (r *flowEndpointRef) AttachReturn(returnPath tun.Return) error {
	if flow, loaded := r.flow(); loaded {
		return flow.AttachReturn(returnPath)
	}
	return ErrNoGeneration
}

func (r *flowEndpointRef) DetachReturn(returnPath tun.Return) error {
	if flow, loaded := r.flow(); loaded {
		return flow.DetachReturn(returnPath)
	}
	return ErrNoGeneration
}

func (r *flowEndpointRef) WritePackets(packets [][]byte) error {
	if flow, loaded := r.flow(); loaded {
		return flow.WritePackets(packets)
	}
	return ErrNoGeneration
}

type preferredFlowEndpointRef struct {
	*flowEndpointRef
}

func (r *preferredFlowEndpointRef) PreferredDomain(metadata *adapter.InboundContext, domain string) bool {
	preferred, loaded := r.Outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredDomain(metadata, domain)
}

func (r *preferredFlowEndpointRef) PreferredAddress(metadata *adapter.InboundContext, address netip.Addr) bool {
	preferred, loaded := r.Outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredAddress(metadata, address)
}
