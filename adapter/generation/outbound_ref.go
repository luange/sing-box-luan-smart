package generation

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-tun"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

type outboundRef struct {
	manager     *Manager
	tag         string
	useDefault  bool
	useEndpoint bool
}

func newOutboundRef(manager *Manager, tag string, sample adapter.Outbound) adapter.Outbound {
	base := &outboundRef{manager: manager, tag: tag}
	return newOutboundRefWithBase(base, sample)
}

func newEndpointOutboundRef(manager *Manager, tag string, sample adapter.Outbound) adapter.Outbound {
	base := &outboundRef{manager: manager, tag: tag, useEndpoint: true}
	return newOutboundRefWithBase(base, sample)
}

func newOutboundRefWithBase(base *outboundRef, sample adapter.Outbound) adapter.Outbound {
	switch sample.Type() {
	case C.TypeSmart:
		return &smartOutboundRef{urlTestOutboundRef: urlTestOutboundRef{outboundGroupRef{outboundRef: base}}}
	case C.TypeSelector:
		return &selectorOutboundRef{outboundGroupRef: outboundGroupRef{outboundRef: base}}
	case C.TypeLoadBalance:
		return &loadBalanceOutboundRef{outboundGroupRef: outboundGroupRef{outboundRef: base}}
	case C.TypeURLTest:
		return &urlTestOutboundRef{outboundGroupRef: outboundGroupRef{outboundRef: base}}
	}
	if _, isGroup := sample.(adapter.OutboundGroup); isGroup {
		return &outboundGroupRef{outboundRef: base}
	}
	_, isFlow := sample.(adapter.FlowOutbound)
	_, hasPreferredRoutes := sample.(adapter.OutboundWithPreferredRoutes)
	switch {
	case isFlow && hasPreferredRoutes:
		return &preferredFlowOutboundRef{flowOutboundRef: flowOutboundRef{outboundRef: base}}
	case isFlow:
		return &flowOutboundRef{outboundRef: base}
	case hasPreferredRoutes:
		return &preferredOutboundRef{outboundRef: base}
	default:
		return base
	}
}

func newDefaultOutboundRef(manager *Manager) adapter.Outbound {
	return &outboundRef{manager: manager, useDefault: true}
}

func (r *outboundRef) acquire() (adapter.Outbound, adapter.GenerationLease, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, nil, err
	}
	if r.useDefault {
		outbound := runtime.Outbound.Default()
		if outbound == nil {
			lease.Release()
			return nil, nil, E.New("default outbound is unavailable")
		}
		return outbound, lease, nil
	}
	if r.useEndpoint {
		endpoint, loaded := runtime.Endpoint.Get(r.tag)
		if !loaded {
			lease.Release()
			return nil, nil, E.New("endpoint not found: ", r.tag)
		}
		return endpoint, lease, nil
	}
	outbound, loaded := runtime.Outbound.Outbound(r.tag)
	if !loaded {
		lease.Release()
		return nil, nil, E.New("outbound not found: ", r.tag)
	}
	return outbound, lease, nil
}

func (r *outboundRef) Type() string {
	outbound, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return outbound.Type()
}

func (r *outboundRef) Tag() string {
	if !r.useDefault {
		return r.tag
	}
	outbound, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return outbound.Tag()
}

func (r *outboundRef) Network() []string {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return append([]string(nil), outbound.Network()...)
}

func (r *outboundRef) Dependencies() []string {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return append([]string(nil), outbound.Dependencies()...)
}

func (r *outboundRef) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err != nil {
		lease.Release()
		return nil, err
	}
	return newLeasedConn(conn, lease), nil
}

func (r *outboundRef) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err != nil {
		lease.Release()
		return nil, err
	}
	return newLeasedPacketConn(conn, lease), nil
}

func (r *outboundRef) InterfaceUpdated() {
	outbound, lease, err := r.acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	if listener, loaded := outbound.(adapter.InterfaceUpdateListener); loaded {
		listener.InterfaceUpdated()
	}
}

func (r *outboundRef) IsEmpty() bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	if direct, loaded := outbound.(interface{ IsEmpty() bool }); loaded {
		return direct.IsEmpty()
	}
	return false
}

type preferredOutboundRef struct {
	*outboundRef
}

func (r *preferredOutboundRef) PreferredDomain(metadata *adapter.InboundContext, domain string) bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	preferred, loaded := outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredDomain(metadata, domain)
}

func (r *preferredOutboundRef) PreferredAddress(metadata *adapter.InboundContext, address netip.Addr) bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	preferred, loaded := outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredAddress(metadata, address)
}

type flowOutboundRef struct {
	*outboundRef
}

func (r *flowOutboundRef) flow() (adapter.FlowOutbound, adapter.GenerationLease, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, nil, err
	}
	flow, loaded := outbound.(adapter.FlowOutbound)
	if !loaded {
		lease.Release()
		return nil, nil, E.New("outbound is no longer flow-capable: ", r.Tag())
	}
	return flow, lease, nil
}

func (r *flowOutboundRef) PreMatchFlow(network string, destination netip.Addr) adapter.PreMatchAction {
	flow, lease, err := r.flow()
	if err != nil {
		return adapter.PreMatchContinue
	}
	defer lease.Release()
	return flow.PreMatchFlow(network, destination)
}

func (r *flowOutboundRef) PortAddresses() (netip.Addr, netip.Addr) {
	flow, lease, err := r.flow()
	if err != nil {
		return netip.Addr{}, netip.Addr{}
	}
	defer lease.Release()
	return flow.PortAddresses()
}

func (r *flowOutboundRef) PortMTU() uint32 {
	flow, lease, err := r.flow()
	if err != nil {
		return 0
	}
	defer lease.Release()
	return flow.PortMTU()
}

func (r *flowOutboundRef) AttachReturn(returnPath tun.Return) error {
	flow, lease, err := r.flow()
	if err != nil {
		return err
	}
	defer lease.Release()
	return flow.AttachReturn(returnPath)
}

func (r *flowOutboundRef) DetachReturn(returnPath tun.Return) error {
	flow, lease, err := r.flow()
	if err != nil {
		return err
	}
	defer lease.Release()
	return flow.DetachReturn(returnPath)
}

func (r *flowOutboundRef) WritePackets(packets [][]byte) error {
	flow, lease, err := r.flow()
	if err != nil {
		return err
	}
	defer lease.Release()
	return flow.WritePackets(packets)
}

type preferredFlowOutboundRef struct {
	flowOutboundRef
}

func (r *preferredFlowOutboundRef) PreferredDomain(metadata *adapter.InboundContext, domain string) bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	preferred, loaded := outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredDomain(metadata, domain)
}

func (r *preferredFlowOutboundRef) PreferredAddress(metadata *adapter.InboundContext, address netip.Addr) bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	preferred, loaded := outbound.(adapter.OutboundWithPreferredRoutes)
	return loaded && preferred.PreferredAddress(metadata, address)
}

type outboundGroupRef struct {
	*outboundRef
}

func (r *outboundGroupRef) Now() string {
	outbound, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	group, loaded := outbound.(adapter.OutboundGroup)
	if !loaded {
		return ""
	}
	return group.Now()
}

func (r *outboundGroupRef) All() []string {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	group, loaded := outbound.(adapter.OutboundGroup)
	if !loaded {
		return nil
	}
	return append([]string(nil), group.All()...)
}

type urlTestOutboundRef struct {
	outboundGroupRef
}

func (r *urlTestOutboundRef) URLTest(ctx context.Context) (map[string]uint16, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	group, loaded := outbound.(adapter.URLTestGroup)
	if !loaded {
		return nil, E.New("outbound is no longer a URLTest group: ", r.Tag())
	}
	return group.URLTest(ctx)
}

type loadBalanceOutboundRef struct {
	outboundGroupRef
}

func (r *loadBalanceOutboundRef) URLTest(ctx context.Context) (map[string]uint16, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	group, loaded := outbound.(adapter.LoadBalanceGroup)
	if !loaded {
		return nil, E.New("outbound is no longer a load-balance group: ", r.Tag())
	}
	return group.URLTest(ctx)
}

type selectorOutboundRef struct {
	outboundGroupRef
}

func (r *selectorOutboundRef) Selected() adapter.Outbound {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	selector, loaded := outbound.(adapter.SelectorGroup)
	if !loaded {
		lease.Release()
		return nil
	}
	selected := selector.Selected()
	if selected == nil {
		lease.Release()
		return nil
	}
	result := newOutboundRef(r.manager, selected.Tag(), selected)
	lease.Release()
	return result
}

func (r *selectorOutboundRef) SelectOutbound(tag string) bool {
	outbound, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	selector, loaded := outbound.(adapter.SelectorGroup)
	return loaded && selector.SelectOutbound(tag)
}

type smartOutboundRef struct {
	urlTestOutboundRef
}

func (r *smartOutboundRef) smart() (adapter.SmartGroup, adapter.GenerationLease, error) {
	outbound, lease, err := r.acquire()
	if err != nil {
		return nil, nil, err
	}
	group, loaded := outbound.(adapter.SmartGroup)
	if !loaded {
		lease.Release()
		return nil, nil, E.New("outbound is no longer a Smart group: ", r.Tag())
	}
	return group, lease, nil
}

func (r *smartOutboundRef) SmartStatus() adapter.SmartGroupStatus {
	group, lease, err := r.smart()
	if err != nil {
		return adapter.SmartGroupStatus{Reason: err.Error()}
	}
	defer lease.Release()
	return group.SmartStatus()
}

func (r *smartOutboundRef) SelectOutbound(tag string) bool {
	group, lease, err := r.smart()
	if err != nil {
		return false
	}
	defer lease.Release()
	return group.SelectOutbound(tag)
}

func (r *smartOutboundRef) ClearSelection() {
	group, lease, err := r.smart()
	if err != nil {
		return
	}
	defer lease.Release()
	group.ClearSelection()
}

func (r *smartOutboundRef) SelectTemporaryOutbound(tag string, ttl time.Duration, reason string) bool {
	group, lease, err := r.smart()
	if err != nil {
		return false
	}
	defer lease.Release()
	return group.SelectTemporaryOutbound(tag, ttl, reason)
}

func (r *smartOutboundRef) ClearTemporarySelection() {
	group, lease, err := r.smart()
	if err != nil {
		return
	}
	defer lease.Release()
	group.ClearTemporarySelection()
}
