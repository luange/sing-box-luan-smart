package generation

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	sing_tun "github.com/sagernet/sing-tun"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/x/list"

	"github.com/miekg/dns"
)

type referenceOutbound struct {
	tag      string
	typeName string
	dial     func() (net.Conn, error)
}

func (o *referenceOutbound) Type() string           { return o.typeName }
func (o *referenceOutbound) Tag() string            { return o.tag }
func (o *referenceOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (o *referenceOutbound) Dependencies() []string { return nil }

func (o *referenceOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return o.dial()
}

func (o *referenceOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

type referenceFlowOutbound struct {
	*referenceOutbound
}

type referenceOutboundGroup struct {
	*referenceOutbound
}

func (*referenceOutboundGroup) Now() string   { return "proxy" }
func (*referenceOutboundGroup) All() []string { return []string{"proxy"} }
func (*referenceOutboundGroup) URLTest(context.Context) (map[string]uint16, error) {
	return map[string]uint16{"proxy": 1}, nil
}

type referenceEndpoint struct {
	*referenceFlowOutbound
}

func (*referenceEndpoint) Start(adapter.StartStage) error { return nil }
func (*referenceEndpoint) Close() error                   { return nil }

func (*referenceFlowOutbound) PreMatchFlow(string, netip.Addr) adapter.PreMatchAction {
	return adapter.PreMatchFlow
}
func (*referenceFlowOutbound) PortAddresses() (netip.Addr, netip.Addr) {
	return netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("fd00::1")
}
func (*referenceFlowOutbound) PortMTU() uint32                    { return 1400 }
func (*referenceFlowOutbound) AttachReturn(sing_tun.Return) error { return nil }
func (*referenceFlowOutbound) DetachReturn(sing_tun.Return) error { return nil }
func (*referenceFlowOutbound) WritePackets([][]byte) error        { return nil }
func (*referenceFlowOutbound) PreferredDomain(*adapter.InboundContext, string) bool {
	return true
}
func (*referenceFlowOutbound) PreferredAddress(*adapter.InboundContext, netip.Addr) bool {
	return true
}

type referenceOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

type referenceEndpointManager struct {
	adapter.EndpointManager
	endpoints []adapter.Endpoint
}

func (m *referenceEndpointManager) Endpoints() []adapter.Endpoint { return m.endpoints }
func (m *referenceEndpointManager) Get(tag string) (adapter.Endpoint, bool) {
	for _, endpoint := range m.endpoints {
		if endpoint.Tag() == tag {
			return endpoint, true
		}
	}
	return nil, false
}

func (m *referenceOutboundManager) Outbounds() []adapter.Outbound { return m.outbounds }
func (m *referenceOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, outbound := range m.outbounds {
		if outbound.Tag() == tag {
			return outbound, true
		}
	}
	return nil, false
}
func (m *referenceOutboundManager) Default() adapter.Outbound {
	if len(m.outbounds) == 0 {
		return nil
	}
	return m.outbounds[0]
}

type referenceDNSTransport struct {
	tag             string
	typeName        string
	preferredDomain bool
	exchange        func(context.Context, *dns.Msg) (*dns.Msg, error)
}

func (*referenceDNSTransport) Start(adapter.StartStage) error { return nil }
func (*referenceDNSTransport) Close() error                   { return nil }
func (t *referenceDNSTransport) Type() string                 { return t.typeName }
func (t *referenceDNSTransport) Tag() string                  { return t.tag }
func (*referenceDNSTransport) Dependencies() []string         { return nil }
func (*referenceDNSTransport) Reset()                         {}
func (t *referenceDNSTransport) Exchange(ctx context.Context, message *dns.Msg) (*dns.Msg, error) {
	return t.exchange(ctx, message)
}
func (t *referenceDNSTransport) PreferredDomain(string) bool { return t.preferredDomain }

type referenceDNSTransportManager struct {
	adapter.DNSTransportManager
	transports []adapter.DNSTransport
}

func (m *referenceDNSTransportManager) Transports() []adapter.DNSTransport { return m.transports }
func (m *referenceDNSTransportManager) Transport(tag string) (adapter.DNSTransport, bool) {
	for _, transport := range m.transports {
		if transport.Tag() == tag {
			return transport, true
		}
	}
	return nil, false
}
func (m *referenceDNSTransportManager) Default() adapter.DNSTransport {
	if len(m.transports) == 0 {
		return nil
	}
	return m.transports[0]
}
func (*referenceDNSTransportManager) FakeIP() adapter.FakeIPTransport { return nil }

type referenceProvider struct {
	tag       string
	typeName  string
	outbounds []adapter.Outbound
	access    sync.Mutex
	callbacks list.List[adapter.ProviderUpdateCallback]
}

func (p *referenceProvider) Type() string                  { return p.typeName }
func (p *referenceProvider) Tag() string                   { return p.tag }
func (p *referenceProvider) Outbounds() []adapter.Outbound { return p.outbounds }
func (p *referenceProvider) Outbound(tag string) (adapter.Outbound, bool) {
	for _, outbound := range p.outbounds {
		if outbound.Tag() == tag {
			return outbound, true
		}
	}
	return nil, false
}
func (*referenceProvider) UpdatedAt() time.Time { return time.Time{} }
func (*referenceProvider) HealthCheck(context.Context) (map[string]uint16, error) {
	return nil, nil
}
func (p *referenceProvider) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	p.access.Lock()
	defer p.access.Unlock()
	return p.callbacks.PushBack(callback)
}
func (p *referenceProvider) UnregisterCallback(handle *list.Element[adapter.ProviderUpdateCallback]) {
	p.access.Lock()
	defer p.access.Unlock()
	p.callbacks.Remove(handle)
}
func (p *referenceProvider) notify() {
	p.access.Lock()
	callbacks := make([]adapter.ProviderUpdateCallback, 0, p.callbacks.Len())
	for element := p.callbacks.Front(); element != nil; element = element.Next() {
		callbacks = append(callbacks, element.Value)
	}
	p.access.Unlock()
	for _, callback := range callbacks {
		_ = callback(p.tag)
	}
}

type referenceProviderManager struct {
	adapter.ProviderManager
	providers []adapter.Provider
}

func (m *referenceProviderManager) Providers() []adapter.Provider { return m.providers }
func (m *referenceProviderManager) Get(tag string) (adapter.Provider, bool) {
	for _, provider := range m.providers {
		if provider.Tag() == tag {
			return provider, true
		}
	}
	return nil, false
}

type validatingDNSRouter struct {
	adapter.DNSRouter
	validated atomic.Int32
}

func (r *validatingDNSRouter) ValidateRuleSetMetadataUpdate(string, adapter.RuleSetMetadata) error {
	r.validated.Add(1)
	return nil
}

func referenceRuntime(
	outbound adapter.OutboundManager,
	dnsRouter adapter.DNSRouter,
	dnsTransport adapter.DNSTransportManager,
	provider adapter.ProviderManager,
	closeCount *atomic.Int32,
	closed chan struct{},
) Runtime {
	runtime := testRuntime(&testRouter{}, closeCount, closed)
	if outbound != nil {
		runtime.Outbound = outbound
	}
	if dnsRouter != nil {
		runtime.DNSRouter = dnsRouter
	}
	if dnsTransport != nil {
		runtime.DNSTransport = dnsTransport
	}
	if provider != nil {
		runtime.Provider = provider
	}
	return runtime
}

func TestOutboundRefGroupDispatchUsesOutboundType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		assert   func(adapter.Outbound) bool
	}{
		{"smart", C.TypeSmart, func(outbound adapter.Outbound) bool { _, ok := outbound.(*smartOutboundRef); return ok }},
		{"selector", C.TypeSelector, func(outbound adapter.Outbound) bool { _, ok := outbound.(*selectorOutboundRef); return ok }},
		{"loadbalance", C.TypeLoadBalance, func(outbound adapter.Outbound) bool { _, ok := outbound.(*loadBalanceOutboundRef); return ok }},
		{"urltest", C.TypeURLTest, func(outbound adapter.Outbound) bool { _, ok := outbound.(*urlTestOutboundRef); return ok }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sample := &referenceOutboundGroup{referenceOutbound: &referenceOutbound{typeName: test.typeName}}
			if outbound := newOutboundRefWithBase(&outboundRef{}, sample); !test.assert(outbound) {
				t.Fatalf("unexpected wrapper type %T for %s", outbound, test.typeName)
			}
		})
	}
}

func TestCachedOutboundRefUsesCurrentGeneration(t *testing.T) {
	manager := NewManager()
	firstPeer := make(chan net.Conn, 1)
	firstOutbound := &referenceOutbound{tag: "proxy", typeName: "first", dial: func() (net.Conn, error) {
		client, server := net.Pipe()
		firstPeer <- server
		return client, nil
	}}
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(referenceRuntime(&referenceOutboundManager{outbounds: []adapter.Outbound{firstOutbound}}, nil, nil, nil, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	stableManager := NewOutboundManager(manager)
	stableOutbound, loaded := stableManager.Outbound("proxy")
	if !loaded {
		t.Fatal("stable outbound reference was not created")
	}
	conn, err := stableOutbound.DialContext(context.Background(), "tcp", M.Socksaddr{})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-firstPeer
	defer peer.Close()

	secondOutbound := &referenceOutbound{tag: "proxy", typeName: "second", dial: func() (net.Conn, error) {
		return nil, net.ErrClosed
	}}
	if _, err = manager.Publish(referenceRuntime(&referenceOutboundManager{outbounds: []adapter.Outbound{secondOutbound}}, nil, nil, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	if stableOutbound.Type() != "second" {
		t.Fatalf("cached reference did not switch generation: %q", stableOutbound.Type())
	}
	select {
	case <-firstClosed:
		t.Fatal("old generation closed while a cached outbound connection was active")
	default:
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("old generation did not close after cached outbound connection closed")
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCachedDNSTransportRefUsesCurrentGeneration(t *testing.T) {
	manager := NewManager()
	firstTransport := &referenceDNSTransport{tag: "dns", typeName: "first", exchange: func(context.Context, *dns.Msg) (*dns.Msg, error) {
		return &dns.Msg{MsgHdr: dns.MsgHdr{Id: 1}}, nil
	}}
	if _, err := manager.Publish(referenceRuntime(nil, nil, &referenceDNSTransportManager{transports: []adapter.DNSTransport{firstTransport}}, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	stableManager := NewDNSTransportManager(manager)
	stableTransport, loaded := stableManager.Transport("dns")
	if !loaded {
		t.Fatal("stable DNS transport reference was not created")
	}
	secondTransport := &referenceDNSTransport{tag: "dns", typeName: "second", exchange: func(context.Context, *dns.Msg) (*dns.Msg, error) {
		return &dns.Msg{MsgHdr: dns.MsgHdr{Id: 2}}, nil
	}}
	if _, err := manager.Publish(referenceRuntime(nil, nil, &referenceDNSTransportManager{transports: []adapter.DNSTransport{secondTransport}}, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	response, err := stableTransport.Exchange(context.Background(), new(dns.Msg))
	if err != nil {
		t.Fatal(err)
	}
	if response.Id != 2 || stableTransport.Type() != "second" {
		t.Fatalf("cached DNS reference did not switch generation: id=%d type=%q", response.Id, stableTransport.Type())
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDNSExchangeHoldsGenerationLease(t *testing.T) {
	manager := NewManager()
	started := make(chan struct{})
	releaseExchange := make(chan struct{})
	firstTransport := &referenceDNSTransport{tag: "dns", typeName: "first", exchange: func(context.Context, *dns.Msg) (*dns.Msg, error) {
		close(started)
		<-releaseExchange
		return new(dns.Msg), nil
	}}
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(referenceRuntime(nil, nil, &referenceDNSTransportManager{transports: []adapter.DNSTransport{firstTransport}}, nil, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	stableTransport, _ := NewDNSTransportManager(manager).Transport("dns")
	exchangeDone := make(chan error, 1)
	go func() {
		_, err := stableTransport.Exchange(context.Background(), new(dns.Msg))
		exchangeDone <- err
	}()
	<-started
	if _, err := manager.Publish(referenceRuntime(nil, nil, &referenceDNSTransportManager{}, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
		t.Fatal("old generation closed during an active DNS exchange")
	default:
	}
	close(releaseExchange)
	if err := <-exchangeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("old generation did not close after DNS exchange completed")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStableReferencesPreserveAlpha44OptionalInterfaces(t *testing.T) {
	manager := NewManager()
	flow := &referenceFlowOutbound{referenceOutbound: &referenceOutbound{
		tag: "flow", typeName: "flow", dial: func() (net.Conn, error) { return nil, net.ErrClosed },
	}}
	transport := &referenceDNSTransport{
		tag: "preferred", typeName: "preferred", preferredDomain: true,
		exchange: func(context.Context, *dns.Msg) (*dns.Msg, error) { return new(dns.Msg), nil },
	}
	if _, err := manager.Publish(referenceRuntime(
		&referenceOutboundManager{outbounds: []adapter.Outbound{flow}},
		nil,
		&referenceDNSTransportManager{transports: []adapter.DNSTransport{transport}},
		nil,
		new(atomic.Int32),
		nil,
	)); err != nil {
		t.Fatal(err)
	}
	stableOutbound, _ := NewOutboundManager(manager).Outbound("flow")
	if _, loaded := stableOutbound.(adapter.FlowOutbound); !loaded {
		t.Fatal("stable outbound lost FlowOutbound capability")
	}
	if _, loaded := stableOutbound.(adapter.OutboundWithPreferredRoutes); !loaded {
		t.Fatal("stable outbound lost preferred-route capability")
	}
	stableTransport, _ := NewDNSTransportManager(manager).Transport("preferred")
	preferred, loaded := stableTransport.(adapter.DNSTransportWithPreferredDomain)
	if !loaded || !preferred.PreferredDomain("example.com.") {
		t.Fatal("stable DNS transport lost preferred-domain capability")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationEndpointViewPreservesCapabilitiesAndSwitches(t *testing.T) {
	manager := NewManager()
	first := &referenceEndpoint{referenceFlowOutbound: &referenceFlowOutbound{referenceOutbound: &referenceOutbound{
		tag: "provider/wg", typeName: "first", dial: func() (net.Conn, error) { return nil, net.ErrClosed },
	}}}
	firstRuntime := referenceRuntime(&referenceOutboundManager{}, nil, nil, nil, new(atomic.Int32), nil)
	firstRuntime.Endpoint = &referenceEndpointManager{endpoints: []adapter.Endpoint{first}}
	if _, err := manager.Publish(firstRuntime); err != nil {
		t.Fatal(err)
	}
	view := NewEndpointManager(manager, &referenceEndpointManager{})
	stable, loaded := view.Get(first.Tag())
	if !loaded {
		t.Fatal("generation endpoint is missing from stable view")
	}
	if _, loaded = stable.(adapter.FlowOutbound); !loaded {
		t.Fatal("generation endpoint lost FlowOutbound capability")
	}
	if _, loaded = stable.(adapter.OutboundWithPreferredRoutes); !loaded {
		t.Fatal("generation endpoint lost preferred-route capability")
	}

	second := &referenceEndpoint{referenceFlowOutbound: &referenceFlowOutbound{referenceOutbound: &referenceOutbound{
		tag: "provider/wg", typeName: "second", dial: func() (net.Conn, error) { return nil, net.ErrClosed },
	}}}
	secondRuntime := referenceRuntime(&referenceOutboundManager{}, nil, nil, nil, new(atomic.Int32), nil)
	secondRuntime.Endpoint = &referenceEndpointManager{endpoints: []adapter.Endpoint{second}}
	if _, err := manager.Publish(secondRuntime); err != nil {
		t.Fatal(err)
	}
	if stable.Type() != "second" {
		t.Fatalf("cached endpoint reference did not switch generation: %q", stable.Type())
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDNSRuleSetValidatorUsesCurrentGeneration(t *testing.T) {
	manager := NewManager()
	first := &validatingDNSRouter{}
	if _, err := manager.Publish(referenceRuntime(nil, first, nil, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	stableRouter := NewDNSRouter(manager)
	second := &validatingDNSRouter{}
	if _, err := manager.Publish(referenceRuntime(nil, second, nil, nil, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	if err := stableRouter.ValidateRuleSetMetadataUpdate("rules", adapter.RuleSetMetadata{}); err != nil {
		t.Fatal(err)
	}
	if first.validated.Load() != 0 || second.validated.Load() != 1 {
		t.Fatalf("validator used wrong generation: first=%d second=%d", first.validated.Load(), second.validated.Load())
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderRefAndCallbackMoveToCurrentGeneration(t *testing.T) {
	manager := NewManager()
	firstOutbound := &referenceOutbound{tag: "leaf", typeName: "first"}
	first := &referenceProvider{tag: "pool", typeName: "first", outbounds: []adapter.Outbound{firstOutbound}}
	if _, err := manager.Publish(referenceRuntime(
		&referenceOutboundManager{outbounds: []adapter.Outbound{firstOutbound}},
		nil,
		nil,
		&referenceProviderManager{providers: []adapter.Provider{first}},
		new(atomic.Int32),
		nil,
	)); err != nil {
		t.Fatal(err)
	}
	stableProvider, loaded := NewProviderManager(manager).Get("pool")
	if !loaded {
		t.Fatal("stable provider reference was not created")
	}
	stableLeaf, loaded := stableProvider.Outbound("leaf")
	if !loaded {
		t.Fatal("stable provider outbound reference was not created")
	}
	var callbackCount atomic.Int32
	handle := stableProvider.RegisterCallback(func(string) error {
		callbackCount.Add(1)
		return nil
	})
	if handle == nil {
		t.Fatal("provider callback registration failed")
	}
	first.notify()
	if callbackCount.Load() != 1 {
		t.Fatal("callback was not attached to first generation")
	}

	secondOutbound := &referenceOutbound{tag: "leaf", typeName: "second"}
	second := &referenceProvider{tag: "pool", typeName: "second", outbounds: []adapter.Outbound{secondOutbound}}
	if _, err := manager.Publish(referenceRuntime(
		&referenceOutboundManager{outbounds: []adapter.Outbound{secondOutbound}},
		nil,
		nil,
		&referenceProviderManager{providers: []adapter.Provider{second}},
		new(atomic.Int32),
		nil,
	)); err != nil {
		t.Fatal(err)
	}
	if stableProvider.Type() != "second" || stableLeaf.Type() != "second" {
		t.Fatalf("provider references did not switch generation: provider=%q leaf=%q", stableProvider.Type(), stableLeaf.Type())
	}
	first.notify()
	if callbackCount.Load() != 1 {
		t.Fatal("callback remained attached to retired provider")
	}
	second.notify()
	if callbackCount.Load() != 2 {
		t.Fatal("callback was not moved to current provider")
	}
	stableProvider.UnregisterCallback(handle)
	second.notify()
	if callbackCount.Load() != 2 {
		t.Fatal("provider callback was not unregistered")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}
