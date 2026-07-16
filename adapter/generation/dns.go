package generation

import (
	"context"
	"net/netip"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"

	"github.com/miekg/dns"
)

var _ adapter.DNSRouter = (*DNSRouter)(nil)
var _ adapter.DNSRuleSetUpdateValidator = (*DNSRouter)(nil)
var _ adapter.GenerationLeaser = (*DNSRouter)(nil)
var _ adapter.DNSTransportManager = (*DNSTransportManager)(nil)
var _ adapter.GenerationLeaser = (*DNSTransportManager)(nil)

type DNSRouter struct {
	manager *Manager
}

func NewDNSRouter(manager *Manager) *DNSRouter {
	return &DNSRouter{manager: manager}
}

func (r *DNSRouter) Start(adapter.StartStage) error {
	return nil
}

func (r *DNSRouter) Close() error {
	return nil
}

func (r *DNSRouter) AcquireGeneration() (adapter.GenerationLease, error) {
	_, lease, err := r.manager.Acquire()
	return lease, err
}

func (r *DNSRouter) Exchange(ctx context.Context, message *dns.Msg, options adapter.DNSQueryOptions) (*dns.Msg, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return runtime.DNSRouter.Exchange(ctx, message, options)
}

func (r *DNSRouter) Lookup(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return runtime.DNSRouter.Lookup(ctx, domain, options)
}

func (r *DNSRouter) ClearCache() {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	runtime.DNSRouter.ClearCache()
}

func (r *DNSRouter) LookupReverseMapping(ip netip.Addr) (string, bool) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return "", false
	}
	defer lease.Release()
	return runtime.DNSRouter.LookupReverseMapping(ip)
}

func (r *DNSRouter) Rules() []adapter.DNSRule {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return runtime.DNSRouter.Rules()
}

func (r *DNSRouter) Rule(uuid string) (adapter.DNSRule, bool) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, false
	}
	defer lease.Release()
	return runtime.DNSRouter.Rule(uuid)
}

func (r *DNSRouter) ResetNetwork() {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	runtime.DNSRouter.ResetNetwork()
}

func (r *DNSRouter) ValidateRuleSetMetadataUpdate(tag string, metadata adapter.RuleSetMetadata) error {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	validator, loaded := runtime.DNSRouter.(adapter.DNSRuleSetUpdateValidator)
	if !loaded {
		return nil
	}
	return validator.ValidateRuleSetMetadataUpdate(tag, metadata)
}

type DNSTransportManager struct {
	manager    *Manager
	defaultRef adapter.DNSTransport
	fakeIPRef  adapter.FakeIPTransport
}

func NewDNSTransportManager(manager *Manager) *DNSTransportManager {
	return &DNSTransportManager{
		manager:    manager,
		defaultRef: newDefaultDNSTransportRef(manager),
		fakeIPRef:  newFakeIPTransportRef(manager),
	}
}

func (m *DNSTransportManager) Start(adapter.StartStage) error {
	return nil
}

func (m *DNSTransportManager) Close() error {
	return nil
}

func (m *DNSTransportManager) AcquireGeneration() (adapter.GenerationLease, error) {
	_, lease, err := m.manager.Acquire()
	return lease, err
}

func (m *DNSTransportManager) Transports() []adapter.DNSTransport {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	transports := runtime.DNSTransport.Transports()
	result := make([]adapter.DNSTransport, 0, len(transports))
	for _, transport := range transports {
		if transport != nil {
			result = append(result, newDNSTransportRef(m.manager, transport.Tag(), transport))
		}
	}
	return result
}

func (m *DNSTransportManager) Transport(tag string) (adapter.DNSTransport, bool) {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil, false
	}
	transport, loaded := runtime.DNSTransport.Transport(tag)
	lease.Release()
	if !loaded || transport == nil {
		return nil, false
	}
	return newDNSTransportRef(m.manager, tag, transport), true
}

func (m *DNSTransportManager) Default() adapter.DNSTransport {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	transport := runtime.DNSTransport.Default()
	lease.Release()
	if transport == nil {
		return nil
	}
	return m.defaultRef
}

func (m *DNSTransportManager) FakeIP() adapter.FakeIPTransport {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	transport := runtime.DNSTransport.FakeIP()
	lease.Release()
	if transport == nil {
		return nil
	}
	return m.fakeIPRef
}

func (m *DNSTransportManager) Remove(tag string) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.DNSTransport.Remove(tag)
}

func (m *DNSTransportManager) Create(ctx context.Context, logger log.ContextLogger, tag string, transportType string, options any) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.DNSTransport.Create(ctx, logger, tag, transportType, options)
}
