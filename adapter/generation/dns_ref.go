package generation

import (
	"context"
	"net/netip"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/miekg/dns"
)

type dnsTransportRef struct {
	manager    *Manager
	tag        string
	useDefault bool
}

func newDNSTransportRef(manager *Manager, tag string, sample adapter.DNSTransport) adapter.DNSTransport {
	base := dnsTransportRef{manager: manager, tag: tag}
	if _, loaded := sample.(adapter.DNSTransportWithPreferredDomain); loaded {
		return &preferredDNSTransportRef{dnsTransportRef: base}
	}
	return &base
}

func newDefaultDNSTransportRef(manager *Manager) adapter.DNSTransport {
	return &dnsTransportRef{manager: manager, useDefault: true}
}

func (r *dnsTransportRef) acquire() (adapter.DNSTransport, adapter.GenerationLease, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, nil, err
	}
	if r.useDefault {
		transport := runtime.DNSTransport.Default()
		if transport == nil {
			lease.Release()
			return nil, nil, E.New("default DNS transport is unavailable")
		}
		return transport, lease, nil
	}
	transport, loaded := runtime.DNSTransport.Transport(r.tag)
	if !loaded {
		lease.Release()
		return nil, nil, E.New("DNS transport not found: ", r.tag)
	}
	return transport, lease, nil
}

func (r *dnsTransportRef) Start(adapter.StartStage) error { return nil }
func (r *dnsTransportRef) Close() error                   { return nil }

func (r *dnsTransportRef) Type() string {
	transport, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return transport.Type()
}

func (r *dnsTransportRef) Tag() string {
	if !r.useDefault {
		return r.tag
	}
	transport, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return transport.Tag()
}

func (r *dnsTransportRef) Dependencies() []string {
	transport, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return append([]string(nil), transport.Dependencies()...)
}

func (r *dnsTransportRef) Reset() {
	transport, lease, err := r.acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	transport.Reset()
}

func (r *dnsTransportRef) Exchange(ctx context.Context, message *dns.Msg) (*dns.Msg, error) {
	transport, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return transport.Exchange(ctx, message)
}

type preferredDNSTransportRef struct {
	dnsTransportRef
}

func (r *preferredDNSTransportRef) PreferredDomain(domain string) bool {
	transport, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	preferred, loaded := transport.(adapter.DNSTransportWithPreferredDomain)
	return loaded && preferred.PreferredDomain(domain)
}

type fakeIPTransportRef struct {
	dnsTransportRef
	store fakeIPStoreRef
}

func newFakeIPTransportRef(manager *Manager) adapter.FakeIPTransport {
	return &fakeIPTransportRef{
		dnsTransportRef: dnsTransportRef{manager: manager},
		store:           fakeIPStoreRef{manager: manager},
	}
}

func (r *fakeIPTransportRef) acquire() (adapter.DNSTransport, adapter.GenerationLease, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, nil, err
	}
	transport := runtime.DNSTransport.FakeIP()
	if transport == nil {
		lease.Release()
		return nil, nil, E.New("FakeIP transport is unavailable")
	}
	return transport, lease, nil
}

func (r *fakeIPTransportRef) Type() string {
	transport, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return transport.Type()
}

func (r *fakeIPTransportRef) Tag() string {
	transport, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return transport.Tag()
}

func (r *fakeIPTransportRef) Dependencies() []string {
	transport, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return append([]string(nil), transport.Dependencies()...)
}

func (r *fakeIPTransportRef) Reset() {
	transport, lease, err := r.acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	transport.Reset()
}

func (r *fakeIPTransportRef) Exchange(ctx context.Context, message *dns.Msg) (*dns.Msg, error) {
	transport, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return transport.Exchange(ctx, message)
}

func (r *fakeIPTransportRef) Store() adapter.FakeIPStore { return &r.store }

type fakeIPStoreRef struct {
	manager *Manager
}

func (r *fakeIPStoreRef) acquire() (adapter.FakeIPStore, adapter.GenerationLease, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, nil, err
	}
	transport := runtime.DNSTransport.FakeIP()
	if transport == nil || transport.Store() == nil {
		lease.Release()
		return nil, nil, E.New("FakeIP store is unavailable")
	}
	return transport.Store(), lease, nil
}

func (r *fakeIPStoreRef) Start() error { return nil }
func (r *fakeIPStoreRef) Close() error { return nil }

func (r *fakeIPStoreRef) Contains(address netip.Addr) bool {
	store, lease, err := r.acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	return store.Contains(address)
}

func (r *fakeIPStoreRef) Create(domain string, isIPv6 bool) (netip.Addr, error) {
	store, lease, err := r.acquire()
	if err != nil {
		return netip.Addr{}, err
	}
	defer lease.Release()
	return store.Create(domain, isIPv6)
}

func (r *fakeIPStoreRef) Lookup(address netip.Addr) (string, bool) {
	store, lease, err := r.acquire()
	if err != nil {
		return "", false
	}
	defer lease.Release()
	return store.Lookup(address)
}

func (r *fakeIPStoreRef) Reset() error {
	store, lease, err := r.acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return store.Reset()
}
