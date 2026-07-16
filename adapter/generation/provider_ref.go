package generation

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/x/list"
)

type providerRef struct {
	manager *Manager
	tag     string
}

func newProviderRef(manager *Manager, tag string, sample adapter.Provider) adapter.Provider {
	base := providerRef{manager: manager, tag: tag}
	_, isUpdater := sample.(adapter.ProviderUpdater)
	_, hasSubscription := sample.(adapter.ProviderSubscriptionInfo)
	switch {
	case isUpdater && hasSubscription:
		return &updatableSubscriptionProviderRef{providerRef: base}
	case isUpdater:
		return &updatableProviderRef{providerRef: base}
	case hasSubscription:
		return &subscriptionProviderRef{providerRef: base}
	default:
		return &base
	}
}

func (r *providerRef) acquire() (adapter.Provider, adapter.GenerationLease, error) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, nil, err
	}
	provider, loaded := runtime.Provider.Get(r.tag)
	if !loaded || provider == nil {
		lease.Release()
		return nil, nil, E.New("provider not found: ", r.tag)
	}
	return provider, lease, nil
}

func (r *providerRef) Type() string {
	provider, lease, err := r.acquire()
	if err != nil {
		return ""
	}
	defer lease.Release()
	return provider.Type()
}

func (r *providerRef) Tag() string { return r.tag }

func (r *providerRef) Outbounds() []adapter.Outbound {
	provider, lease, err := r.acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	outbounds := provider.Outbounds()
	result := make([]adapter.Outbound, 0, len(outbounds))
	for _, outbound := range outbounds {
		if outbound != nil {
			result = append(result, newOutboundRef(r.manager, outbound.Tag(), outbound))
		}
	}
	return result
}

func (r *providerRef) Outbound(tag string) (adapter.Outbound, bool) {
	provider, lease, err := r.acquire()
	if err != nil {
		return nil, false
	}
	outbound, loaded := provider.Outbound(tag)
	lease.Release()
	if !loaded || outbound == nil {
		return nil, false
	}
	return newOutboundRef(r.manager, tag, outbound), true
}

func (r *providerRef) UpdatedAt() time.Time {
	provider, lease, err := r.acquire()
	if err != nil {
		return time.Time{}
	}
	defer lease.Release()
	return provider.UpdatedAt()
}

func (r *providerRef) HealthCheck(ctx context.Context) (map[string]uint16, error) {
	provider, lease, err := r.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return provider.HealthCheck(ctx)
}

func (r *providerRef) RegisterCallback(callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	return r.manager.registerProviderCallback(r.tag, callback)
}

func (r *providerRef) UnregisterCallback(element *list.Element[adapter.ProviderUpdateCallback]) {
	r.manager.unregisterProviderCallback(element)
}

type updatableProviderRef struct {
	providerRef
}

func (r *updatableProviderRef) Update() error {
	provider, lease, err := r.acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	updater, loaded := provider.(adapter.ProviderUpdater)
	if !loaded {
		return E.New("provider is no longer updatable: ", r.tag)
	}
	return updater.Update()
}

type subscriptionProviderRef struct {
	providerRef
}

func (r *subscriptionProviderRef) SubscriptionInfo() adapter.SubscriptionInfo {
	provider, lease, err := r.acquire()
	if err != nil {
		return adapter.SubscriptionInfo{}
	}
	defer lease.Release()
	subscription, loaded := provider.(adapter.ProviderSubscriptionInfo)
	if !loaded {
		return adapter.SubscriptionInfo{}
	}
	return subscription.SubscriptionInfo()
}

type updatableSubscriptionProviderRef struct {
	providerRef
}

func (r *updatableSubscriptionProviderRef) Update() error {
	provider, lease, err := r.acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	updater, loaded := provider.(adapter.ProviderUpdater)
	if !loaded {
		return E.New("provider is no longer updatable: ", r.tag)
	}
	return updater.Update()
}

func (r *updatableSubscriptionProviderRef) SubscriptionInfo() adapter.SubscriptionInfo {
	provider, lease, err := r.acquire()
	if err != nil {
		return adapter.SubscriptionInfo{}
	}
	defer lease.Release()
	subscription, loaded := provider.(adapter.ProviderSubscriptionInfo)
	if !loaded {
		return adapter.SubscriptionInfo{}
	}
	return subscription.SubscriptionInfo()
}
