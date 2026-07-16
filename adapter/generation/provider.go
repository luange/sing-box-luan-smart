package generation

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

var _ adapter.ProviderManager = (*ProviderManager)(nil)
var _ adapter.GenerationLeaser = (*ProviderManager)(nil)

type ProviderManager struct {
	manager *Manager
}

func NewProviderManager(manager *Manager) *ProviderManager {
	return &ProviderManager{manager: manager}
}

func (m *ProviderManager) Start(adapter.StartStage) error {
	return nil
}

func (m *ProviderManager) Close() error {
	return nil
}

func (m *ProviderManager) AcquireGeneration() (adapter.GenerationLease, error) {
	_, lease, err := m.manager.Acquire()
	return lease, err
}

func (m *ProviderManager) Providers() []adapter.Provider {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	providers := runtime.Provider.Providers()
	result := make([]adapter.Provider, 0, len(providers))
	for _, provider := range providers {
		if provider != nil {
			result = append(result, newProviderRef(m.manager, provider.Tag(), provider))
		}
	}
	return result
}

func (m *ProviderManager) Get(tag string) (adapter.Provider, bool) {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil, false
	}
	provider, loaded := runtime.Provider.Get(tag)
	lease.Release()
	if !loaded || provider == nil {
		return nil, false
	}
	return newProviderRef(m.manager, tag, provider), true
}

func (m *ProviderManager) Remove(tag string) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Provider.Remove(tag)
}

func (m *ProviderManager) Create(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, providerType string, options any) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Provider.Create(ctx, router, logFactory, tag, providerType, options)
}
