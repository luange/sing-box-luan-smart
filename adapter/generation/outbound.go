package generation

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

var _ adapter.OutboundManager = (*OutboundManager)(nil)
var _ adapter.GenerationLeaser = (*OutboundManager)(nil)

type OutboundManager struct {
	manager    *Manager
	defaultRef adapter.Outbound
}

func NewOutboundManager(manager *Manager) *OutboundManager {
	m := &OutboundManager{manager: manager}
	m.defaultRef = newDefaultOutboundRef(manager)
	return m
}

func (m *OutboundManager) Start(adapter.StartStage) error {
	return nil
}

func (m *OutboundManager) Close() error {
	return nil
}

func (m *OutboundManager) AcquireGeneration() (adapter.GenerationLease, error) {
	_, lease, err := m.manager.Acquire()
	return lease, err
}

func (m *OutboundManager) Outbounds() []adapter.Outbound {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	outbounds := runtime.Outbound.Outbounds()
	endpoints := runtime.Endpoint.Endpoints()
	result := make([]adapter.Outbound, 0, len(outbounds)+len(endpoints))
	seen := make(map[string]bool, len(outbounds)+len(endpoints))
	for _, outbound := range outbounds {
		if outbound != nil {
			result = append(result, newOutboundRef(m.manager, outbound.Tag(), outbound))
			seen[outbound.Tag()] = true
		}
	}
	for _, endpoint := range endpoints {
		if endpoint != nil && !seen[endpoint.Tag()] {
			result = append(result, newOutboundRef(m.manager, endpoint.Tag(), endpoint))
		}
	}
	return result
}

func (m *OutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil, false
	}
	outbound, loaded := runtime.Outbound.Outbound(tag)
	lease.Release()
	if !loaded || outbound == nil {
		return nil, false
	}
	return newOutboundRef(m.manager, tag, outbound), true
}

func (m *OutboundManager) Default() adapter.Outbound {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return nil
	}
	defaultOutbound := runtime.Outbound.Default()
	lease.Release()
	if defaultOutbound == nil {
		return nil
	}
	return m.defaultRef
}

func (m *OutboundManager) Remove(tag string) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Outbound.Remove(tag)
}

func (m *OutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	runtime, lease, err := m.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Outbound.Create(ctx, router, logger, tag, outboundType, options)
}
