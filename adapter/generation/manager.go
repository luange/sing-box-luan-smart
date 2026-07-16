package generation

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/x/list"
)

var ErrNoGeneration = errors.New("no active runtime generation")

type Runtime struct {
	ID           uint64
	Router       adapter.Router
	DNSRouter    adapter.DNSRouter
	DNSTransport adapter.DNSTransportManager
	Outbound     adapter.OutboundManager
	Provider     adapter.ProviderManager
	Endpoint     adapter.EndpointManager
	Publish      func()
	Retire       func()
	Close        func() error
}

func (r Runtime) validate() error {
	switch {
	case r.Router == nil:
		return errors.New("missing generation router")
	case r.DNSRouter == nil:
		return errors.New("missing generation DNS router")
	case r.DNSTransport == nil:
		return errors.New("missing generation DNS transport manager")
	case r.Outbound == nil:
		return errors.New("missing generation outbound manager")
	case r.Provider == nil:
		return errors.New("missing generation provider manager")
	case r.Endpoint == nil:
		return errors.New("missing generation endpoint manager")
	case r.Close == nil:
		return errors.New("missing generation close function")
	default:
		return nil
	}
}

type runtimeState struct {
	runtime         Runtime
	leases          int64
	retired         bool
	published       bool
	retiredNotified bool
	closing         bool
	closeErr        error
	done            chan struct{}
}

type Manager struct {
	access             sync.Mutex
	current            *runtimeState
	states             map[*runtimeState]struct{}
	trackers           []adapter.ConnectionTracker
	providerCallbacks  list.List[adapter.ProviderUpdateCallback]
	providerCallbackBy map[*list.Element[adapter.ProviderUpdateCallback]]*providerCallbackRegistration
	closeErr           error
	nextID             uint64
	closed             bool
}

type providerCallbackRegistration struct {
	tag      string
	callback adapter.ProviderUpdateCallback
	provider adapter.Provider
	handle   *list.Element[adapter.ProviderUpdateCallback]
}

func NewManager() *Manager {
	return &Manager{
		states:             make(map[*runtimeState]struct{}),
		providerCallbackBy: make(map[*list.Element[adapter.ProviderUpdateCallback]]*providerCallbackRegistration),
	}
}

func (m *Manager) Publish(runtime Runtime) (uint64, error) {
	if err := runtime.validate(); err != nil {
		return 0, err
	}
	m.access.Lock()
	if m.closed {
		m.access.Unlock()
		return 0, errors.New("generation manager is closed")
	}
	if runtime.ID == 0 {
		m.nextID++
		runtime.ID = m.nextID
	} else if runtime.ID > m.nextID {
		m.nextID = runtime.ID
	}
	if runtime.Publish != nil {
		runtime.Publish()
	}
	m.rebindProviderCallbacksLocked(runtime.Provider)
	for _, tracker := range m.trackers {
		runtime.Router.AppendTracker(tracker)
	}
	next := &runtimeState{
		runtime:   runtime,
		done:      make(chan struct{}),
		published: true,
	}
	previous := m.current
	m.current = next
	m.states[next] = struct{}{}
	if previous != nil {
		m.retireLocked(previous)
		m.startCloseLocked(previous)
	}
	m.access.Unlock()
	return runtime.ID, nil
}

func (m *Manager) PrepareInitial(runtime Runtime) (uint64, error) {
	if err := runtime.validate(); err != nil {
		return 0, err
	}
	m.access.Lock()
	defer m.access.Unlock()
	if m.closed {
		return 0, errors.New("generation manager is closed")
	}
	if m.current != nil || len(m.states) != 0 {
		return 0, errors.New("initial generation is already prepared")
	}
	if runtime.ID == 0 {
		m.nextID++
		runtime.ID = m.nextID
	} else if runtime.ID > m.nextID {
		m.nextID = runtime.ID
	}
	for _, tracker := range m.trackers {
		runtime.Router.AppendTracker(tracker)
	}
	state := &runtimeState{
		runtime: runtime,
		done:    make(chan struct{}),
	}
	m.current = state
	m.states[state] = struct{}{}
	m.rebindProviderCallbacksLocked(runtime.Provider)
	return runtime.ID, nil
}

func (m *Manager) ActivateInitial() (uint64, error) {
	m.access.Lock()
	defer m.access.Unlock()
	if m.closed {
		return 0, errors.New("generation manager is closed")
	}
	if m.current == nil || m.current.retired {
		return 0, ErrNoGeneration
	}
	if m.current.published {
		return m.current.runtime.ID, nil
	}
	if m.current.runtime.Publish != nil {
		m.current.runtime.Publish()
	}
	m.current.published = true
	return m.current.runtime.ID, nil
}

func (m *Manager) AppendTracker(tracker adapter.ConnectionTracker) {
	m.access.Lock()
	m.trackers = append(m.trackers, tracker)
	if m.current != nil {
		m.current.runtime.Router.AppendTracker(tracker)
	}
	m.access.Unlock()
}

func (m *Manager) Acquire() (Runtime, adapter.GenerationLease, error) {
	m.access.Lock()
	state := m.current
	if state == nil || state.retired {
		m.access.Unlock()
		return Runtime{}, nil, ErrNoGeneration
	}
	state.leases++
	m.access.Unlock()
	return state.runtime, &lease{
		manager: m,
		state:   state,
	}, nil
}

func (m *Manager) CurrentID() uint64 {
	m.access.Lock()
	defer m.access.Unlock()
	if m.current == nil {
		return 0
	}
	return m.current.runtime.ID
}

func (m *Manager) Close() error {
	m.access.Lock()
	if !m.closed {
		m.closed = true
		m.current = nil
		m.clearProviderCallbacksLocked()
		for state := range m.states {
			m.retireLocked(state)
			m.startCloseLocked(state)
		}
	}
	states := make([]*runtimeState, 0, len(m.states))
	for state := range m.states {
		states = append(states, state)
	}
	m.access.Unlock()

	for _, state := range states {
		<-state.done
	}
	m.access.Lock()
	result := m.closeErr
	m.access.Unlock()
	return result
}

func (m *Manager) registerProviderCallback(tag string, callback adapter.ProviderUpdateCallback) *list.Element[adapter.ProviderUpdateCallback] {
	if callback == nil {
		return nil
	}
	m.access.Lock()
	defer m.access.Unlock()
	if m.closed {
		return nil
	}
	publicHandle := m.providerCallbacks.PushBack(callback)
	registration := &providerCallbackRegistration{tag: tag, callback: callback}
	m.providerCallbackBy[publicHandle] = registration
	if m.current != nil && !m.current.retired {
		if provider, loaded := m.current.runtime.Provider.Get(tag); loaded && provider != nil {
			registration.provider = provider
			registration.handle = provider.RegisterCallback(callback)
		}
	}
	return publicHandle
}

func (m *Manager) unregisterProviderCallback(publicHandle *list.Element[adapter.ProviderUpdateCallback]) {
	if publicHandle == nil {
		return
	}
	m.access.Lock()
	defer m.access.Unlock()
	registration, loaded := m.providerCallbackBy[publicHandle]
	if !loaded {
		return
	}
	if registration.provider != nil && registration.handle != nil {
		registration.provider.UnregisterCallback(registration.handle)
	}
	delete(m.providerCallbackBy, publicHandle)
	m.providerCallbacks.Remove(publicHandle)
}

func (m *Manager) rebindProviderCallbacksLocked(providerManager adapter.ProviderManager) {
	for _, registration := range m.providerCallbackBy {
		var nextProvider adapter.Provider
		var nextHandle *list.Element[adapter.ProviderUpdateCallback]
		if provider, loaded := providerManager.Get(registration.tag); loaded && provider != nil {
			nextProvider = provider
			nextHandle = provider.RegisterCallback(registration.callback)
		}
		if registration.provider != nil && registration.handle != nil {
			registration.provider.UnregisterCallback(registration.handle)
		}
		registration.provider = nextProvider
		registration.handle = nextHandle
	}
}

func (m *Manager) clearProviderCallbacksLocked() {
	for publicHandle, registration := range m.providerCallbackBy {
		if registration.provider != nil && registration.handle != nil {
			registration.provider.UnregisterCallback(registration.handle)
		}
		m.providerCallbacks.Remove(publicHandle)
		delete(m.providerCallbackBy, publicHandle)
	}
}

func (m *Manager) retireLocked(state *runtimeState) {
	state.retired = true
	if state.retiredNotified {
		return
	}
	state.retiredNotified = true
	if state.published && state.runtime.Retire != nil {
		state.runtime.Retire()
	}
}

func (m *Manager) release(state *runtimeState) {
	m.access.Lock()
	if state.leases <= 0 {
		m.access.Unlock()
		panic("generation lease released more than once")
	}
	state.leases--
	m.startCloseLocked(state)
	m.access.Unlock()
}

func (m *Manager) startCloseLocked(state *runtimeState) {
	if !state.retired || state.leases != 0 || state.closing {
		return
	}
	state.closing = true
	go func() {
		err := state.runtime.Close()
		m.access.Lock()
		state.closeErr = err
		if err != nil {
			m.closeErr = errors.Join(m.closeErr, fmt.Errorf("close generation %d: %w", state.runtime.ID, err))
		}
		delete(m.states, state)
		close(state.done)
		m.access.Unlock()
	}()
}

type lease struct {
	manager  *Manager
	state    *runtimeState
	released atomic.Bool
}

func (l *lease) Release() {
	if l == nil || l.released.Swap(true) {
		return
	}
	l.manager.release(l.state)
}
