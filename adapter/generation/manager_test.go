package generation

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	tun "github.com/sagernet/sing-tun"
	N "github.com/sagernet/sing/common/network"
)

type testRouter struct {
	adapter.Router
	access        sync.Mutex
	onClose       N.CloseHandlerFunc
	packetOnClose N.CloseHandlerFunc
	preMatch      adapter.PreMatchResult
}

func (r *testRouter) RouteConnectionEx(_ context.Context, _ net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.access.Lock()
	r.onClose = onClose
	r.access.Unlock()
}

func (r *testRouter) closeFlow() {
	r.access.Lock()
	onClose := r.onClose
	r.access.Unlock()
	if onClose != nil {
		onClose(nil)
	}
}

func (r *testRouter) RoutePacketConnectionEx(_ context.Context, _ N.PacketConn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.access.Lock()
	r.packetOnClose = onClose
	r.access.Unlock()
}

func (r *testRouter) closePacketFlow() {
	r.access.Lock()
	onClose := r.packetOnClose
	r.access.Unlock()
	if onClose != nil {
		onClose(nil)
	}
}

func (r *testRouter) PreMatch(adapter.InboundContext, []byte) adapter.PreMatchResult {
	return r.preMatch
}

type testFlowOutbound struct {
	adapter.Outbound
}

func (*testFlowOutbound) PortAddresses() (netip.Addr, netip.Addr) { return netip.Addr{}, netip.Addr{} }
func (*testFlowOutbound) PortMTU() uint32                         { return 1500 }
func (*testFlowOutbound) AttachReturn(tun.Return) error           { return nil }
func (*testFlowOutbound) DetachReturn(tun.Return) error           { return nil }
func (*testFlowOutbound) WritePackets([][]byte) error             { return nil }

type testDNSRouter struct {
	adapter.DNSRouter
}

type testDNSTransportManager struct {
	adapter.DNSTransportManager
}

type testOutboundManager struct {
	adapter.OutboundManager
}

type testProviderManager struct {
	adapter.ProviderManager
}

type testEndpointManager struct {
	adapter.EndpointManager
}

func (*testEndpointManager) Endpoints() []adapter.Endpoint { return nil }

func testRuntime(router adapter.Router, closeCount *atomic.Int32, closed chan struct{}) Runtime {
	return Runtime{
		Router:       router,
		DNSRouter:    &testDNSRouter{},
		DNSTransport: &testDNSTransportManager{},
		Outbound:     &testOutboundManager{},
		Provider:     &testProviderManager{},
		Endpoint:     &testEndpointManager{},
		Close: func() error {
			if closeCount.Add(1) == 1 && closed != nil {
				close(closed)
			}
			return nil
		},
	}
}

func TestRetiredGenerationWaitsForLease(t *testing.T) {
	manager := NewManager()
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(&testRouter{}, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	var secondCloseCount atomic.Int32
	if _, err := manager.Publish(testRuntime(&testRouter{}, &secondCloseCount, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
		t.Fatal("retired generation closed with an active lease")
	default:
	}
	lease.Release()
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("retired generation did not close after its final lease")
	}
	lease.Release()
	if firstCloseCount.Load() != 1 {
		t.Fatalf("generation closed %d times", firstCloseCount.Load())
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if secondCloseCount.Load() != 1 {
		t.Fatalf("current generation closed %d times", secondCloseCount.Load())
	}
}

func TestRetiredGenerationCloseErrorRemainsObservable(t *testing.T) {
	manager := NewManager()
	expected := errors.New("retired close failed")
	firstClosed := make(chan struct{})
	first := testRuntime(&testRouter{}, new(atomic.Int32), nil)
	first.Close = func() error {
		close(firstClosed)
		return expected
	}
	if _, err := manager.Publish(first); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Publish(testRuntime(&testRouter{}, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("retired generation did not close")
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("retired generation close error was lost: %v", err)
	}
}

func TestPreparedInitialGenerationActivatesAfterConstruction(t *testing.T) {
	manager := NewManager()
	var published atomic.Int32
	var retired atomic.Int32
	runtime := testRuntime(&testRouter{}, new(atomic.Int32), nil)
	runtime.Publish = func() { published.Add(1) }
	runtime.Retire = func() { retired.Add(1) }
	preparedID, err := manager.PrepareInitial(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if preparedID == 0 || manager.CurrentID() != preparedID {
		t.Fatalf("prepared generation is not queryable: prepared=%d current=%d", preparedID, manager.CurrentID())
	}
	if published.Load() != 0 {
		t.Fatal("prepared generation published before lifecycle start completed")
	}
	activatedID, err := manager.ActivateInitial()
	if err != nil {
		t.Fatal(err)
	}
	if activatedID != preparedID || published.Load() != 1 {
		t.Fatalf("initial generation activation mismatch: prepared=%d activated=%d published=%d", preparedID, activatedID, published.Load())
	}
	if _, err = manager.ActivateInitial(); err != nil {
		t.Fatal(err)
	}
	if published.Load() != 1 {
		t.Fatal("initial generation published more than once")
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if retired.Load() != 1 {
		t.Fatalf("activated generation retired %d times", retired.Load())
	}
}

func TestRouterFlowLeaseTracksOnClose(t *testing.T) {
	manager := NewManager()
	firstRouter := &testRouter{}
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(firstRouter, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	switchRouter := NewRouter(manager)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	switchRouter.RouteConnectionEx(context.Background(), server, adapter.InboundContext{}, nil)

	var secondCloseCount atomic.Int32
	if _, err := manager.Publish(testRuntime(&testRouter{}, &secondCloseCount, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
		t.Fatal("flow generation closed before onClose")
	default:
	}
	firstRouter.closeFlow()
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("flow generation did not retire after onClose")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseWaitsForOutstandingLease(t *testing.T) {
	manager := NewManager()
	var closeCount atomic.Int32
	closed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(&testRouter{}, &closeCount, closed)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- manager.Close()
	}()
	select {
	case <-closeDone:
		t.Fatal("manager close returned with an outstanding lease")
	case <-time.After(20 * time.Millisecond):
	}
	lease.Release()
	select {
	case err = <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager close did not finish after lease release")
	}
}

func TestRouterPacketFlowLeaseTracksOnClose(t *testing.T) {
	manager := NewManager()
	firstRouter := &testRouter{}
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(firstRouter, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	switchRouter := NewRouter(manager)
	switchRouter.RoutePacketConnectionEx(context.Background(), nil, adapter.InboundContext{}, nil)

	var secondCloseCount atomic.Int32
	if _, err := manager.Publish(testRuntime(&testRouter{}, &secondCloseCount, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
		t.Fatal("packet generation closed before onClose")
	default:
	}
	firstRouter.closePacketFlow()
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("packet generation did not retire after onClose")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestL3FlowLeaseTracksTrackerClose(t *testing.T) {
	manager := NewManager()
	firstRouter := &testRouter{preMatch: adapter.PreMatchResult{
		Action:   adapter.PreMatchFlow,
		Outbound: &testFlowOutbound{},
	}}
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(firstRouter, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	switchRouter := NewRouter(manager)
	result := switchRouter.PreMatch(adapter.InboundContext{}, nil)
	if result.Action != adapter.PreMatchFlow || result.NewTracker == nil {
		t.Fatalf("unexpected pre-match result: %+v", result)
	}

	var secondCloseCount atomic.Int32
	if _, err := manager.Publish(testRuntime(&testRouter{}, &secondCloseCount, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
		t.Fatal("L3 generation closed before tracker creation")
	default:
	}
	tracker := result.NewTracker()
	if tracker == nil {
		t.Fatal("generation tracker factory returned nil")
	}
	tracker.CloseFlow(tun.FlowCloseFinished)
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("L3 generation did not retire after tracker close")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestL3FlowFactoryAbandonReleasesLease(t *testing.T) {
	manager := NewManager()
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(&testRouter{}, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	factory := newGenerationFlowTrackerFactory(lease, nil)
	if _, err = manager.Publish(testRuntime(&testRouter{}, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	factory.timer.Stop()
	factory.abandon()
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("abandoned L3 verdict leaked its generation lease")
	}
	if tracker := factory.New(); tracker != nil {
		t.Fatal("abandoned tracker factory became usable")
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestL3FlowFactoryTimeoutReleasesLease(t *testing.T) {
	manager := NewManager()
	var firstCloseCount atomic.Int32
	firstClosed := make(chan struct{})
	if _, err := manager.Publish(testRuntime(&testRouter{}, &firstCloseCount, firstClosed)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	factory := newGenerationFlowTrackerFactoryWithTimeout(lease, nil, 20*time.Millisecond)
	if _, err = manager.Publish(testRuntime(&testRouter{}, new(atomic.Int32), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstClosed:
	case <-time.After(time.Second):
		t.Fatal("unused L3 tracker factory did not release its generation lease")
	}
	if tracker := factory.New(); tracker != nil {
		t.Fatal("timed-out L3 tracker factory became usable")
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerHundredThousandAcquireRelease(t *testing.T) {
	manager := NewManager()
	var closeCount atomic.Int32
	if _, err := manager.Publish(testRuntime(&testRouter{}, &closeCount, nil)); err != nil {
		t.Fatal(err)
	}
	for range 100000 {
		_, lease, err := manager.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		lease.Release()
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if closeCount.Load() != 1 {
		t.Fatalf("runtime closed %d times", closeCount.Load())
	}
	assertManagerDrained(t, manager)
}

func TestManagerThousandPublishesDrainAllStates(t *testing.T) {
	manager := NewManager()
	const publishCount = 1000
	var closeCount atomic.Int64
	for range publishCount {
		if _, err := manager.Publish(stressRuntime(&closeCount)); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if closeCount.Load() != publishCount {
		t.Fatalf("expected %d closes, got %d", publishCount, closeCount.Load())
	}
	assertManagerDrained(t, manager)
}

func TestManagerConcurrentAcquireAndPublish(t *testing.T) {
	manager := NewManager()
	var closeCount atomic.Int64
	if _, err := manager.Publish(stressRuntime(&closeCount)); err != nil {
		t.Fatal(err)
	}
	var acquireErrors atomic.Int64
	var waitGroup sync.WaitGroup
	for range 12 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for range 5000 {
				_, lease, err := manager.Acquire()
				if err != nil {
					acquireErrors.Add(1)
					continue
				}
				lease.Release()
			}
		}()
	}
	for range 250 {
		if _, err := manager.Publish(stressRuntime(&closeCount)); err != nil {
			t.Fatal(err)
		}
	}
	waitGroup.Wait()
	if acquireErrors.Load() != 0 {
		t.Fatalf("concurrent acquire failed %d times", acquireErrors.Load())
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if closeCount.Load() != 251 {
		t.Fatalf("expected 251 closes, got %d", closeCount.Load())
	}
	assertManagerDrained(t, manager)
}

func TestManagerPublishesAndRetiresRuntimeLifecycle(t *testing.T) {
	manager := NewManager()
	var firstCloseCount atomic.Int32
	var secondCloseCount atomic.Int32
	var firstPublished atomic.Int32
	var firstRetired atomic.Int32
	var secondPublished atomic.Int32
	first := testRuntime(&testRouter{}, &firstCloseCount, nil)
	first.Publish = func() {
		firstPublished.Add(1)
	}
	first.Retire = func() {
		firstRetired.Add(1)
	}
	second := testRuntime(&testRouter{}, &secondCloseCount, nil)
	second.Publish = func() {
		secondPublished.Add(1)
	}
	if _, err := manager.Publish(first); err != nil {
		t.Fatal(err)
	}
	_, lease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Publish(second); err != nil {
		t.Fatal(err)
	}
	if firstPublished.Load() != 1 || firstRetired.Load() != 1 || secondPublished.Load() != 1 {
		t.Fatalf(
			"unexpected lifecycle counts: first publish=%d retire=%d second publish=%d",
			firstPublished.Load(),
			firstRetired.Load(),
			secondPublished.Load(),
		)
	}
	lease.Release()
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if firstRetired.Load() != 1 {
		t.Fatalf("first runtime retired %d times", firstRetired.Load())
	}
}

func stressRuntime(closeCount *atomic.Int64) Runtime {
	return Runtime{
		Router:       &testRouter{},
		DNSRouter:    &testDNSRouter{},
		DNSTransport: &testDNSTransportManager{},
		Outbound:     &testOutboundManager{},
		Provider:     &testProviderManager{},
		Endpoint:     &testEndpointManager{},
		Close: func() error {
			closeCount.Add(1)
			return nil
		},
	}
}

func assertManagerDrained(t *testing.T, manager *Manager) {
	t.Helper()
	manager.access.Lock()
	defer manager.access.Unlock()
	if len(manager.states) != 0 {
		t.Fatalf("generation states not drained: %d", len(manager.states))
	}
	if manager.current != nil {
		t.Fatal("current generation remains after close")
	}
}
