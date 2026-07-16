package generation

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-tun"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.Router = (*Router)(nil)
var _ adapter.GenerationLeaser = (*Router)(nil)

type Router struct {
	manager *Manager
}

func NewRouter(manager *Manager) *Router {
	return &Router{manager: manager}
}

func (r *Router) Start(adapter.StartStage) error {
	return nil
}

func (r *Router) Close() error {
	return nil
}

func (r *Router) AcquireGeneration() (adapter.GenerationLease, error) {
	_, lease, err := r.manager.Acquire()
	return lease, err
}

func (r *Router) RouteConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext) error {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Router.RouteConnection(ctx, conn, metadata)
}

func (r *Router) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		return
	}
	runtime.Router.RouteConnectionEx(ctx, conn, metadata, generationCloseHandler(lease, onClose))
}

func (r *Router) RoutePacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext) error {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	return runtime.Router.RoutePacketConnection(ctx, conn, metadata)
}

func (r *Router) RoutePacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		return
	}
	runtime.Router.RoutePacketConnectionEx(ctx, conn, metadata, generationCloseHandler(lease, onClose))
}

func generationCloseHandler(lease adapter.GenerationLease, onClose N.CloseHandlerFunc) N.CloseHandlerFunc {
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			if onClose != nil {
				onClose(err)
			}
			lease.Release()
		})
	}
}

const generationFlowLeaseAbandonTimeout = 2 * time.Second

func (r *Router) PreMatch(metadata adapter.InboundContext, firstPacket []byte) adapter.PreMatchResult {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return adapter.PreMatchResult{Action: adapter.PreMatchContinue}
	}
	result := runtime.Router.PreMatch(metadata, firstPacket)
	if result.Action != adapter.PreMatchFlow {
		lease.Release()
		return result
	}
	if _, isPort := result.Outbound.(tun.Port); !isPort {
		lease.Release()
		return result
	}
	factory := newGenerationFlowTrackerFactory(lease, result.NewTracker)
	result.NewTracker = factory.New
	return result
}

func (r *Router) RuleSets() []adapter.RuleSet {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return runtime.Router.RuleSets()
}

func (r *Router) RuleSet(tag string) (adapter.RuleSet, bool) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, false
	}
	defer lease.Release()
	return runtime.Router.RuleSet(tag)
}

func (r *Router) Rules() []adapter.Rule {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return runtime.Router.Rules()
}

func (r *Router) NeedFindProcess() bool {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	return runtime.Router.NeedFindProcess()
}

func (r *Router) NeedFindNeighbor() bool {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return false
	}
	defer lease.Release()
	return runtime.Router.NeedFindNeighbor()
}

func (r *Router) NeighborResolver() adapter.NeighborResolver {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	return runtime.Router.NeighborResolver()
}

func (r *Router) Rule(uuid string) (adapter.Rule, bool) {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return nil, false
	}
	defer lease.Release()
	return runtime.Router.Rule(uuid)
}

func (r *Router) AppendTracker(tracker adapter.ConnectionTracker) {
	r.manager.AppendTracker(tracker)
}

func (r *Router) ResetNetwork() {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	runtime.Router.ResetNetwork()
}

func (r *Router) DefaultDomainMatchStrategy() C.DomainMatchStrategy {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return C.DomainMatchStrategyAsIS
	}
	defer lease.Release()
	return runtime.Router.DefaultDomainMatchStrategy()
}

func (r *Router) Reload() {
	runtime, lease, err := r.manager.Acquire()
	if err != nil {
		return
	}
	defer lease.Release()
	runtime.Router.Reload()
}

type generationFlowTrackerFactory struct {
	lease    adapter.GenerationLease
	upstream func() tun.FlowTracker
	state    uint32
	timer    *time.Timer
}

func newGenerationFlowTrackerFactory(lease adapter.GenerationLease, upstream func() tun.FlowTracker) *generationFlowTrackerFactory {
	return newGenerationFlowTrackerFactoryWithTimeout(lease, upstream, generationFlowLeaseAbandonTimeout)
}

func newGenerationFlowTrackerFactoryWithTimeout(lease adapter.GenerationLease, upstream func() tun.FlowTracker, timeout time.Duration) *generationFlowTrackerFactory {
	factory := &generationFlowTrackerFactory{
		lease:    lease,
		upstream: upstream,
	}
	factory.timer = time.AfterFunc(timeout, factory.abandon)
	return factory
}

func (f *generationFlowTrackerFactory) New() tun.FlowTracker {
	if !atomic.CompareAndSwapUint32(&f.state, 0, 1) {
		return nil
	}
	f.timer.Stop()
	var upstream tun.FlowTracker
	if f.upstream != nil {
		upstream = f.upstream()
	}
	return &generationFlowTracker{
		FlowTracker: upstream,
		lease:       f.lease,
	}
}

func (f *generationFlowTrackerFactory) abandon() {
	if atomic.CompareAndSwapUint32(&f.state, 0, 2) {
		f.lease.Release()
	}
}

type generationFlowTracker struct {
	tun.FlowTracker
	lease adapter.GenerationLease
	once  sync.Once
}

func (t *generationFlowTracker) AttachFlow(handle tun.FlowHandle) {
	if t.FlowTracker != nil {
		t.FlowTracker.AttachFlow(handle)
	}
}

func (t *generationFlowTracker) CountForward(n int) {
	if t.FlowTracker != nil {
		t.FlowTracker.CountForward(n)
	}
}

func (t *generationFlowTracker) CountReverse(n int) {
	if t.FlowTracker != nil {
		t.FlowTracker.CountReverse(n)
	}
}

func (t *generationFlowTracker) FlowEstablished() {
	if t.FlowTracker != nil {
		t.FlowTracker.FlowEstablished()
	}
}

func (t *generationFlowTracker) CloseFlow(reason tun.FlowCloseReason) {
	defer t.once.Do(t.lease.Release)
	if t.FlowTracker != nil {
		t.FlowTracker.CloseFlow(reason)
	}
}
