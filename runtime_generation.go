package box

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/generation"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/adapter/provider"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/service"
)

type runtimeGeneration struct {
	ctx          context.Context
	cancel       context.CancelFunc
	logger       log.ContextLogger
	outbound     *outbound.Manager
	provider     *provider.Manager
	dnsTransport *dns.TransportManager
	dnsRouter    *dns.Router
	router       *route.Router
	endpoint     *runtimeEndpointManager
	network      *runtimeNetworkManager
	closeOnce    sync.Once
	closeErr     error
}

type runtimeNetworkManager struct {
	adapter.NetworkManager
	stableInitialization bool
	requiresRestart      atomic.Bool
}

func (m *runtimeNetworkManager) Initialize(ruleSets []adapter.RuleSet) {
	if m.stableInitialization {
		m.NetworkManager.Initialize(ruleSets)
		return
	}
	if m.NetworkManager.NeedWIFIState() {
		return
	}
	for _, ruleSet := range ruleSets {
		if ruleSet.Metadata().ContainsWIFIRule {
			m.requiresRestart.Store(true)
			return
		}
	}
}

func (m *runtimeNetworkManager) validationError() error {
	if !m.requiresRestart.Load() {
		return nil
	}
	return restartRequired("route.rule_set", "candidate rule-set requires Wi-Fi state monitoring that is not active in the stable network manager")
}

func (s *Box) buildRuntime(options option.Options, stableEndpointsStarted bool) (*runtimeGeneration, error) {
	ctx := service.ExtendContext(s.ctx)
	ctx, cancel := context.WithCancel(ctx)
	routeOptions := common.PtrValueOrDefault(options.Route)
	dnsOptions := common.PtrValueOrDefault(options.DNS)
	if stableEndpointsStarted && route.RequiresWIFIState(routeOptions, dnsOptions) && !s.network.NeedWIFIState() {
		cancel()
		return nil, restartRequired("route.rules", "candidate rules require Wi-Fi state monitoring that is not active in the stable network manager")
	}
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	providerRegistry := service.FromContext[adapter.ProviderRegistry](ctx)
	dnsTransportRegistry := service.FromContext[adapter.DNSTransportRegistry](ctx)
	privateEndpointManager := endpoint.NewManager(s.logFactory.NewLogger("runtime-endpoint"), service.FromContext[adapter.EndpointRegistry](ctx))
	runtimeEndpointManager := newRuntimeEndpointManager(s.endpoint, privateEndpointManager)

	networkManager := &runtimeNetworkManager{
		NetworkManager:       s.network,
		stableInitialization: !stableEndpointsStarted,
	}
	service.MustRegister[adapter.NetworkManager](ctx, networkManager)
	service.MustRegister[adapter.EndpointManager](ctx, runtimeEndpointManager)
	outboundManager := outbound.NewManager(s.logFactory.NewLogger("outbound"), outboundRegistry, runtimeEndpointManager, routeOptions.Final)
	if stableEndpointsStarted {
		outboundManager.UseStartedEndpoints(runtimeEndpointManager.stableEndpoints())
	}
	providerManager := provider.NewManager(ctx, s.logFactory.NewLogger("provider"), providerRegistry)
	dnsTransportManager := dns.NewTransportManager(s.logFactory.NewLogger("dns/transport"), dnsTransportRegistry, outboundManager, dnsOptions.Final)
	service.MustRegister[adapter.OutboundManager](ctx, outboundManager)
	service.MustRegister[adapter.ProviderManager](ctx, providerManager)
	service.MustRegister[adapter.DNSTransportManager](ctx, dnsTransportManager)

	dnsRouter, err := dns.NewRouter(ctx, s.logFactory, dnsOptions)
	if err != nil {
		cancel()
		return nil, E.Cause(err, "initialize DNS router")
	}
	service.MustRegister[adapter.DNSRouter](ctx, dnsRouter)
	service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, dnsRouter)
	router := route.NewRouter(ctx, s.logFactory, routeOptions, dnsOptions, s.reloadChan)
	service.MustRegister[adapter.Router](ctx, router)

	runtime := &runtimeGeneration{
		ctx:          ctx,
		cancel:       cancel,
		logger:       s.logger,
		outbound:     outboundManager,
		provider:     providerManager,
		dnsTransport: dnsTransportManager,
		dnsRouter:    dnsRouter,
		router:       router,
		endpoint:     runtimeEndpointManager,
		network:      networkManager,
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = runtime.Close()
		}
	}()

	if err = router.Initialize(routeOptions.Rules, routeOptions.RuleSet); err != nil {
		return nil, E.Cause(err, "initialize router")
	}
	for i, transportOptions := range dnsOptions.Servers {
		tag := transportOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		if err = dnsTransportManager.Create(
			ctx,
			s.logFactory.NewLogger(F.ToString("dns/", transportOptions.Type, "[", tag, "]")),
			tag,
			transportOptions.Type,
			transportOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize DNS server[", i, "]")
		}
	}
	if err = dnsRouter.Initialize(dnsOptions.Rules); err != nil {
		return nil, E.Cause(err, "initialize DNS router")
	}

	outboundOptions := append([]option.Outbound(nil), options.Outbounds...)
	outboundOptions = append(outboundOptions, option.Outbound{Tag: "Compatible", Type: C.TypeDirect})
	for i, outboundOptions := range outboundOptions {
		tag := outboundOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		outboundCtx := ctx
		if tag != "" {
			outboundCtx = adapter.WithContext(outboundCtx, &adapter.InboundContext{Outbound: tag})
		}
		if err = outboundManager.Create(
			outboundCtx,
			router,
			s.logFactory.NewLogger(F.ToString("outbound/", outboundOptions.Type, "[", tag, "]")),
			tag,
			outboundOptions.Type,
			outboundOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize outbound[", i, "]")
		}
	}
	for i, providerOptions := range options.Providers {
		tag := providerOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		if err = providerManager.Create(
			ctx,
			router,
			s.logFactory,
			tag,
			providerOptions.Type,
			providerOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize provider[", i, "]")
		}
	}
	outboundManager.Initialize(func() (adapter.Outbound, error) {
		return direct.NewOutbound(
			ctx,
			router,
			s.logFactory.NewLogger("outbound/direct"),
			"direct",
			option.DirectOutboundOptions{},
		)
	})
	dnsTransportManager.Initialize(func() (adapter.DNSTransport, error) {
		return dnsTransportRegistry.CreateDNSTransport(
			ctx,
			s.logFactory.NewLogger("dns/local"),
			"local",
			C.DNSTypeLocal,
			&option.LocalDNSServerOptions{},
		)
	})
	cleanup = false
	return runtime, nil
}

func (r *runtimeGeneration) Start(stage adapter.StartStage) error {
	var err error
	switch stage {
	case adapter.StartStateInitialize:
		err = adapter.Start(r.logger, stage, r.dnsTransport, r.dnsRouter, r.router, r.outbound, r.provider, r.endpoint)
	case adapter.StartStateStart:
		err = adapter.Start(r.logger, stage, r.outbound, r.dnsTransport, r.provider, r.router, r.dnsRouter, r.endpoint)
		if err == nil {
			err = r.network.validationError()
		}
	case adapter.StartStatePostStart:
		err = adapter.Start(r.logger, stage, r.outbound, r.provider, r.dnsTransport, r.dnsRouter, r.router, r.endpoint)
	case adapter.StartStateStarted:
		err = adapter.Start(r.logger, stage, r.dnsTransport, r.dnsRouter, r.router, r.outbound, r.provider, r.endpoint)
	default:
		panic("unknown runtime start stage")
	}
	return err
}

func (r *runtimeGeneration) snapshot() generation.Runtime {
	return generation.Runtime{
		Router:       r.router,
		DNSRouter:    r.dnsRouter,
		DNSTransport: r.dnsTransport,
		Outbound:     r.outbound,
		Provider:     r.provider,
		Endpoint:     r.endpoint.local,
		Publish:      r.publish,
		Retire:       r.retire,
		Close:        r.Close,
	}
}

func (r *runtimeGeneration) publish() {
	for _, candidate := range r.outbound.Outbounds() {
		if lifecycle, loaded := candidate.(adapter.GenerationLifecycle); loaded {
			lifecycle.OnGenerationPublish()
		}
	}
}

func (r *runtimeGeneration) retire() {
	for _, candidate := range r.outbound.Outbounds() {
		if lifecycle, loaded := candidate.(adapter.GenerationLifecycle); loaded {
			lifecycle.OnGenerationRetire()
		}
	}
}

func (r *runtimeGeneration) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		for _, closeItem := range []struct {
			name    string
			service adapter.Lifecycle
		}{
			{"provider", r.provider},
			{"outbound", r.outbound},
			{"endpoint", r.endpoint},
			{"router", r.router},
			{"dns-router", r.dnsRouter},
			{"dns-transport", r.dnsTransport},
		} {
			r.closeErr = E.Append(r.closeErr, closeItem.service.Close(), func(err error) error {
				return E.Cause(err, "close ", closeItem.name)
			})
		}
	})
	return r.closeErr
}
