package box

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxCertificate "github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/generation"
	"github.com/sagernet/sing-box/adapter/inbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/common/certificate"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/httpclient"
	"github.com/sagernet/sing-box/common/netns"
	"github.com/sagernet/sing-box/common/taskmonitor"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/experimental"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/experimental/deprecated"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

var _ adapter.SimpleLifecycle = (*Box)(nil)

type Box struct {
	ctx                 context.Context
	options             option.Options
	createdAt           time.Time
	logFactory          log.Factory
	logger              log.ContextLogger
	network             *route.NetworkManager
	endpoint            *endpoint.Manager
	endpointView        *generation.EndpointManager
	inbound             *inbound.Manager
	outbound            *generation.OutboundManager
	provider            *generation.ProviderManager
	service             *boxService.Manager
	certificateProvider *boxCertificate.Manager
	dnsTransport        *generation.DNSTransportManager
	dnsRouter           *generation.DNSRouter
	connection          *route.ConnectionManager
	router              *generation.Router
	httpClientService   adapter.LifecycleService
	internalService     []adapter.LifecycleService
	reloadChan          chan struct{}
	done                chan struct{}
	generation          *generation.Manager
	initialRuntime      *runtimeGeneration
	reloadAccess        sync.Mutex
	started             bool
}

type Options struct {
	option.Options
	Context                    context.Context
	PlatformLogWriter          log.PlatformWriter
	NetworkNamespaceHolderArgs []string
}

func Context(
	ctx context.Context,
	inboundRegistry adapter.InboundRegistry,
	providerRegistry adapter.ProviderRegistry,
	outboundRegistry adapter.OutboundRegistry,
	endpointRegistry adapter.EndpointRegistry,
	dnsTransportRegistry adapter.DNSTransportRegistry,
	serviceRegistry adapter.ServiceRegistry,
	certificateProviderRegistry adapter.CertificateProviderRegistry,
) context.Context {
	if service.FromContext[option.InboundOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.InboundRegistry](ctx) == nil {
		ctx = service.ContextWith[option.InboundOptionsRegistry](ctx, inboundRegistry)
		ctx = service.ContextWith[adapter.InboundRegistry](ctx, inboundRegistry)
	}
	if service.FromContext[option.ProviderOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.ProviderRegistry](ctx) == nil {
		ctx = service.ContextWith[option.ProviderOptionsRegistry](ctx, providerRegistry)
		ctx = service.ContextWith[adapter.ProviderRegistry](ctx, providerRegistry)
	}
	if service.FromContext[option.OutboundOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.OutboundRegistry](ctx) == nil {
		ctx = service.ContextWith[option.OutboundOptionsRegistry](ctx, outboundRegistry)
		ctx = service.ContextWith[adapter.OutboundRegistry](ctx, outboundRegistry)
	}
	if service.FromContext[option.EndpointOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.EndpointRegistry](ctx) == nil {
		ctx = service.ContextWith[option.EndpointOptionsRegistry](ctx, endpointRegistry)
		ctx = service.ContextWith[adapter.EndpointRegistry](ctx, endpointRegistry)
	}
	if service.FromContext[adapter.DNSTransportRegistry](ctx) == nil {
		ctx = service.ContextWith[option.DNSTransportOptionsRegistry](ctx, dnsTransportRegistry)
		ctx = service.ContextWith[adapter.DNSTransportRegistry](ctx, dnsTransportRegistry)
	}
	if service.FromContext[adapter.ServiceRegistry](ctx) == nil {
		ctx = service.ContextWith[option.ServiceOptionsRegistry](ctx, serviceRegistry)
		ctx = service.ContextWith[adapter.ServiceRegistry](ctx, serviceRegistry)
	}
	if service.FromContext[adapter.CertificateProviderRegistry](ctx) == nil {
		ctx = service.ContextWith[option.CertificateProviderOptionsRegistry](ctx, certificateProviderRegistry)
		ctx = service.ContextWith[adapter.CertificateProviderRegistry](ctx, certificateProviderRegistry)
	}
	return ctx
}

func New(options Options) (_ *Box, err error) {
	createdAt := time.Now()
	reloadChan := make(chan struct{}, 1)
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = service.ContextWithDefaultRegistry(ctx)
	ctx = pause.WithDefaultManager(ctx)

	endpointRegistry := service.FromContext[adapter.EndpointRegistry](ctx)
	inboundRegistry := service.FromContext[adapter.InboundRegistry](ctx)
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	providerRegistry := service.FromContext[adapter.ProviderRegistry](ctx)
	dnsTransportRegistry := service.FromContext[adapter.DNSTransportRegistry](ctx)
	serviceRegistry := service.FromContext[adapter.ServiceRegistry](ctx)
	certificateProviderRegistry := service.FromContext[adapter.CertificateProviderRegistry](ctx)
	switch {
	case endpointRegistry == nil:
		return nil, E.New("missing endpoint registry in context")
	case inboundRegistry == nil:
		return nil, E.New("missing inbound registry in context")
	case outboundRegistry == nil:
		return nil, E.New("missing outbound registry in context")
	case providerRegistry == nil:
		return nil, E.New("missing provider registry in context")
	case dnsTransportRegistry == nil:
		return nil, E.New("missing DNS transport registry in context")
	case serviceRegistry == nil:
		return nil, E.New("missing service registry in context")
	case certificateProviderRegistry == nil:
		return nil, E.New("missing certificate provider registry in context")
	}

	experimentalOptions := common.PtrValueOrDefault(options.Experimental)
	if err = applyDebugOptions(common.PtrValueOrDefault(experimentalOptions.Debug)); err != nil {
		return nil, err
	}
	needCacheFile := experimentalOptions.CacheFile != nil && experimentalOptions.CacheFile.Enabled || options.PlatformLogWriter != nil
	needClashAPI := experimentalOptions.ClashAPI != nil || options.PlatformLogWriter != nil
	needV2RayAPI := experimentalOptions.V2RayAPI != nil && experimentalOptions.V2RayAPI.Listen != ""
	needAPIService := common.Any(options.Services, func(it option.Service) bool { return it.Type == C.TypeAPI })
	if service.PtrFromContext[urltest.HistoryStorage](ctx) == nil {
		ctx = service.ContextWithPtr(ctx, urltest.NewHistoryStorage())
	}
	platformInterface := service.FromContext[adapter.PlatformInterface](ctx)
	var defaultLogWriter io.Writer
	if platformInterface != nil {
		defaultLogWriter = io.Discard
	}
	logFactory, err := log.New(log.Options{
		Context:        ctx,
		Options:        common.PtrValueOrDefault(options.Log),
		Observable:     needClashAPI || needAPIService,
		DefaultWriter:  defaultLogWriter,
		BaseTime:       createdAt,
		PlatformWriter: options.PlatformLogWriter,
	})
	if err != nil {
		return nil, E.Cause(err, "create log factory")
	}
	service.MustRegister[log.Factory](ctx, logFactory)
	C.URLTestUnifiedDelay = experimentalOptions.URLTestUnifiedDelay

	var internalServices []adapter.LifecycleService
	certificateOptions := common.PtrValueOrDefault(options.Certificate)
	if C.IsAndroid || certificateOptions.Store != "" && certificateOptions.Store != C.CertificateStoreSystem ||
		len(certificateOptions.Certificate) > 0 ||
		len(certificateOptions.CertificatePath) > 0 ||
		len(certificateOptions.CertificateDirectoryPath) > 0 {
		certificateStore, certificateErr := certificate.NewStore(logFactory.NewLogger("certificate"), certificateOptions)
		if certificateErr != nil {
			return nil, certificateErr
		}
		service.MustRegister[adapter.CertificateStore](ctx, certificateStore)
		internalServices = append(internalServices, certificateStore)
	}
	netnsManager, err := netns.NewManager(logFactory.NewLogger("netns"), options.NetworkNamespaces, options.NetworkNamespaceHolderArgs)
	if err != nil {
		return nil, err
	}
	service.MustRegister[adapter.NetworkNamespaceManager](ctx, netnsManager)
	internalServices = append(internalServices, netnsManager)

	routeOptions := common.PtrValueOrDefault(options.Route)
	dnsOptions := common.PtrValueOrDefault(options.DNS)
	generationManager := generation.NewManager()
	endpointManager := endpoint.NewManager(logFactory.NewLogger("endpoint"), endpointRegistry)
	generationEndpoint := generation.NewEndpointManager(generationManager, endpointManager)
	inboundManager := inbound.NewManager(logFactory.NewLogger("inbound"), inboundRegistry, generationEndpoint)
	serviceManager := boxService.NewManager(logFactory.NewLogger("service"), serviceRegistry)
	certificateProviderManager := boxCertificate.NewManager(logFactory.NewLogger("certificate-provider"), certificateProviderRegistry)
	connectionManager := route.NewConnectionManager(logFactory.NewLogger("connection"))
	generationRouter := generation.NewRouter(generationManager)
	generationDNSRouter := generation.NewDNSRouter(generationManager)
	generationDNS := generation.NewDNSTransportManager(generationManager)
	generationOutbound := generation.NewOutboundManager(generationManager)
	generationProvider := generation.NewProviderManager(generationManager)
	service.MustRegister[adapter.EndpointManager](ctx, generationEndpoint)
	service.MustRegister[adapter.InboundManager](ctx, inboundManager)
	service.MustRegister[adapter.OutboundManager](ctx, generationOutbound)
	service.MustRegister[adapter.ProviderManager](ctx, generationProvider)
	service.MustRegister[adapter.DNSTransportManager](ctx, generationDNS)
	service.MustRegister[adapter.DNSRouter](ctx, generationDNSRouter)
	service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, generationDNSRouter)
	service.MustRegister[adapter.Router](ctx, generationRouter)
	service.MustRegister[adapter.ConnectionManager](ctx, connectionManager)
	service.MustRegister[adapter.ServiceManager](ctx, serviceManager)
	service.MustRegister[adapter.CertificateProviderManager](ctx, certificateProviderManager)

	networkManager, err := route.NewNetworkManager(ctx, logFactory.NewLogger("network"), routeOptions, dnsOptions)
	if err != nil {
		return nil, E.Cause(err, "initialize network manager")
	}
	service.MustRegister[adapter.NetworkManager](ctx, networkManager)
	// Must register after ConnectionManager: the Apple HTTP engine's proxy bridge reads it when the default client is resolved.
	httpClientManager := httpclient.NewManager(ctx, logFactory.NewLogger("httpclient"), options.HTTPClients, routeOptions.DefaultHTTPClient)
	service.MustRegister[adapter.HTTPClientManager](ctx, httpClientManager)
	httpClientService := adapter.LifecycleService(httpClientManager)

	if needClashAPI || needAPIService {
		trafficManager := trafficcontrol.NewManager(generationOutbound)
		service.MustRegisterPtr(ctx, trafficManager)
		generationRouter.AppendTracker(trafficManager)
		internalServices = append(internalServices, trafficManager)
	}
	ntpOptions := common.PtrValueOrDefault(options.NTP)
	var timeService *tls.TimeServiceWrapper
	if ntpOptions.Enabled {
		timeService = new(tls.TimeServiceWrapper)
		service.MustRegister[ntp.TimeService](ctx, timeService)
	}
	if needCacheFile {
		cacheFile := cachefile.New(ctx, logFactory.NewLogger("cache-file"), common.PtrValueOrDefault(experimentalOptions.CacheFile))
		service.MustRegister[adapter.CacheFile](ctx, cacheFile)
		internalServices = append(internalServices, cacheFile)
	}

	instance := &Box{
		ctx:                 ctx,
		options:             options.Options,
		createdAt:           createdAt,
		logFactory:          logFactory,
		logger:              logFactory.Logger(),
		network:             networkManager,
		endpoint:            endpointManager,
		endpointView:        generationEndpoint,
		inbound:             inboundManager,
		outbound:            generationOutbound,
		provider:            generationProvider,
		service:             serviceManager,
		certificateProvider: certificateProviderManager,
		dnsTransport:        generationDNS,
		dnsRouter:           generationDNSRouter,
		connection:          connectionManager,
		router:              generationRouter,
		httpClientService:   httpClientService,
		internalService:     internalServices,
		reloadChan:          reloadChan,
		done:                make(chan struct{}),
		generation:          generationManager,
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = instance.Close()
		}
	}()

	if platformInterface != nil {
		if err = platformInterface.Initialize(networkManager); err != nil {
			return nil, E.Cause(err, "initialize platform interface")
		}
	}
	initialRuntime, err := instance.buildRuntime(options.Options, false)
	if err != nil {
		return nil, err
	}
	instance.initialRuntime = initialRuntime
	if _, err = generationManager.PrepareInitial(initialRuntime.snapshot()); err != nil {
		_ = initialRuntime.Close()
		return nil, err
	}

	for i, endpointOptions := range options.Endpoints {
		tag := endpointOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		endpointCtx := ctx
		if tag != "" {
			endpointCtx = adapter.WithContext(endpointCtx, &adapter.InboundContext{Outbound: tag})
		}
		if err = endpointManager.Create(
			endpointCtx,
			generationRouter,
			logFactory.NewLogger(F.ToString("endpoint/", endpointOptions.Type, "[", tag, "]")),
			tag,
			endpointOptions.Type,
			endpointOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize endpoint[", i, "]")
		}
	}
	for i, inboundOptions := range options.Inbounds {
		tag := inboundOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		if err = inboundManager.Create(
			ctx,
			generationRouter,
			logFactory.NewLogger(F.ToString("inbound/", inboundOptions.Type, "[", tag, "]")),
			tag,
			inboundOptions.Type,
			inboundOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize inbound[", i, "]")
		}
	}
	for i, serviceOptions := range options.Services {
		tag := serviceOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		if err = serviceManager.Create(
			ctx,
			logFactory.NewLogger(F.ToString("service/", serviceOptions.Type, "[", tag, "]")),
			tag,
			serviceOptions.Type,
			serviceOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize service[", i, "]")
		}
	}
	for i, certificateProviderOptions := range options.CertificateProviders {
		tag := certificateProviderOptions.Tag
		if tag == "" {
			tag = F.ToString(i)
		}
		if err = certificateProviderManager.Create(
			ctx,
			logFactory.NewLogger(F.ToString("certificate-provider/", certificateProviderOptions.Type, "[", tag, "]")),
			tag,
			certificateProviderOptions.Type,
			certificateProviderOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize certificate provider[", i, "]")
		}
	}

	httpClientManager.Initialize(func() (*httpclient.ManagedTransport, error) {
		deprecated.Report(ctx, deprecated.OptionImplicitDefaultHTTPClient)
		var httpClientOptions option.HTTPClientOptions
		httpClientOptions.DefaultOutbound = true
		return httpclient.NewTransport(ctx, logFactory.NewLogger("httpclient"), "", httpClientOptions)
	})
	if needClashAPI {
		clashAPIOptions := common.PtrValueOrDefault(experimentalOptions.ClashAPI)
		clashAPIOptions.ModeList = experimental.CalculateClashModeList(options.Options)
		clashServer, clashErr := experimental.NewClashServer(ctx, logFactory.(log.ObservableFactory), clashAPIOptions)
		if clashErr != nil {
			return nil, E.Cause(clashErr, "create clash-server")
		}
		service.MustRegister[adapter.ClashServer](ctx, clashServer)
		instance.internalService = append(instance.internalService, clashServer)
	}
	if needV2RayAPI {
		v2rayServer, v2rayErr := experimental.NewV2RayServer(logFactory.NewLogger("v2ray-api"), common.PtrValueOrDefault(experimentalOptions.V2RayAPI))
		if v2rayErr != nil {
			return nil, E.Cause(v2rayErr, "create v2ray-server")
		}
		if v2rayServer.StatsService() != nil {
			generationRouter.AppendTracker(v2rayServer.StatsService())
			instance.internalService = append(instance.internalService, v2rayServer)
			service.MustRegister[adapter.V2RayServer](ctx, v2rayServer)
		}
	}
	if ntpOptions.Enabled {
		ntpDialer, ntpErr := dialer.New(ctx, ntpOptions.DialerOptions, ntpOptions.ServerIsDomain())
		if ntpErr != nil {
			return nil, E.Cause(ntpErr, "create NTP service")
		}
		ntpService := ntp.NewService(ntp.Options{
			Context:       ctx,
			Dialer:        ntpDialer,
			Logger:        logFactory.NewLogger("ntp"),
			Server:        ntpOptions.ServerOptions.Build(),
			Interval:      time.Duration(ntpOptions.Interval),
			WriteToSystem: ntpOptions.WriteToSystem,
		})
		timeService.TimeService = ntpService
		instance.internalService = append(instance.internalService, adapter.NewLifecycleService(ntpService, "ntp service"))
	}
	cleanup = false
	return instance, nil
}

func (s *Box) PreStart() error {
	err := s.preStart()
	if err != nil {
		// TODO: remove catch error
		defer func() {
			v := recover()
			if v != nil {
				println(err.Error())
				debug.PrintStack()
				panic("panic on early close: " + fmt.Sprint(v))
			}
		}()
		s.Close()
		return err
	}
	s.logger.Info("sing-box pre-started (", F.Seconds(time.Since(s.createdAt).Seconds()), "s)")
	return nil
}

func (s *Box) Start() error {
	err := s.start()
	if err != nil {
		// TODO: remove catch error
		defer func() {
			v := recover()
			if v != nil {
				println(err.Error())
				debug.PrintStack()
				println("panic on early start: " + fmt.Sprint(v))
			}
		}()
		s.Close()
		return err
	}
	s.logger.Info("sing-box started (", F.Seconds(time.Since(s.createdAt).Seconds()), "s)")
	return nil
}

func (s *Box) preStart() error {
	if s.initialRuntime == nil {
		return E.New("missing initial runtime generation")
	}
	runtime := s.initialRuntime
	monitor := taskmonitor.New(s.logger, C.StartTimeout)
	monitor.Start("start logger")
	err := s.logFactory.Start()
	monitor.Finish()
	if err != nil {
		return E.Cause(err, "start logger")
	}
	err = adapter.StartNamed(s.logger, adapter.StartStateInitialize, s.internalService) // cache-file clash-api v2ray-api
	if err != nil {
		return err
	}
	err = adapter.Start(
		s.logger,
		adapter.StartStateInitialize,
		s.network,
		runtime.dnsTransport,
		runtime.dnsRouter,
		s.connection,
		runtime.router,
		runtime.outbound,
		runtime.provider,
		runtime.endpoint,
		s.inbound,
		s.endpoint,
		s.service,
		s.certificateProvider,
	)
	if err != nil {
		return err
	}
	err = adapter.Start(s.logger, adapter.StartStateStart, runtime.outbound, runtime.dnsTransport, s.network, s.connection)
	if err != nil {
		return err
	}
	err = adapter.StartNamed(s.logger, adapter.StartStateStart, []adapter.LifecycleService{s.httpClientService})
	if err != nil {
		return err
	}
	err = adapter.Start(s.logger, adapter.StartStateStart, runtime.provider, runtime.router, runtime.dnsRouter, runtime.endpoint)
	if err != nil {
		return err
	}
	if err = runtime.network.validationError(); err != nil {
		return err
	}
	_, err = s.generation.ActivateInitial()
	if err != nil {
		return E.Cause(err, "activate initial runtime generation")
	}
	return nil
}

func (s *Box) start() error {
	err := s.preStart()
	if err != nil {
		return err
	}
	err = adapter.StartNamed(s.logger, adapter.StartStateStart, s.internalService)
	if err != nil {
		return err
	}
	err = adapter.Start(s.logger, adapter.StartStateStart, s.endpoint)
	if err != nil {
		return err
	}
	err = adapter.Start(s.logger, adapter.StartStateStart, s.certificateProvider)
	if err != nil {
		return err
	}
	err = adapter.Start(s.logger, adapter.StartStateStart, s.inbound, s.service)
	if err != nil {
		return err
	}
	runtime := s.initialRuntime
	err = adapter.Start(
		s.logger,
		adapter.StartStatePostStart,
		runtime.outbound,
		runtime.provider,
		s.network,
		runtime.dnsTransport,
		runtime.dnsRouter,
		s.connection,
		runtime.router,
		runtime.endpoint,
		s.endpoint,
		s.certificateProvider,
		s.inbound,
		s.service,
	)
	if err != nil {
		return err
	}
	err = adapter.StartNamed(s.logger, adapter.StartStatePostStart, s.internalService)
	if err != nil {
		return err
	}
	err = adapter.Start(
		s.logger,
		adapter.StartStateStarted,
		s.network,
		runtime.dnsTransport,
		runtime.dnsRouter,
		s.connection,
		runtime.router,
		runtime.outbound,
		runtime.provider,
		runtime.endpoint,
		s.endpoint,
		s.certificateProvider,
		s.inbound,
		s.service,
	)
	if err != nil {
		return err
	}
	err = adapter.StartNamed(s.logger, adapter.StartStateStarted, s.internalService)
	if err != nil {
		return err
	}
	s.reloadAccess.Lock()
	s.started = true
	s.initialRuntime = nil
	s.reloadAccess.Unlock()
	return nil
}

func (s *Box) Close() error {
	s.reloadAccess.Lock()
	defer s.reloadAccess.Unlock()
	select {
	case <-s.done:
		return os.ErrClosed
	default:
		close(s.done)
	}
	s.started = false
	var err error
	for _, closeItem := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"service", s.service},
		{"inbound", s.inbound},
		{"certificate-provider", s.certificateProvider},
		{"endpoint", s.endpoint},
		{"connection", s.connection},
	} {
		done := adapter.LogElapsed(s.logger, "close ", closeItem.name)
		err = E.Append(err, closeItem.service.Close(), func(err error) error {
			return E.Cause(err, "close ", closeItem.name)
		})
		done()
	}
	doneGeneration := adapter.LogElapsed(s.logger, "close runtime generations")
	err = E.Append(err, s.generation.Close(), func(closeErr error) error {
		return E.Cause(closeErr, "close runtime generations")
	})
	doneGeneration()
	doneNetwork := adapter.LogElapsed(s.logger, "close network")
	err = E.Append(err, s.network.Close(), func(closeErr error) error {
		return E.Cause(closeErr, "close network")
	})
	doneNetwork()
	if s.httpClientService != nil {
		s.logger.Trace("close ", s.httpClientService.Name())
		startTime := time.Now()
		err = E.Append(err, s.httpClientService.Close(), func(err error) error {
			return E.Cause(err, "close ", s.httpClientService.Name())
		})
		s.logger.Trace("close ", s.httpClientService.Name(), " completed (", F.Seconds(time.Since(startTime).Seconds()), "s)")
	}
	for _, lifecycleService := range s.internalService {
		done := adapter.LogElapsed(s.logger, "close ", lifecycleService.Name())
		err = E.Append(err, lifecycleService.Close(), func(err error) error {
			return E.Cause(err, "close ", lifecycleService.Name())
		})
		done()
	}
	done := adapter.LogElapsed(s.logger, "close logger")
	err = E.Append(err, s.logFactory.Close(), func(err error) error {
		return E.Cause(err, "close logger")
	})
	done()
	return err
}

func (s *Box) Network() adapter.NetworkManager {
	return s.network
}

func (s *Box) Router() adapter.Router {
	return s.router
}

func (s *Box) Inbound() adapter.InboundManager {
	return s.inbound
}

func (s *Box) Outbound() adapter.OutboundManager {
	return s.outbound
}

func (s *Box) Endpoint() adapter.EndpointManager {
	return s.endpointView
}

func (s *Box) LogFactory() log.Factory {
	return s.logFactory
}

func (s *Box) ReloadChan() <-chan struct{} {
	return s.reloadChan
}
