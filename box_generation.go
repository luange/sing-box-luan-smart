package box

import (
	"context"
	"fmt"
	"reflect"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/experimental"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

type RestartRequiredError struct {
	Field  string
	Reason string
}

func (e *RestartRequiredError) Error() string {
	return fmt.Sprintf("restart-required: %s: %s", e.Field, e.Reason)
}

func restartRequired(field, reason string) error {
	return &RestartRequiredError{Field: field, Reason: reason}
}

func (s *Box) Reload(options option.Options) error {
	s.reloadAccess.Lock()
	defer s.reloadAccess.Unlock()
	select {
	case <-s.done:
		return context.Canceled
	default:
	}
	if !s.started {
		return restartRequired("lifecycle", "graceful reload requires a fully started service")
	}
	if err := validateGenerationReload(s.options, options); err != nil {
		return err
	}
	candidate, err := s.buildRuntime(options, true)
	if err != nil {
		return E.Cause(err, "prepare runtime generation")
	}
	for _, stage := range adapter.ListStartStages {
		if err = candidate.Start(stage); err != nil {
			_ = candidate.Close()
			return E.Cause(err, "prepare runtime generation at ", stage.String())
		}
	}
	generationID, err := s.generation.Publish(candidate.snapshot())
	if err != nil {
		_ = candidate.Close()
		return err
	}
	s.options = options
	s.logger.Info("published runtime generation ", generationID)
	return nil
}

func validateGenerationReload(current option.Options, candidate option.Options) error {
	for _, check := range []struct {
		field  string
		old    any
		new    any
		reason string
	}{
		{"log", current.Log, candidate.Log, "logger ownership is process-wide"},
		{"certificate", current.Certificate, candidate.Certificate, "certificate store is shared by stable listeners"},
		{"certificate_providers", current.CertificateProviders, candidate.CertificateProviders, "certificate providers are owned by stable listeners"},
		{"http_clients", current.HTTPClients, candidate.HTTPClients, "HTTP client pools are shared across generations"},
		{"network_namespaces", current.NetworkNamespaces, candidate.NetworkNamespaces, "network namespace holders are process-wide"},
		{"endpoints", current.Endpoints, candidate.Endpoints, "endpoint ownership handoff is not supported"},
		{"inbounds", current.Inbounds, candidate.Inbounds, "listener ownership handoff is not supported"},
		{"services", current.Services, candidate.Services, "service listener ownership handoff is not supported"},
		{"ntp", current.NTP, candidate.NTP, "NTP owns a stable dialer and time service"},
		{"experimental", current.Experimental, candidate.Experimental, "cache and API listeners are stable services"},
		{"route.network", stableRouteOptions(current.Route), stableRouteOptions(candidate.Route), "stable network manager configuration changed"},
	} {
		if !reflect.DeepEqual(check.old, check.new) {
			return restartRequired(check.field, check.reason)
		}
	}
	if !reflect.DeepEqual(experimental.CalculateClashModeList(current), experimental.CalculateClashModeList(candidate)) {
		return restartRequired("route.mode", "Clash mode list is owned by the stable API server")
	}
	if hasStableTUNRuleSetReference(current.Inbounds) || hasStableTUNRuleSetReference(candidate.Inbounds) {
		return restartRequired("inbounds.tun.route_address_set", "stable TUN route-address-set references cannot be rebound safely yet")
	}
	return nil
}

func stableRouteOptions(routeOptions *option.RouteOptions) *option.RouteOptions {
	if routeOptions == nil {
		return nil
	}
	stable := *routeOptions
	stable.Rules = nil
	stable.RuleSet = nil
	stable.Final = ""
	stable.FindProcess = false
	stable.FindNeighbor = false
	stable.DHCPLeaseFiles = nil
	stable.DefaultDomainMatchStrategy = 0
	return &stable
}

func hasStableTUNRuleSetReference(inbounds []option.Inbound) bool {
	return common.Any(inbounds, func(inbound option.Inbound) bool {
		tunOptions, loaded := inbound.Options.(*option.TunInboundOptions)
		return loaded && (len(tunOptions.RouteAddressSet) > 0 || len(tunOptions.RouteExcludeAddressSet) > 0)
	})
}
