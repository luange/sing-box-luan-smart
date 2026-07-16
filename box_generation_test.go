package box

import (
	"errors"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestValidateGenerationReloadAllowsRuntimeFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*option.Options)
	}{
		{"outbounds", func(options *option.Options) {
			options.Outbounds = []option.Outbound{{Type: C.TypeDirect, Tag: "direct", Options: &option.DirectOutboundOptions{}}}
		}},
		{"providers", func(options *option.Options) {
			options.Providers = []option.Provider{{Type: C.ProviderTypeInline, Tag: "pool", Options: &option.ProviderInlineOptions{}}}
		}},
		{"dns", func(options *option.Options) {
			options.DNS = &option.DNSOptions{RawDNSOptions: option.RawDNSOptions{DNSClientOptions: option.DNSClientOptions{CacheClientSubnet: true}}}
		}},
		{"route-final", func(options *option.Options) {
			options.Route.Final = "proxy"
		}},
		{"route-runtime-discovery", func(options *option.Options) {
			options.Route.FindProcess = true
			options.Route.FindNeighbor = true
			options.Route.DHCPLeaseFiles = badoption.Listable[string]{"/tmp/dhcp.leases"}
			options.Route.DefaultDomainMatchStrategy = option.DomainMatchStrategy(C.DomainMatchStrategyPreferFQDN)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := generationReloadBaseOptions()
			candidate := generationReloadBaseOptions()
			test.mutate(&candidate)
			if err := validateGenerationReload(current, candidate); err != nil {
				t.Fatalf("runtime change was rejected: %v", err)
			}
		})
	}
}

func TestValidateGenerationReloadRejectsStableFields(t *testing.T) {
	tests := []struct {
		name          string
		expectedField string
		mutate        func(*option.Options)
	}{
		{"log", "log", func(options *option.Options) { options.Log.Level = "debug" }},
		{"certificate", "certificate", func(options *option.Options) { options.Certificate = &option.CertificateOptions{Store: "system"} }},
		{"http-clients", "http_clients", func(options *option.Options) { options.HTTPClients = []option.HTTPClient{{Tag: "test"}} }},
		{"network-namespaces", "network_namespaces", func(options *option.Options) {
			options.NetworkNamespaces = []option.NetworkNamespace{{Tag: "test", Type: "unshare"}}
		}},
		{"endpoints", "endpoints", func(options *option.Options) {
			options.Endpoints = []option.Endpoint{{Type: C.TypeWireGuard, Tag: "wg"}}
		}},
		{"inbounds", "inbounds", func(options *option.Options) {
			options.Inbounds = []option.Inbound{{Type: C.TypeMixed, Tag: "mixed", Options: &option.HTTPMixedInboundOptions{}}}
		}},
		{"services", "services", func(options *option.Options) {
			options.Services = []option.Service{{Type: C.TypeResolved, Tag: "resolved"}}
		}},
		{"ntp", "ntp", func(options *option.Options) { options.NTP = &option.NTPOptions{Enabled: true} }},
		{"experimental", "experimental", func(options *option.Options) {
			options.Experimental = &option.ExperimentalOptions{URLTestUnifiedDelay: true}
		}},
		{"stable-network", "route.network", func(options *option.Options) { options.Route.DefaultInterface = "eth0" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := generationReloadBaseOptions()
			candidate := generationReloadBaseOptions()
			test.mutate(&candidate)
			err := validateGenerationReload(current, candidate)
			var restartErr *RestartRequiredError
			if !errors.As(err, &restartErr) {
				t.Fatalf("stable change did not return RestartRequiredError: %v", err)
			}
			if restartErr.Field != test.expectedField {
				t.Fatalf("unexpected restart field %q, expected %q", restartErr.Field, test.expectedField)
			}
		})
	}
}

func TestValidateGenerationReloadRejectsStableTUNRuleSetReferences(t *testing.T) {
	options := generationReloadBaseOptions()
	options.Inbounds = []option.Inbound{{
		Type: C.TypeTun,
		Tag:  "tun",
		Options: &option.TunInboundOptions{
			RouteAddressSet: badoption.Listable[string]{"private"},
		},
	}}
	err := validateGenerationReload(options, options)
	var restartErr *RestartRequiredError
	if !errors.As(err, &restartErr) || restartErr.Field != "inbounds.tun.route_address_set" {
		t.Fatalf("stable TUN rule-set reference was not classified: %v", err)
	}
}

func generationReloadBaseOptions() option.Options {
	return option.Options{
		Log:   &option.LogOptions{Disabled: true},
		Route: &option.RouteOptions{},
	}
}
