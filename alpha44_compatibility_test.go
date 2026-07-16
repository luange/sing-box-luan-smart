package box_test

import (
	"context"
	stdJSON "encoding/json"
	"testing"

	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestAlpha44FieldsSurviveSmartConfigRoundTrip(t *testing.T) {
	ctx := include.Context(context.Background())
	content := []byte(`{
  "network_namespaces": [
    {"type":"unshare","tag":"isolated","pid_file":"/tmp/sing-box-netns.pid"}
  ],
  "dns": {
    "cache_client_subnet": true,
    "servers": [
      {"type":"tcp","tag":"dns-tcp","server":"1.1.1.1","pipeline":true,"max_queries":8}
    ]
  },
  "outbounds": [
    {"type":"snell","tag":"snell6","server":"example.com","server_port":443,"version":6,"psk":"test-psk","mode":"v6"},
    {"type":"hysteria2","tag":"hy2","server":"example.com","server_port":443,"password":"test","tls":{"enabled":true,"server_name":"example.com"},"realm":{"server_url":"https://realm.example.com","realm_id":"test","stun_servers":["stun.example.com:3478"],"ip_version":6,"port_mapping":{"enabled":true,"timeout":"5s","lifetime":"1m"}}},
    {"type":"smart","tag":"smart","outbounds":["snell6","hy2"],"max_attempts":2,"attempt_timeout":"3s","reach_tests":[{"tag":"gemini","preset":"gemini","unmeasured_policy":"fallback"}]}
  ],
  "route": {"final":"smart"},
  "experimental": {"cache_file":{"enabled":true,"store_dns":true}}
}`)
	options, err := json.UnmarshalExtendedContext[option.Options](ctx, content)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.NetworkNamespaces) != 1 || options.NetworkNamespaces[0].Tag != "isolated" || options.NetworkNamespaces[0].UnshareOptions.PidFile == "" {
		t.Fatalf("network namespace fields were not decoded: %+v", options.NetworkNamespaces)
	}
	if options.DNS == nil || !options.DNS.CacheClientSubnet {
		t.Fatal("dns.cache_client_subnet was lost")
	}
	snell, loaded := options.Outbounds[0].Options.(*option.SnellOutboundOptions)
	if !loaded || snell.Version != 6 || snell.V6Options.Mode != "v6" {
		t.Fatalf("Snell v6 fields were not decoded: %#v", options.Outbounds[0].Options)
	}
	hysteria, loaded := options.Outbounds[1].Options.(*option.Hysteria2OutboundOptions)
	if !loaded || hysteria.Realm == nil || hysteria.Realm.IPVersion != 6 || hysteria.Realm.PortMapping == nil || !hysteria.Realm.PortMapping.Enabled {
		t.Fatalf("Hysteria2 realm fields were not decoded: %#v", options.Outbounds[1].Options)
	}
	smart, loaded := options.Outbounds[2].Options.(*option.SmartOutboundOptions)
	if !loaded || smart.MaxAttempts != 2 || len(smart.ReachTests) != 1 {
		t.Fatalf("Smart fields were not decoded: %#v", options.Outbounds[2].Options)
	}

	roundTrip, err := json.MarshalContext(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = stdJSON.Unmarshal(roundTrip, &document); err != nil {
		t.Fatal(err)
	}
	dnsDocument := document["dns"].(map[string]any)
	if dnsDocument["cache_client_subnet"] != true {
		t.Fatal("round-trip removed dns.cache_client_subnet")
	}
	cacheDocument := document["experimental"].(map[string]any)["cache_file"].(map[string]any)
	if cacheDocument["store_dns"] != true {
		t.Fatal("round-trip removed experimental.cache_file.store_dns")
	}
	outbounds := document["outbounds"].([]any)
	realm := outbounds[1].(map[string]any)["realm"].(map[string]any)
	if realm["ip_version"] != float64(6) || realm["port_mapping"].(map[string]any)["enabled"] != true {
		t.Fatal("round-trip removed Hysteria2 realm fields")
	}
	if _, err = json.UnmarshalExtendedContext[option.Options](ctx, roundTrip); err != nil {
		t.Fatal("round-trip output no longer decodes: ", err)
	}
}

func TestAlpha44UnknownFieldsRemainRejected(t *testing.T) {
	ctx := include.Context(context.Background())
	_, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(`{"unknown_alpha44_field":true}`))
	if err == nil {
		t.Fatal("unknown top-level field was silently accepted")
	}
}
