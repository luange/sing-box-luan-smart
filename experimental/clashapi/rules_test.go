package clashapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/adapter"

	"github.com/miekg/dns"
)

type rulesTestRouter struct {
	adapter.Router
	rules []adapter.Rule
}

func (r *rulesTestRouter) Rules() []adapter.Rule { return r.rules }

type rulesTestDNSRouter struct {
	adapter.DNSRouter
	rules []adapter.DNSRule
}

func (r *rulesTestDNSRouter) Rules() []adapter.DNSRule { return r.rules }

type rulesTestAction struct {
	value string
}

func (*rulesTestAction) Type() string     { return "route" }
func (a *rulesTestAction) String() string { return a.value }

type rulesTestRule struct {
	adapter.Rule
	ruleType string
	payload  string
	proxy    string
	disabled bool
	uuid     string
}

func (r *rulesTestRule) Type() string               { return r.ruleType }
func (r *rulesTestRule) String() string             { return r.payload }
func (r *rulesTestRule) Action() adapter.RuleAction { return &rulesTestAction{value: r.proxy} }
func (r *rulesTestRule) Disabled() bool             { return r.disabled }
func (r *rulesTestRule) UUID() string               { return r.uuid }

type rulesTestDNSRule struct {
	*rulesTestRule
}

func (*rulesTestDNSRule) LegacyPreMatch(*adapter.InboundContext) bool { return false }
func (*rulesTestDNSRule) WithAddressLimit() bool                      { return false }
func (*rulesTestDNSRule) MatchAddressLimit(*adapter.InboundContext, *dns.Msg) bool {
	return false
}

func TestGetRulesEncodesEmptyRulesAsArray(t *testing.T) {
	handler := getRules(new(rulesTestRouter), new(rulesTestDNSRouter))
	request := httptest.NewRequest(http.MethodGet, "/rules", nil)
	response := httptest.NewRecorder()
	handler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", response.Code, response.Body.String())
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if string(payload["rules"]) != "[]" {
		t.Fatalf("empty rules must be encoded as an array: %s", response.Body.String())
	}
}

func TestGetRulesPreservesDNSAndRouteRuleOrder(t *testing.T) {
	dnsRule := &rulesTestDNSRule{rulesTestRule: &rulesTestRule{
		ruleType: "dns", payload: "dns-payload", proxy: "dns-out", uuid: "dns-uuid",
	}}
	routeRule := &rulesTestRule{
		ruleType: "route", payload: "route-payload", proxy: "route-out", disabled: true, uuid: "route-uuid",
	}
	handler := getRules(
		&rulesTestRouter{rules: []adapter.Rule{routeRule}},
		&rulesTestDNSRouter{rules: []adapter.DNSRule{dnsRule}},
	)
	request := httptest.NewRequest(http.MethodGet, "/rules", nil)
	response := httptest.NewRecorder()
	handler(response, request)

	var payload struct {
		Rules []Rule `json:"rules"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Rules) != 2 {
		t.Fatalf("unexpected rules: %+v", payload.Rules)
	}
	if payload.Rules[0].UUID != "dns-uuid" || payload.Rules[1].UUID != "route-uuid" {
		t.Fatalf("rule order changed: %+v", payload.Rules)
	}
	if !payload.Rules[1].Disabled || payload.Rules[1].Proxy != "route-out" {
		t.Fatalf("route rule fields changed: %+v", payload.Rules[1])
	}
}
