package clashapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type smartAutomaticTestOutbound struct {
	tag string
}

func (*smartAutomaticTestOutbound) Type() string { return "smart" }
func (s *smartAutomaticTestOutbound) Tag() string {
	if s.tag == "" {
		return "smart-test"
	}
	return s.tag
}
func (*smartAutomaticTestOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (*smartAutomaticTestOutbound) Dependencies() []string { return nil }
func (*smartAutomaticTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, nil
}
func (*smartAutomaticTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}
func (*smartAutomaticTestOutbound) Now() string   { return "candidate" }
func (*smartAutomaticTestOutbound) All() []string { return []string{"candidate"} }
func (*smartAutomaticTestOutbound) URLTest(context.Context) (map[string]uint16, error) {
	return map[string]uint16{"candidate": 1}, nil
}
func (*smartAutomaticTestOutbound) SmartStatus() adapter.SmartGroupStatus {
	return adapter.SmartGroupStatus{}
}
func (*smartAutomaticTestOutbound) SelectOutbound(string) bool { return true }
func (*smartAutomaticTestOutbound) ClearSelection()            {}
func (*smartAutomaticTestOutbound) SelectTemporaryOutbound(string, time.Duration, string) bool {
	return true
}
func (*smartAutomaticTestOutbound) ClearTemporarySelection() {}

func TestSmartAutomaticProxyInfoSupportsDashboardRendering(t *testing.T) {
	content, err := json.Marshal(smartAutomaticProxyInfo(smartAutomaticProxyName))
	if err != nil {
		t.Fatal(err)
	}
	var info map[string]any
	if err = json.Unmarshal(content, &info); err != nil {
		t.Fatal(err)
	}
	if info["name"] != smartAutomaticProxyName {
		t.Fatalf("unexpected automatic proxy name: %v", info["name"])
	}
	if info["type"] != "Direct" {
		t.Fatalf("unexpected automatic proxy type: %v", info["type"])
	}
	if udp, loaded := info["udp"].(bool); !loaded || !udp {
		t.Fatalf("automatic proxy must advertise UDP support: %v", info["udp"])
	}
	if _, loaded := info["history"].([]any); !loaded {
		t.Fatalf("automatic proxy history is not a JSON array: %T", info["history"])
	}
}

func TestSmartAutomaticDelayOutbound(t *testing.T) {
	smart := &smartAutomaticTestOutbound{}
	selected, loaded := smartAutomaticOutboundByName([]adapter.Outbound{nil, smart}, smartAutomaticProxyName)
	if !loaded || selected.Tag() != smartAutomaticProxyName {
		t.Fatalf("expected Smart automatic outbound, got %T loaded=%v", selected, loaded)
	}
	if selected, loaded = smartAutomaticOutboundByName(nil, smartAutomaticProxyName); loaded || selected != nil {
		t.Fatalf("unexpected outbound without Smart group: %T", selected)
	}
}

func TestSmartAutomaticOutboundsAreUniquePerGroup(t *testing.T) {
	first := &smartAutomaticTestOutbound{tag: "smart-hk"}
	second := &smartAutomaticTestOutbound{tag: "smart-us"}
	outbounds := []adapter.Outbound{first, second}
	names := smartAutomaticProxyNames(outbounds)
	if names[first.Tag()] == names[second.Tag()] || names[first.Tag()] == smartAutomaticProxyName {
		t.Fatalf("multi-group automatic names are ambiguous: %v", names)
	}
	for _, group := range outbounds {
		name := names[group.Tag()]
		selected, loaded := smartAutomaticOutboundByName(outbounds, name)
		if !loaded || selected.Tag() != name {
			t.Fatalf("automatic name %q did not resolve to its group", name)
		}
	}
}

type smartControlTestOutbound struct {
	smartAutomaticTestOutbound
	access    sync.Mutex
	pinned    string
	temporary string
}

func (s *smartControlTestOutbound) SmartStatus() adapter.SmartGroupStatus {
	s.access.Lock()
	defer s.access.Unlock()
	return adapter.SmartGroupStatus{Pinned: s.pinned, TemporaryOverride: s.temporary}
}

func (s *smartControlTestOutbound) SelectOutbound(tag string) bool {
	if tag != "candidate" {
		return false
	}
	s.access.Lock()
	s.pinned = tag
	s.access.Unlock()
	return true
}

func (s *smartControlTestOutbound) ClearSelection() {
	s.access.Lock()
	s.pinned = ""
	s.access.Unlock()
}

func (s *smartControlTestOutbound) SelectTemporaryOutbound(tag string, _ time.Duration, _ string) bool {
	if tag != "candidate" {
		return false
	}
	s.access.Lock()
	s.temporary = tag
	s.access.Unlock()
	return true
}

func (s *smartControlTestOutbound) ClearTemporarySelection() {
	s.access.Lock()
	s.temporary = ""
	s.access.Unlock()
}

type smartControlOutboundManager struct {
	adapter.OutboundManager
	smart adapter.Outbound
}

func (m *smartControlOutboundManager) Outbounds() []adapter.Outbound {
	return []adapter.Outbound{m.smart}
}
func (m *smartControlOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	if tag == m.smart.Tag() {
		return m.smart, true
	}
	return nil, false
}
func (m *smartControlOutboundManager) Default() adapter.Outbound { return m.smart }

type multiSmartControlOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

func (m *multiSmartControlOutboundManager) Outbounds() []adapter.Outbound { return m.outbounds }
func (m *multiSmartControlOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, outbound := range m.outbounds {
		if outbound.Tag() == tag {
			return outbound, true
		}
	}
	return nil, false
}
func (m *multiSmartControlOutboundManager) Default() adapter.Outbound { return m.outbounds[0] }

type emptySmartEndpointManager struct {
	adapter.EndpointManager
}

func (*emptySmartEndpointManager) Endpoints() []adapter.Endpoint { return nil }

func TestZashboardSmartSelectionStateMachine(t *testing.T) {
	smart := new(smartControlTestOutbound)
	manager := &smartControlOutboundManager{smart: smart}
	server := &Server{outbound: manager, urlTestHistory: urltest.NewHistoryStorage()}
	handler := proxyRouter(server, nil)

	for range 500 {
		assertSmartUpdate(t, handler, `{"name":"candidate"}`, http.StatusNoContent)
		assertSmartNow(t, handler, "candidate")
		assertSmartUpdate(t, handler, `{"name":"`+smartAutomaticProxyName+`"}`, http.StatusNoContent)
		assertSmartNow(t, handler, smartAutomaticProxyName)
	}
	assertSmartUpdate(t, handler, `{"name":"candidate","persistent":true}`, http.StatusNoContent)
	assertSmartNow(t, handler, "candidate")
	assertSmartUpdate(t, handler, `{"name":`, http.StatusBadRequest)
	assertSmartNow(t, handler, "candidate")
	assertSmartUpdate(t, handler, `{"name":"missing"}`, http.StatusBadRequest)
	assertSmartNow(t, handler, "candidate")
	assertSmartUpdate(t, handler, `{"name":"`+smartAutomaticProxyName+`"}`, http.StatusNoContent)
	assertSmartNow(t, handler, smartAutomaticProxyName)
}

func TestZashboardMultipleSmartGroupsUseUniqueAutomaticItems(t *testing.T) {
	first := &smartControlTestOutbound{smartAutomaticTestOutbound: smartAutomaticTestOutbound{tag: "smart-hk"}}
	second := &smartControlTestOutbound{smartAutomaticTestOutbound: smartAutomaticTestOutbound{tag: "smart-us"}}
	manager := &multiSmartControlOutboundManager{outbounds: []adapter.Outbound{first, second}}
	server := &Server{outbound: manager, endpoint: &emptySmartEndpointManager{}, urlTestHistory: urltest.NewHistoryStorage()}
	handler := proxyRouter(server, nil)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected proxy list status %d: %s", response.Code, response.Body.String())
	}
	var payload struct {
		Proxies map[string]struct {
			Now string   `json:"now"`
			All []string `json:"all"`
		} `json:"proxies"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	for _, groupTag := range []string{"smart-hk", "smart-us"} {
		automaticName := smartAutomaticProxyName + " · " + groupTag
		groupInfo := payload.Proxies[groupTag]
		if len(groupInfo.All) == 0 || groupInfo.All[0] != automaticName || groupInfo.Now != automaticName {
			t.Fatalf("group %s has ambiguous automatic state: %+v", groupTag, groupInfo)
		}
		if _, loaded := payload.Proxies[automaticName]; !loaded {
			t.Fatalf("automatic proxy %q missing from top-level map", automaticName)
		}
		getAutomatic := httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(automaticName), nil)
		getResponse := httptest.NewRecorder()
		handler.ServeHTTP(getResponse, getAutomatic)
		if getResponse.Code != http.StatusOK {
			t.Fatalf("automatic proxy %q GET failed: %d %s", automaticName, getResponse.Code, getResponse.Body.String())
		}
		assertSmartUpdate(t, handler, `{"name":"candidate"}`, http.StatusNoContent, groupTag)
		assertSmartUpdate(t, handler, `{"name":"`+automaticName+`"}`, http.StatusNoContent, groupTag)
	}
}

func assertSmartUpdate(t *testing.T, handler http.Handler, body string, expectedStatus int, groupTag ...string) {
	t.Helper()
	tag := "smart-test"
	if len(groupTag) > 0 {
		tag = groupTag[0]
	}
	request := httptest.NewRequest(http.MethodPut, "/"+tag, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != expectedStatus {
		t.Fatalf("unexpected update status %d, body=%s", response.Code, response.Body.String())
	}
}

func assertSmartNow(t *testing.T, handler http.Handler, expected string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/smart-test", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected GET status %d, body=%s", response.Code, response.Body.String())
	}
	var info map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info["now"] != expected {
		t.Fatalf("unexpected Smart state: got=%v expected=%s", info["now"], expected)
	}
}
