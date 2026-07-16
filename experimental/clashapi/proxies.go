package clashapi

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json/badjson"
	N "github.com/sagernet/sing/common/network"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

const smartAutomaticProxyName = "♻️ 智能选择"

func proxyRouter(server *Server, router adapter.Router) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProxies(server))

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName(server))
		r.Get("/", getProxy(server))
		r.Get("/delay", getProxyDelay(server))
		r.Put("/", updateProxy)
	})
	return r
}

func parseProxyName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProxyName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findProxyByName(server *Server) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name := r.Context().Value(CtxKeyProxyName).(string)
			proxy, exist := server.outbound.Outbound(name)
			if !exist {
				proxy, exist = smartAutomaticOutboundByName(server.outbound.Outbounds(), name)
			}
			if !exist {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			ctx := context.WithValue(r.Context(), CtxKeyProxy, proxy)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type smartAutomaticOutbound struct {
	adapter.Outbound
	name string
}

func (*smartAutomaticOutbound) Type() string  { return C.TypeDirect }
func (o *smartAutomaticOutbound) Tag() string { return o.name }

func smartAutomaticProxyNames(outbounds []adapter.Outbound) map[string]string {
	smartTags := make([]string, 0, len(outbounds))
	for _, detour := range outbounds {
		if detour == nil {
			continue
		}
		if _, isSmart := detour.(adapter.SmartGroup); isSmart {
			smartTags = append(smartTags, detour.Tag())
		}
	}
	names := make(map[string]string, len(smartTags))
	for _, tag := range smartTags {
		if len(smartTags) == 1 {
			names[tag] = smartAutomaticProxyName
		} else {
			names[tag] = smartAutomaticProxyName + " · " + tag
		}
	}
	return names
}

func smartAutomaticOutboundByName(outbounds []adapter.Outbound, name string) (adapter.Outbound, bool) {
	names := smartAutomaticProxyNames(outbounds)
	for _, detour := range outbounds {
		if detour == nil || names[detour.Tag()] != name {
			continue
		}
		return &smartAutomaticOutbound{Outbound: detour, name: name}, true
	}
	return nil, false
}

func proxyInfo(server *Server, detour adapter.Outbound) *badjson.JSONObject {
	automaticName := smartAutomaticProxyNames(server.outbound.Outbounds())[detour.Tag()]
	return proxyInfoWithAutomaticName(server, detour, automaticName)
}

func proxyInfoWithAutomaticName(server *Server, detour adapter.Outbound, automaticName string) *badjson.JSONObject {
	var info badjson.JSONObject
	var clashType string
	switch detour.Type() {
	case C.TypeBlock:
		clashType = "Reject"
	default:
		clashType = C.ProxyDisplayName(detour.Type())
	}
	info.Put("type", clashType)
	info.Put("name", detour.Tag())
	info.Put("udp", common.Contains(detour.Network(), N.NetworkUDP))
	delayHistory := server.urlTestHistory.LoadURLTestHistory(adapter.OutboundTag(detour))
	if delayHistory != nil {
		info.Put("history", []*adapter.URLTestHistory{delayHistory})
	} else {
		info.Put("history", []*adapter.URLTestHistory{})
	}
	if group, isGroup := detour.(adapter.OutboundGroup); isGroup {
		info.Put("now", group.Now())
		info.Put("all", group.All())
	}
	if smartGroup, isSmart := detour.(adapter.SmartGroup); isSmart {
		if automaticName == "" {
			automaticName = smartAutomaticProxyName
		}
		status := smartGroup.SmartStatus()
		info.Put("type", "Selector")
		info.Put("all", append([]string{automaticName}, smartGroup.All()...))
		switch {
		case status.TemporaryOverride != "":
			info.Put("now", status.TemporaryOverride)
		case status.Pinned != "":
			info.Put("now", status.Pinned)
		default:
			info.Put("now", automaticName)
		}
		info.Put("smart", status)
	}
	return &info
}

func getProxies(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var proxyMap badjson.JSONObject
		outbounds := common.Filter(server.outbound.Outbounds(), func(detour adapter.Outbound) bool {
			return detour.Tag() != ""
		})
		outbounds = append(outbounds, common.Map(common.Filter(server.endpoint.Endpoints(), func(detour adapter.Endpoint) bool {
			return detour.Tag() != ""
		}), func(it adapter.Endpoint) adapter.Outbound {
			return it
		})...)
		outbounds = uniqueProxyOutbounds(outbounds)
		automaticNames := smartAutomaticProxyNames(outbounds)

		allProxies := make([]string, 0, len(outbounds))

		for _, detour := range outbounds {
			switch detour.Type() {
			case C.TypeDirect, C.TypeBlock, C.TypeDNS:
				continue
			}
			allProxies = append(allProxies, detour.Tag())
		}

		defaultTag := server.outbound.Default().Tag()

		sort.SliceStable(allProxies, func(i, j int) bool {
			return allProxies[i] == defaultTag
		})

		// fix clash dashboard
		proxyMap.Put("GLOBAL", map[string]any{
			"type":    "Fallback",
			"name":    "GLOBAL",
			"udp":     true,
			"history": []*adapter.URLTestHistory{},
			"all":     allProxies,
			"now":     defaultTag,
		})

		for _, detour := range outbounds {
			automaticName := automaticNames[detour.Tag()]
			if automaticName != "" {
				proxyMap.Put(automaticName, smartAutomaticProxyInfo(automaticName))
			}
		}

		for i, detour := range outbounds {
			var tag string
			if detour.Tag() == "" {
				tag = F.ToString(i)
			} else {
				tag = detour.Tag()
			}
			proxyMap.Put(tag, proxyInfoWithAutomaticName(server, detour, automaticNames[detour.Tag()]))
		}
		var responseMap badjson.JSONObject
		responseMap.Put("proxies", &proxyMap)
		response, err := responseMap.MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

func uniqueProxyOutbounds(outbounds []adapter.Outbound) []adapter.Outbound {
	seen := make(map[string]bool, len(outbounds))
	result := outbounds[:0]
	for _, outbound := range outbounds {
		if outbound == nil || seen[outbound.Tag()] {
			continue
		}
		seen[outbound.Tag()] = true
		result = append(result, outbound)
	}
	return result
}

func getProxy(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		response, err := proxyInfo(server, proxy).MarshalJSON()
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		w.Write(response)
	}
}

type UpdateProxyRequest struct {
	Name       string `json:"name"`
	Temporary  *bool  `json:"temporary,omitempty"`
	TTL        int64  `json:"ttl,omitempty"`
	Persistent bool   `json:"persistent,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

func updateProxy(w http.ResponseWriter, r *http.Request) {
	req := UpdateProxyRequest{}
	if err := render.DecodeJSON(r.Body, &req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
	if selector, isSelector := proxy.(adapter.SelectorGroup); isSelector {
		if !selector.SelectOutbound(req.Name) {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("Selector update error: not found"))
			return
		}
	} else if smartGroup, isSmart := proxy.(adapter.SmartGroup); isSmart {
		switch {
		case req.Name == "" || isSmartAutomaticSelection(req.Name, proxy.Tag()):
			smartGroup.ClearTemporarySelection()
			smartGroup.ClearSelection()
		case req.Persistent || req.Temporary != nil && !*req.Temporary:
			if !smartGroup.SelectOutbound(req.Name) {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError("Smart pin error: candidate not found"))
				return
			}
			smartGroup.ClearTemporarySelection()
		default:
			ttl := req.TTL
			if ttl <= 0 {
				ttl = 1800
			}
			ttl = min(max(ttl, 60), 86400)
			if !smartGroup.SelectTemporaryOutbound(req.Name, time.Duration(ttl)*time.Second, req.Reason) {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError("Smart override error: candidate not found"))
				return
			}
			smartGroup.ClearSelection()
		}
	} else {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("Must be a Selector or Smart group"))
		return
	}

	render.NoContent(w, r)
}

func isSmartAutomaticSelection(name, groupTag string) bool {
	return name == smartAutomaticProxyName || name == smartAutomaticProxyName+" · "+groupTag
}

func smartAutomaticProxyInfo(name string) map[string]any {
	return map[string]any{
		"type":    "Direct",
		"name":    name,
		"udp":     true,
		"history": []*adapter.URLTestHistory{},
	}
}

func getProxyDelay(server *Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		url := query.Get("url")
		if strings.HasPrefix(url, "http://") {
			url = ""
		}
		timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 16)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		proxy := r.Context().Value(CtxKeyProxy).(adapter.Outbound)
		ctx, cancel := context.WithTimeout(r.Context(), time.Millisecond*time.Duration(timeout))
		defer cancel()

		delay, err := urltest.URLTest(ctx, url, proxy)
		defer func() {
			realTag := group.RealTag(proxy)
			if err != nil {
				server.urlTestHistory.DeleteURLTestHistory(realTag)
			} else {
				server.urlTestHistory.StoreURLTestHistory(realTag, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: delay,
				})
			}
		}()

		if ctx.Err() != nil {
			render.Status(r, http.StatusGatewayTimeout)
			render.JSON(w, r, ErrRequestTimeout)
			return
		}

		if err != nil || delay == 0 {
			render.Status(r, http.StatusServiceUnavailable)
			render.JSON(w, r, newError("An error occurred in the delay test"))
			return
		}

		render.JSON(w, r, render.M{
			"delay": delay,
		})
	}
}
