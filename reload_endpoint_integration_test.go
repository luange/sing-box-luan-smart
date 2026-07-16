package box_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	boxEndpoint "github.com/sagernet/sing-box/adapter/endpoint"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const reloadTestEndpointType = "generation-test-endpoint"

type reloadTestEndpointOptions struct {
	FailStart bool
}

type reloadTestEndpoint struct {
	tag       string
	failStart bool
	starts    [4]atomic.Int32
	closes    atomic.Int32
}

func (*reloadTestEndpoint) Type() string           { return reloadTestEndpointType }
func (e *reloadTestEndpoint) Tag() string          { return e.tag }
func (*reloadTestEndpoint) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (*reloadTestEndpoint) Dependencies() []string { return nil }
func (*reloadTestEndpoint) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}
func (*reloadTestEndpoint) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}
func (e *reloadTestEndpoint) Start(stage adapter.StartStage) error {
	e.starts[stage].Add(1)
	if stage == adapter.StartStateStart && e.failStart {
		return net.ErrClosed
	}
	return nil
}
func (e *reloadTestEndpoint) Close() error {
	e.closes.Add(1)
	return nil
}

type reloadTestEndpointRecords struct {
	access sync.Mutex
	byTag  map[string][]*reloadTestEndpoint
}

func (r *reloadTestEndpointRecords) add(endpoint *reloadTestEndpoint) {
	r.access.Lock()
	r.byTag[endpoint.tag] = append(r.byTag[endpoint.tag], endpoint)
	r.access.Unlock()
}

func (r *reloadTestEndpointRecords) records(tag string) []*reloadTestEndpoint {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]*reloadTestEndpoint(nil), r.byTag[tag]...)
}

func TestReloadProviderEndpointsAreGenerationPrivate(t *testing.T) {
	records := &reloadTestEndpointRecords{byTag: make(map[string][]*reloadTestEndpoint)}
	ctx := include.Context(context.Background())
	registry := service.FromContext[adapter.EndpointRegistry](ctx).(*boxEndpoint.Registry)
	boxEndpoint.Register[reloadTestEndpointOptions](registry, reloadTestEndpointType, func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options reloadTestEndpointOptions) (adapter.Endpoint, error) {
		endpoint := &reloadTestEndpoint{tag: tag, failStart: options.FailStart}
		records.add(endpoint)
		return endpoint, nil
	})

	instance, err := box.New(box.Options{Context: ctx, Options: reloadEndpointTestOptions(false)})
	if err != nil {
		t.Fatal(err)
	}
	if err = instance.Start(); err != nil {
		_ = instance.Close()
		t.Fatal(err)
	}

	stable := singleReloadEndpointRecord(t, records, "stable")
	firstLeaf := singleReloadEndpointRecord(t, records, "pool/leaf")
	assertReloadEndpointStartedOnce(t, stable)
	assertReloadEndpointStartedOnce(t, firstLeaf)
	assertReloadEndpointVisible(t, instance, "pool/leaf")
	assertReloadEndpointManagerVisible(t, instance, "pool/leaf")

	if err = instance.Reload(reloadEndpointTestOptions(false)); err != nil {
		_ = instance.Close()
		t.Fatal(err)
	}
	leaves := records.records("pool/leaf")
	if len(leaves) != 2 {
		_ = instance.Close()
		t.Fatalf("expected two private provider endpoints after reload, got %d", len(leaves))
	}
	secondLeaf := leaves[1]
	assertReloadEndpointStartedOnce(t, secondLeaf)
	assertReloadEndpointVisible(t, instance, "pool/leaf")
	assertReloadEndpointManagerVisible(t, instance, "pool/leaf")
	waitForReloadEndpointClose(t, firstLeaf, 1)
	if stable.closes.Load() != 0 {
		_ = instance.Close()
		t.Fatal("stable endpoint was closed by generation publication")
	}
	assertReloadEndpointStartedOnce(t, stable)

	if err = instance.Reload(reloadEndpointTestOptions(true)); err == nil {
		_ = instance.Close()
		t.Fatal("endpoint start failure unexpectedly published")
	}
	leaves = records.records("pool/leaf")
	if len(leaves) != 3 {
		_ = instance.Close()
		t.Fatalf("expected failed candidate endpoint record, got %d", len(leaves))
	}
	failedLeaf := leaves[2]
	waitForReloadEndpointClose(t, failedLeaf, 1)
	if secondLeaf.closes.Load() != 0 || stable.closes.Load() != 0 {
		_ = instance.Close()
		t.Fatalf("failed PREPARE touched active endpoints: stable=%d active-leaf=%d", stable.closes.Load(), secondLeaf.closes.Load())
	}

	if err = instance.Close(); err != nil {
		t.Fatal(err)
	}
	if stable.closes.Load() != 1 || secondLeaf.closes.Load() != 1 {
		t.Fatalf("final close mismatch: stable=%d active-leaf=%d", stable.closes.Load(), secondLeaf.closes.Load())
	}
}

func reloadEndpointTestOptions(failStart bool) option.Options {
	return option.Options{
		Log: &option.LogOptions{Disabled: true},
		Endpoints: []option.Endpoint{{
			Type:    reloadTestEndpointType,
			Tag:     "stable",
			Options: &reloadTestEndpointOptions{},
		}},
		Providers: []option.Provider{{
			Type: C.ProviderTypeInline,
			Tag:  "pool",
			Options: &option.ProviderInlineOptions{Endpoints: []option.Endpoint{{
				Type:    reloadTestEndpointType,
				Tag:     "leaf",
				Options: &reloadTestEndpointOptions{FailStart: failStart},
			}}},
		}},
		Outbounds: []option.Outbound{{Type: C.TypeDirect, Tag: "direct", Options: &option.DirectOutboundOptions{}}},
		Route:     &option.RouteOptions{Final: "direct"},
	}
}

func singleReloadEndpointRecord(t *testing.T, records *reloadTestEndpointRecords, tag string) *reloadTestEndpoint {
	t.Helper()
	items := records.records(tag)
	if len(items) != 1 {
		t.Fatalf("expected one endpoint %q, got %d", tag, len(items))
	}
	return items[0]
}

func assertReloadEndpointStartedOnce(t *testing.T, endpoint *reloadTestEndpoint) {
	t.Helper()
	for stage := adapter.StartStateInitialize; stage <= adapter.StartStateStarted; stage++ {
		if count := endpoint.starts[stage].Load(); count != 1 {
			t.Fatalf("endpoint %s stage %s started %d times", endpoint.tag, stage.String(), count)
		}
	}
}

func waitForReloadEndpointClose(t *testing.T, endpoint *reloadTestEndpoint, expected int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if endpoint.closes.Load() == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("endpoint %s close count=%d expected=%d", endpoint.tag, endpoint.closes.Load(), expected)
}

func assertReloadEndpointVisible(t *testing.T, instance *box.Box, tag string) {
	t.Helper()
	for _, outbound := range instance.Outbound().Outbounds() {
		if outbound.Tag() == tag {
			return
		}
	}
	t.Fatalf("generation endpoint %q is missing from stable outbound view", tag)
}

func assertReloadEndpointManagerVisible(t *testing.T, instance *box.Box, tag string) {
	t.Helper()
	if endpoint, loaded := instance.Endpoint().Get(tag); !loaded || endpoint.Tag() != tag {
		t.Fatalf("generation endpoint %q is missing from endpoint view", tag)
	}
}
