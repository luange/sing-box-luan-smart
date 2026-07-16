package box_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
)

func TestGracefulReloadKeepsEstablishedTCP(t *testing.T) {
	echoListener := listenLocalTCP(t)
	defer echoListener.Close()
	go serveEcho(echoListener)
	proxyPort := reserveTCPPort(t)
	instance := startReloadTestBox(t, reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
	defer instance.Close()

	proxyDialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", proxyPort), socks.Version5, "", "")
	destination := M.ParseSocksaddr(echoListener.Addr().String())
	oldConn, err := proxyDialer.DialContext(context.Background(), N.NetworkTCP, destination)
	if err != nil {
		t.Fatal(err)
	}
	defer oldConn.Close()
	assertEcho(t, oldConn, "before reload")

	if err = instance.Reload(reloadTestOptions(proxyPort, C.TypeBlock, "blocked")); err != nil {
		t.Fatal(err)
	}
	assertEcho(t, oldConn, "after reload")
	newConn, err := proxyDialer.DialContext(context.Background(), N.NetworkTCP, destination)
	if err == nil {
		newConn.Close()
		t.Fatal("new connection unexpectedly used the retired direct generation")
	}
}

func TestReloadRejectsStableListenerChange(t *testing.T) {
	instance := startReloadTestBox(t, reloadTestOptions(18080, C.TypeDirect, "direct"))
	defer instance.Close()
	err := instance.Reload(reloadTestOptions(18081, C.TypeDirect, "direct"))
	if err == nil {
		t.Fatal("listener change was accepted without restart-required")
	}
	if _, loaded := err.(*box.RestartRequiredError); !loaded {
		t.Fatalf("unexpected error type: %T: %v", err, err)
	}
}

func TestReloadPrepareFailureKeepsCurrentRuntime(t *testing.T) {
	echoListener := listenLocalTCP(t)
	defer echoListener.Close()
	go serveEcho(echoListener)
	proxyPort := reserveTCPPort(t)
	instance := startReloadTestBox(t, reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
	defer instance.Close()

	broken := reloadTestOptions(proxyPort, C.TypeDirect, "direct")
	broken.Route.Final = "missing-default"
	if err := instance.Reload(broken); err == nil {
		t.Fatal("invalid candidate generation unexpectedly published")
	}

	proxyDialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", proxyPort), socks.Version5, "", "")
	conn, err := proxyDialer.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr(echoListener.Addr().String()))
	if err != nil {
		t.Fatal("last-known-good runtime stopped serving: ", err)
	}
	defer conn.Close()
	assertEcho(t, conn, "last-known-good")
}

func TestGracefulReloadDuringActiveTCPTransfer(t *testing.T) {
	echoListener := listenLocalTCP(t)
	defer echoListener.Close()
	go serveEcho(echoListener)
	proxyPort := reserveTCPPort(t)
	instance := startReloadTestBox(t, reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
	defer instance.Close()

	proxyDialer := socks.NewClient(N.SystemDialer, M.ParseSocksaddrHostPort("127.0.0.1", proxyPort), socks.Version5, "", "")
	conn, err := proxyDialer.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr(echoListener.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	transferDone := make(chan error, 1)
	go func() {
		payload := make([]byte, 4096)
		for index := range payload {
			payload[index] = byte(index)
		}
		response := make([]byte, len(payload))
		for iteration := range 1024 {
			if _, writeErr := conn.Write(payload); writeErr != nil {
				transferDone <- writeErr
				return
			}
			if _, readErr := io.ReadFull(conn, response); readErr != nil {
				transferDone <- readErr
				return
			}
			if !bytes.Equal(response, payload) {
				transferDone <- io.ErrUnexpectedEOF
				return
			}
			if iteration == 10 {
				close(started)
			}
		}
		transferDone <- nil
	}()
	select {
	case <-started:
	case err = <-transferDone:
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("transfer finished before reload could overlap it")
	case <-time.After(5 * time.Second):
		t.Fatal("active transfer did not start")
	}
	if err = instance.Reload(reloadTestOptions(proxyPort, C.TypeBlock, "blocked")); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-transferDone:
		if err != nil {
			t.Fatal("active transfer was interrupted by reload: ", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("active transfer stalled after reload")
	}
}

func TestConcurrentReloadRequestsAreSerialized(t *testing.T) {
	proxyPort := reserveTCPPort(t)
	instance := startReloadTestBox(t, reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
	defer instance.Close()

	var waitGroup sync.WaitGroup
	reloadErrors := make(chan error, 40)
	for index := range 40 {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			if index%2 == 0 {
				reloadErrors <- instance.Reload(reloadTestOptions(proxyPort, C.TypeBlock, "blocked"))
			} else {
				reloadErrors <- instance.Reload(reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
			}
		}(index)
	}
	waitGroup.Wait()
	close(reloadErrors)
	for reloadErr := range reloadErrors {
		if reloadErr != nil {
			t.Fatal(reloadErr)
		}
	}
	if err := instance.Reload(reloadTestOptions(proxyPort, C.TypeDirect, "direct")); err != nil {
		t.Fatal(err)
	}
}

func TestSmartInlineProviderStartsWithLeafCandidates(t *testing.T) {
	proxyPort := reserveTCPPort(t)
	options := option.Options{
		Log: &option.LogOptions{Disabled: true},
		Inbounds: []option.Inbound{{
			Type: C.TypeMixed,
			Tag:  "mixed",
			Options: &option.HTTPMixedInboundOptions{ListenOptions: option.ListenOptions{
				Listen: common.Ptr(badoption.Addr(netip.MustParseAddr("127.0.0.1"))), ListenPort: proxyPort,
			}},
		}},
		Providers: []option.Provider{{
			Type: C.ProviderTypeInline,
			Tag:  "pool",
			Options: &option.ProviderInlineOptions{Outbounds: []option.Outbound{
				{Type: C.TypeDirect, Tag: "a", Options: &option.DirectOutboundOptions{}},
				{Type: C.TypeDirect, Tag: "b", Options: &option.DirectOutboundOptions{}},
			}},
		}},
		Outbounds: []option.Outbound{{
			Type: C.TypeSmart,
			Tag:  "smart",
			Options: &option.SmartOutboundOptions{
				GroupCommonOption: option.GroupCommonOption{Providers: []string{"pool"}},
				URL:               "http://127.0.0.1:1",
				ProbeTimeout:      badoption.Duration(100 * time.Millisecond),
				ProbeInterval:     badoption.Duration(time.Hour),
				HistoryPath:       filepath.Join(t.TempDir(), "history.json"),
			},
		}},
		Route: &option.RouteOptions{Final: "smart"},
	}
	instance := startReloadTestBox(t, options)
	defer instance.Close()
	lease, err := adapter.AcquireGeneration(instance.Outbound())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	smartOutbound, loaded := instance.Outbound().Outbound("smart")
	if !loaded {
		t.Fatal("smart outbound not found")
	}
	smartGroup, loaded := smartOutbound.(adapter.SmartGroup)
	if !loaded {
		t.Fatalf("unexpected outbound type: %T", smartOutbound)
	}
	candidates := smartGroup.All()
	if len(candidates) != 2 || candidates[0] != "pool/a" || candidates[1] != "pool/b" {
		t.Fatalf("unexpected provider leaf tags: %v", candidates)
	}
}

func startReloadTestBox(t *testing.T, options option.Options) *box.Box {
	t.Helper()
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	t.Cleanup(cancel)
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	if err != nil {
		t.Fatal(err)
	}
	if err = instance.Start(); err != nil {
		_ = instance.Close()
		t.Fatal(err)
	}
	return instance
}

func reloadTestOptions(proxyPort uint16, outboundType, final string) option.Options {
	var outboundOptions any = &option.DirectOutboundOptions{}
	if outboundType == C.TypeBlock {
		outboundOptions = &option.StubOptions{}
	}
	return option.Options{
		Log: &option.LogOptions{Disabled: true},
		Inbounds: []option.Inbound{{
			Type: C.TypeMixed,
			Tag:  "mixed",
			Options: &option.HTTPMixedInboundOptions{ListenOptions: option.ListenOptions{
				Listen: common.Ptr(badoption.Addr(netip.MustParseAddr("127.0.0.1"))), ListenPort: proxyPort,
			}},
		}},
		Outbounds: []option.Outbound{{Type: outboundType, Tag: final, Options: outboundOptions}},
		Route:     &option.RouteOptions{Final: final},
	}
}

func listenLocalTCP(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func reserveTCPPort(t *testing.T) uint16 {
	t.Helper()
	listener := listenLocalTCP(t)
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	_ = listener.Close()
	return port
}

func serveEcho(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}

func assertEcho(t *testing.T, conn net.Conn, payload string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != payload {
		t.Fatalf("unexpected echo %q", response)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}
