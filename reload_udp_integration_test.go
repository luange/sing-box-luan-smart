//go:build !race

package box_test

import (
	"context"
	"net"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
)

// The upstream sing/common canceler currently races inside the real SOCKS UDP
// copy path under -race. Generation packet leases remain covered by the race
// tests in adapter/generation.
func TestGracefulReloadKeepsEstablishedUDP(t *testing.T) {
	echoListener := listenLocalUDP(t)
	defer echoListener.Close()
	go serveUDPEcho(echoListener)
	proxyPort := reserveTCPPort(t)
	instance := startReloadTestBox(t, reloadTestOptions(proxyPort, C.TypeDirect, "direct"))
	defer instance.Close()

	proxyDialer := socks.NewClient(
		N.SystemDialer,
		M.ParseSocksaddrHostPort("127.0.0.1", proxyPort),
		socks.Version5,
		"",
		"",
	)
	destination := M.ParseSocksaddr(echoListener.LocalAddr().String())
	oldPacketConn, err := proxyDialer.ListenPacket(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer oldPacketConn.Close()
	assertPacketEcho(t, oldPacketConn, "before reload")

	if err = instance.Reload(reloadTestOptions(proxyPort, C.TypeBlock, "blocked")); err != nil {
		t.Fatal(err)
	}
	assertPacketEcho(t, oldPacketConn, "after reload")

	newPacketConn, err := proxyDialer.ListenPacket(context.Background(), destination)
	if err == nil {
		defer newPacketConn.Close()
		if err = newPacketConn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if writer, loaded := newPacketConn.(interface{ Write([]byte) (int, error) }); loaded {
			_, _ = writer.Write([]byte("new generation"))
			response := make([]byte, 64*1024)
			if n, _, readErr := newPacketConn.ReadFrom(response); readErr == nil {
				t.Fatalf("new UDP session unexpectedly used retired generation: %q", response[:n])
			}
		}
	}
}

func listenLocalUDP(t *testing.T) net.PacketConn {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func serveUDPEcho(listener net.PacketConn) {
	buffer := make([]byte, 64*1024)
	for {
		n, source, err := listener.ReadFrom(buffer)
		if err != nil {
			return
		}
		if _, err = listener.WriteTo(buffer[:n], source); err != nil {
			return
		}
	}
}

func assertPacketEcho(t *testing.T, conn net.PacketConn, payload string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connectedWriter, loaded := conn.(interface{ Write([]byte) (int, error) })
	if !loaded {
		t.Fatal("packet connection is not connected to its destination")
	}
	if _, err := connectedWriter.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 64*1024)
	n, _, err := conn.ReadFrom(response)
	if err != nil {
		t.Fatal(err)
	}
	if string(response[:n]) != payload {
		t.Fatalf("unexpected packet echo %q", response[:n])
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}
