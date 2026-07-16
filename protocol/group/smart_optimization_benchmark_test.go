package group

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/control"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type smartBenchmarkNetworkManager struct {
	adapter.NetworkManager
	interfaceState *adapter.NetworkInterface
	wifi           adapter.WIFIState
}

func TestSmartNetworkFingerprintCacheExpires(t *testing.T) {
	manager := &smartBenchmarkNetworkManager{
		interfaceState: smartBenchmarkInterface(),
		wifi:           adapter.WIFIState{SSID: "first-network"},
	}
	smart := &Smart{network: manager}
	first := smart.networkFingerprint()
	manager.wifi.SSID = "second-network"
	if cached := smart.networkFingerprint(); cached != first {
		t.Fatal("network fingerprint cache expired before its TTL")
	}
	smart.fingerprint.Store(&smartFingerprintCache{value: first, expiresAt: 0})
	if refreshed := smart.networkFingerprint(); refreshed == first {
		t.Fatal("expired network fingerprint cache was not refreshed")
	}
}

func (m *smartBenchmarkNetworkManager) DefaultNetworkInterface() *adapter.NetworkInterface {
	return m.interfaceState
}

func (m *smartBenchmarkNetworkManager) WIFIState() adapter.WIFIState {
	return m.wifi
}

func smartBenchmarkSnapshot(metricCount int) smartStoreSnapshot {
	metrics := make([]smartPersistedMetric, metricCount)
	updatedAt := time.Unix(1_700_000_000, 0)
	for index := range metrics {
		metrics[index] = smartPersistedMetric{
			Network:   "network-benchmark",
			Site:      "site-" + strconv.Itoa(index/100),
			Candidate: "candidate-" + strconv.Itoa(index%1000),
			Transport: "tcp",
			smartMetric: smartMetric{
				Successes:      8,
				Failures:       1,
				ConnectMS:      50,
				FirstByteMS:    80,
				ThroughputLog:  12,
				ConnectSamples: 9,
				LastUpdated:    updatedAt.Add(time.Duration(index) * time.Second),
			},
		}
	}
	return smartStoreSnapshot{Version: smartStateVersion, Metrics: metrics}
}

func BenchmarkSmartStoreRestore50K(b *testing.B) {
	snapshot := smartBenchmarkSnapshot(50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		store := newSmartStore(time.Hour, 3, time.Minute)
		store.restore(snapshot)
		if len(store.metrics) != len(snapshot.Metrics) {
			b.Fatal("history restore lost metrics")
		}
	}
	b.ReportMetric(float64(unsafe.Sizeof(smartMetricKey{})), "key-bytes")
	b.ReportMetric(float64(unsafe.Sizeof(smartMetric{})), "metric-bytes")
	b.ReportMetric(float64(unsafe.Sizeof(smartPersistedMetric{})), "persisted-metric-bytes")
}

func BenchmarkSmartStoreSnapshot50K(b *testing.B) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	store.restore(smartBenchmarkSnapshot(50_000))
	now := time.Unix(1_700_100_000, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		snapshot := store.snapshot(now, 365*24*time.Hour, 50_000)
		if len(snapshot.Metrics) != 50_000 {
			b.Fatal("history snapshot lost metrics")
		}
	}
}

func BenchmarkSmartNetworkFingerprint(b *testing.B) {
	interfaceState := smartBenchmarkInterface()
	wifi := adapter.WIFIState{SSID: "benchmark", BSSID: "02:00:00:00:00:01"}
	b.ReportAllocs()
	for b.Loop() {
		if smartNetworkFingerprint(interfaceState, wifi) == "" {
			b.Fatal("empty network fingerprint")
		}
	}
}

func BenchmarkSmartCachedNetworkFingerprint(b *testing.B) {
	manager := &smartBenchmarkNetworkManager{
		interfaceState: smartBenchmarkInterface(),
		wifi:           adapter.WIFIState{SSID: "benchmark", BSSID: "02:00:00:00:00:01"},
	}
	smart := &Smart{network: manager}
	b.ReportAllocs()
	for b.Loop() {
		if smart.networkFingerprint() == "" {
			b.Fatal("empty cached network fingerprint")
		}
	}
}

func BenchmarkSmartDataPathRankCandidatePools(b *testing.B) {
	for _, candidateCount := range []int{100, 500, 1000} {
		b.Run(strconv.Itoa(candidateCount), func(b *testing.B) {
			candidates := make([]adapter.Outbound, 0, candidateCount)
			for index := range candidateCount {
				candidates = append(candidates, newSmartFakeOutbound("candidate-"+strconv.Itoa(index), nil))
			}
			smart := newTestSmart(candidates...)
			destination := M.ParseSocksaddr("benchmark.example:443")
			warm, _, _, _ := smart.rankPooled(context.Background(), N.NetworkTCP, destination)
			warm.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ranking, _, _, _ := smart.rankPooled(context.Background(), N.NetworkTCP, destination)
				if len(ranking.ranks) != candidateCount {
					b.Fatal("candidate count changed")
				}
				ranking.Release()
			}
		})
	}
}

func smartBenchmarkInterface() *adapter.NetworkInterface {
	return &adapter.NetworkInterface{
		Interface: control.Interface{
			Name:         "eth0",
			Index:        2,
			HardwareAddr: net.HardwareAddr{0x02, 0x42, 0xac, 0x11, 0x00, 0x02},
			MTU:          1500,
			Addresses: []netip.Prefix{
				netip.MustParsePrefix("10.0.0.2/24"),
				netip.MustParsePrefix("2001:db8::2/64"),
			},
		},
		Type:       C.InterfaceTypeEthernet,
		DNSServers: []string{"1.1.1.1", "8.8.8.8"},
	}
}
