package group

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	N "github.com/sagernet/sing/common/network"
)

func TestSmartReliabilityUsesConfidence(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(1000, 0)
	initial := store.estimate(now, "eth0", "", "a", "tcp", 3)
	store.observeDial(now, "eth0", "", "a", "tcp", true, 100*time.Millisecond)
	oneSuccess := store.estimate(now, "eth0", "", "a", "tcp", 3)
	for range 20 {
		store.observeDial(now, "eth0", "", "a", "tcp", true, 100*time.Millisecond)
	}
	manySuccesses := store.estimate(now, "eth0", "", "a", "tcp", 3)
	if !(initial.Reliability < oneSuccess.Reliability && oneSuccess.Reliability < manySuccesses.Reliability) {
		t.Fatalf("unexpected reliability ordering: initial=%f one=%f many=%f", initial.Reliability, oneSuccess.Reliability, manySuccesses.Reliability)
	}
}

func TestSmartHistorySchemaRemainsBackwardCompatible(t *testing.T) {
	oldHistory := []byte(`{"version":1,"metrics":[{"network":"ethernet","site":"example.com","candidate":"a","transport":"tcp","successes":8,"failures":1,"connect_ms":42,"connect_samples":8,"last_updated":"2026-07-15T00:00:00Z"}]}`)
	var snapshot smartStoreSnapshot
	if err := json.Unmarshal(oldHistory, &snapshot); err != nil {
		t.Fatal(err)
	}
	store := newSmartStore(time.Hour, 3, time.Minute)
	store.restore(snapshot)
	if state := store.estimate(time.Date(2026, 7, 15, 0, 1, 0, 0, time.UTC), "ethernet", "example.com", "a", "tcp", 3); !state.HasConnect || state.ConnectMS <= 0 {
		t.Fatalf("old history was not restored: %+v", state)
	}
	written, err := json.Marshal(store.snapshot(time.Date(2026, 7, 15, 0, 1, 0, 0, time.UTC), time.Hour, 50000))
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip map[string]any
	if err = json.Unmarshal(written, &roundTrip); err != nil {
		t.Fatal(err)
	}
	metrics := roundTrip["metrics"].([]any)
	restored := metrics[0].(map[string]any)
	for _, field := range []string{"network", "site", "candidate", "transport", "successes", "connect_ms", "last_updated"} {
		if _, loaded := restored[field]; !loaded {
			t.Fatalf("Smart.4 history omitted legacy field %q: %s", field, written)
		}
	}
}

func TestSmartHistoryDirtyRevisionDoesNotLoseConcurrentObservation(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	if store.needsFlush() {
		t.Fatal("empty history started dirty")
	}
	now := time.Unix(1000, 0)
	store.observeDial(now, "ethernet", "", "a", "tcp", true, 10*time.Millisecond)
	if !store.needsFlush() {
		t.Fatal("observation did not mark history dirty")
	}
	first := store.snapshot(now, time.Hour, 50000)
	store.observeDial(now.Add(time.Second), "ethernet", "", "b", "tcp", true, 20*time.Millisecond)
	store.markFlushed(first.revision)
	if !store.needsFlush() {
		t.Fatal("flushing an older revision lost a concurrent observation")
	}
	second := store.snapshot(now.Add(time.Second), time.Hour, 50000)
	store.markFlushed(second.revision)
	if store.needsFlush() {
		t.Fatal("latest successful flush left history dirty")
	}
}

func TestSmartBreakerHalfOpenAndRecovery(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(2000, 0)
	for range 3 {
		store.observeDial(now, "eth0", "example.com", "a", "tcp", false, time.Second)
	}
	open := store.estimate(now, "eth0", "example.com", "a", "tcp", 3)
	if open.State != "open" {
		t.Fatalf("expected open, got %s", open.State)
	}
	halfOpen := store.estimate(now.Add(time.Minute+time.Second), "eth0", "example.com", "a", "tcp", 3)
	if halfOpen.State != "half_open" {
		t.Fatalf("expected half_open, got %s", halfOpen.State)
	}
	store.observeDial(now.Add(time.Minute+time.Second), "eth0", "example.com", "a", "tcp", true, 80*time.Millisecond)
	recovered := store.estimate(now.Add(time.Minute+time.Second), "eth0", "example.com", "a", "tcp", 3)
	if recovered.State == "open" || recovered.State == "half_open" {
		t.Fatalf("expected recovered state, got %s", recovered.State)
	}
}

func TestSmartSiteHistoryOverridesGlobalGradually(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(3000, 0)
	for range 20 {
		store.observeDial(now, "eth0", "", "a", "tcp", true, 50*time.Millisecond)
	}
	for range 6 {
		store.observeDial(now, "eth0", "video.example", "a", "tcp", false, time.Second)
	}
	global := store.estimate(now, "eth0", "", "a", "tcp", 3)
	site := store.estimate(now, "eth0", "video.example", "a", "tcp", 3)
	if site.Reliability >= global.Reliability {
		t.Fatalf("site reliability should be lower: site=%f global=%f", site.Reliability, global.Reliability)
	}
}

func TestSmartThroughputAffectsScore(t *testing.T) {
	slow := smartEstimate{
		Reliability:   0.99,
		ConnectMS:     80,
		ThroughputBPS: 512 * 1024,
		Samples:       10,
		State:         "healthy",
		HasConnect:    true,
		HasThroughput: true,
	}
	fast := slow
	fast.ThroughputBPS = 32 * 1024 * 1024
	if smartScoreForProfile(fast, smartProfileBulk, 0, 20) >= smartScoreForProfile(slow, smartProfileBulk, 0, 20) {
		t.Fatal("faster candidate should have a lower score")
	}
}

func TestSmartSiteFailuresDoNotOpenGlobalCircuit(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(4000, 0)
	for range 3 {
		store.observeDial(now, "wifi", "bank.example", "a", "tcp", false, time.Second)
	}
	global := store.estimate(now, "wifi", "", "a", "tcp", 3)
	site := store.estimate(now, "wifi", "bank.example", "a", "tcp", 3)
	if global.State == "open" {
		t.Fatal("site-specific failures opened the global circuit")
	}
	if site.State != "open" {
		t.Fatalf("expected the site circuit to open, got %s", site.State)
	}
}

func TestSmartTCPAndUDPStateAreIndependent(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(5000, 0)
	for range 3 {
		store.observeDial(now, "ethernet", "game.example", "a", "tcp", false, time.Second)
	}
	for range 6 {
		store.observeDial(now, "ethernet", "game.example", "a", "udp", true, 35*time.Millisecond)
	}
	tcp := store.estimate(now, "ethernet", "game.example", "a", "tcp", 3)
	udp := store.estimate(now, "ethernet", "game.example", "a", "udp", 3)
	if tcp.State != "open" {
		t.Fatalf("expected TCP circuit open, got %s", tcp.State)
	}
	if udp.State == "open" || udp.State == "half_open" {
		t.Fatalf("TCP failures contaminated UDP state: %s", udp.State)
	}
}

func TestSmartNetworkHistoryIsIndependent(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(6000, 0)
	for range 3 {
		store.observeDial(now, "wifi", "", "a", "tcp", false, time.Second)
	}
	wifi := store.estimate(now, "wifi", "", "a", "tcp", 3)
	ethernet := store.estimate(now, "ethernet", "", "a", "tcp", 3)
	if wifi.State != "open" {
		t.Fatalf("expected wifi circuit open, got %s", wifi.State)
	}
	if ethernet.State != "unknown" {
		t.Fatalf("new network inherited stale state: %s", ethernet.State)
	}
}

func TestSmartHierarchicalSamplesAreNotDoubleCounted(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(7000, 0)
	for range 5 {
		store.observeDial(now, "ethernet", "video.example", "a", "tcp", true, 50*time.Millisecond)
	}
	estimate := store.estimate(now, "ethernet", "video.example", "a", "tcp", 3)
	if estimate.Samples != 5 {
		t.Fatalf("hierarchical samples were double counted: %f", estimate.Samples)
	}
}

func TestSmartTrafficProfilesPreferDifferentCandidates(t *testing.T) {
	lowLatency := smartEstimate{
		Reliability:       0.98,
		ConnectMS:         25,
		FirstByteMS:       55,
		ThroughputBPS:     512 * 1024,
		ThroughputSamples: 4,
		Samples:           20,
		State:             "healthy",
		HasConnect:        true,
		HasFirstByte:      true,
		HasThroughput:     true,
	}
	highThroughput := lowLatency
	highThroughput.ConnectMS = 130
	highThroughput.FirstByteMS = 180
	highThroughput.ThroughputBPS = 48 * 1024 * 1024
	if smartScoreForProfile(lowLatency, smartProfileInteractive, 0, 40) >= smartScoreForProfile(highThroughput, smartProfileInteractive, 0, 40) {
		t.Fatal("interactive profile should prefer the lower-latency candidate")
	}
	if smartScoreForProfile(highThroughput, smartProfileBulk, 0, 40) >= smartScoreForProfile(lowLatency, smartProfileBulk, 0, 40) {
		t.Fatal("bulk profile should prefer the higher-throughput candidate")
	}
}

func TestSmartDetectsBulkProfileOnlyAfterUsefulSamples(t *testing.T) {
	estimates := map[string]smartEstimate{
		"a": {ThroughputSamples: 1},
		"b": {},
	}
	if profile := detectSmartTrafficProfile("tcp", estimates); profile != smartProfileInteractive {
		t.Fatalf("single throughput sample changed the profile: %s", profile)
	}
	estimate := estimates["a"]
	estimate.ThroughputSamples = 2
	estimates["a"] = estimate
	if profile := detectSmartTrafficProfile("tcp", estimates); profile != smartProfileBulk {
		t.Fatalf("expected bulk profile, got %s", profile)
	}
	if profile := detectSmartTrafficProfile("udp", estimates); profile != smartProfileUDP {
		t.Fatalf("expected UDP profile, got %s", profile)
	}
}

func TestSmartHistorySnapshotRoundTrip(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(8000, 0)
	for range 5 {
		store.observeDial(now, "ethernet", "video.example", "a", "tcp", true, 45*time.Millisecond)
	}
	store.observeFirstByte(now, "ethernet", "video.example", "a", "tcp", 80*time.Millisecond)
	store.observeThroughput(now, "ethernet", "video.example", "a", "tcp", 32*1024*1024, 2*time.Second)
	snapshot := store.snapshot(now, 24*time.Hour, 100)
	if snapshot.Version != smartStateVersion || len(snapshot.Metrics) == 0 {
		t.Fatal("history snapshot is empty")
	}

	restored := newSmartStore(time.Hour, 3, time.Minute)
	restored.restore(snapshot)
	estimate := restored.estimate(now, "ethernet", "video.example", "a", "tcp", 3)
	if estimate.State != "healthy" {
		t.Fatalf("restored state mismatch: %s", estimate.State)
	}
	if !estimate.HasFirstByte || !estimate.HasThroughput {
		t.Fatal("restored observations are incomplete")
	}

	rejected := newSmartStore(time.Hour, 3, time.Minute)
	snapshot.Version++
	rejected.restore(snapshot)
	if estimate := rejected.estimate(now, "ethernet", "video.example", "a", "tcp", 3); estimate.State != "unknown" {
		t.Fatalf("unsupported history schema was accepted: %s", estimate.State)
	}
}

func TestSmartHistorySnapshotDoesNotApplyDecayTwice(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	observedAt := time.Unix(8500, 0)
	for range 20 {
		store.observeDial(observedAt, "ethernet", "video.example", "a", "tcp", true, 45*time.Millisecond)
	}
	snapshotAt := observedAt.Add(time.Hour)
	expected := store.estimate(snapshotAt, "ethernet", "video.example", "a", "tcp", 3)

	restored := newSmartStore(time.Hour, 3, time.Minute)
	restored.restore(store.snapshot(snapshotAt, 24*time.Hour, 100))
	actual := restored.estimate(snapshotAt, "ethernet", "video.example", "a", "tcp", 3)
	if diff := expected.Samples - actual.Samples; diff < -0.000001 || diff > 0.000001 {
		t.Fatalf("snapshot applied decay twice: expected samples=%f actual=%f", expected.Samples, actual.Samples)
	}
	if diff := expected.Reliability - actual.Reliability; diff < -0.000001 || diff > 0.000001 {
		t.Fatalf("snapshot changed reliability: expected=%f actual=%f", expected.Reliability, actual.Reliability)
	}
}

func TestSmartHistorySnapshotHonorsRetentionAndLimit(t *testing.T) {
	store := newSmartStore(time.Hour, 3, time.Minute)
	now := time.Unix(9000, 0)
	store.observeDial(now.Add(-2*time.Hour), "ethernet", "", "old", "tcp", true, time.Millisecond)
	store.observeDial(now.Add(-time.Minute), "ethernet", "", "new-a", "tcp", true, time.Millisecond)
	store.observeDial(now, "ethernet", "", "new-b", "tcp", true, time.Millisecond)
	snapshot := store.snapshot(now, time.Hour, 1)
	if len(snapshot.Metrics) != 1 {
		t.Fatalf("expected one retained metric, got %d", len(snapshot.Metrics))
	}
	if snapshot.Metrics[0].Candidate != "new-b" {
		t.Fatalf("snapshot did not keep the newest metric: %s", snapshot.Metrics[0].Candidate)
	}
}

func TestSmartWorkerStartsOnlyAfterGenerationPublish(t *testing.T) {
	smart := &Smart{
		ctx:               context.Background(),
		store:             newSmartStore(time.Hour, 3, time.Minute),
		probeInterval:     time.Hour,
		probeTimeout:      time.Millisecond,
		halfLife:          time.Hour,
		breakerFailures:   3,
		breakerCooldown:   time.Minute,
		historyPath:       filepath.Join(t.TempDir(), "history.json"),
		historyRetention:  time.Hour,
		maxHistoryEntries: 100,
	}
	if err := smart.PostStart(); err != nil {
		t.Fatal(err)
	}
	smart.lifecycleAccess.Lock()
	startedBeforePublish := smart.workerStarted
	smart.lifecycleAccess.Unlock()
	if startedBeforePublish {
		t.Fatal("Smart worker started during prepare")
	}
	smart.OnGenerationPublish()
	smart.lifecycleAccess.Lock()
	startedAfterPublish := smart.workerStarted
	smart.lifecycleAccess.Unlock()
	if !startedAfterPublish {
		t.Fatal("Smart worker did not start after publish")
	}
	smart.OnGenerationRetire()
	if err := smart.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartHistoryStoreSharedAcrossPublishedGenerations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	first := newSmartHistoryTestInstance(path)
	second := newSmartHistoryTestInstance(path)
	first.OnGenerationPublish()
	first.store.observeDial(time.Now(), "network", "", "candidate-a", "tcp", true, time.Millisecond)
	second.OnGenerationPublish()
	if first.store != second.store {
		t.Fatal("published generations do not share the same history store")
	}
	estimate := second.store.estimate(time.Now(), "network", "", "candidate-a", "tcp", 1)
	if estimate.State == "unknown" {
		t.Fatal("new generation cannot see observations from the previous generation")
	}
	first.OnGenerationRetire()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := decodeSmartHistoryTestSnapshot(t, content, "SMART-TEST")
	if len(snapshot.Metrics) == 0 {
		t.Fatal("shared history was not persisted")
	}
}

func TestSmartControlStateSharedOnlyAfterPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	first := newSmartHistoryTestInstanceWithTag(path, "SMART")
	second := newSmartHistoryTestInstanceWithTag(path, "SMART")
	first.OnGenerationPublish()
	setSmartTestControl(first, "candidate-a", "candidate-b")
	setSmartTestControl(second, "prepare-pin", "prepare-temporary")
	if first.control == second.control {
		t.Fatal("prepare generation shared control before publish")
	}
	second.OnGenerationPublish()
	if first.control != second.control {
		t.Fatal("published generations do not share control state")
	}
	pinned, temporary := readSmartTestControl(second)
	if pinned != "candidate-a" || temporary != "candidate-b" {
		t.Fatalf("publish did not preserve current control: pinned=%q temporary=%q", pinned, temporary)
	}
	setSmartTestControl(first, "candidate-b", "candidate-a")
	pinned, temporary = readSmartTestControl(second)
	if pinned != "candidate-b" || temporary != "candidate-a" {
		t.Fatalf("control update was not visible across generations: pinned=%q temporary=%q", pinned, temporary)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartControlStateIsolatedByTag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	first := newSmartHistoryTestInstanceWithTag(path, "SMART-A")
	second := newSmartHistoryTestInstanceWithTag(path, "SMART-B")
	first.OnGenerationPublish()
	second.OnGenerationPublish()
	if first.control == second.control {
		t.Fatal("different Smart tags shared control state")
	}
	setSmartTestControl(first, "candidate-a", "candidate-b")
	pinned, temporary := readSmartTestControl(second)
	if pinned != "" || temporary != "" {
		t.Fatalf("control leaked across Smart tags: pinned=%q temporary=%q", pinned, temporary)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartHistoryStoreAndPolicyAreIsolatedByTag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	first := newSmartHistoryTestInstanceWithTag(path, "SMART-A")
	second := newSmartHistoryTestInstanceWithTag(path, "SMART-B")
	second.halfLife = 2 * time.Hour
	second.breakerFailures = 7
	second.breakerCooldown = 5 * time.Minute
	first.OnGenerationPublish()
	second.OnGenerationPublish()
	if first.store == second.store {
		t.Fatal("different Smart tags shared a metric store")
	}
	first.store.access.RLock()
	firstHalfLife := first.store.halfLife
	firstFailures := first.store.breakerFailures
	first.store.access.RUnlock()
	second.store.access.RLock()
	secondHalfLife := second.store.halfLife
	secondFailures := second.store.breakerFailures
	second.store.access.RUnlock()
	if firstHalfLife != time.Hour || firstFailures != 3 {
		t.Fatalf("first policy was contaminated: half_life=%v failures=%d", firstHalfLife, firstFailures)
	}
	if secondHalfLife != 2*time.Hour || secondFailures != 7 {
		t.Fatalf("second policy was not applied: half_life=%v failures=%d", secondHalfLife, secondFailures)
	}
	first.store.observeDial(time.Now(), "network", "", "candidate-a", "tcp", true, time.Millisecond)
	second.store.observeDial(time.Now(), "network", "", "candidate-b", "tcp", true, time.Millisecond)
	first.flushHistory()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var historyFile smartHistoryFile
	if err = json.Unmarshal(content, &historyFile); err != nil {
		t.Fatal(err)
	}
	if historyFile.Version != smartHistoryFileVersion || len(historyFile.Groups) != 2 {
		t.Fatalf("multi-group history was not preserved: %+v", historyFile)
	}
	if len(historyFile.Groups["SMART-A"].Metrics) == 0 || len(historyFile.Groups["SMART-B"].Metrics) == 0 {
		t.Fatalf("group metrics missing from history: %+v", historyFile.Groups)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartControlSnapshotDropsRemovedCandidates(t *testing.T) {
	smart := newSmartHistoryTestInstanceWithTag(filepath.Join(t.TempDir(), "history.json"), "SMART")
	smart.candidateByTag = map[string]adapter.Outbound{"candidate-a": nil}
	setSmartTestControl(smart, "removed-pin", "removed-temporary")
	pinned, temporary, _, _ := smart.controlSnapshot(time.Now())
	if pinned != "" || temporary != "" {
		t.Fatalf("removed candidate control survived: pinned=%q temporary=%q", pinned, temporary)
	}
}

func TestSmartHistoryConcurrentFlushUsesAtomicFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	first := newSmartHistoryTestInstance(path)
	second := newSmartHistoryTestInstance(path)
	first.OnGenerationPublish()
	second.OnGenerationPublish()
	first.store.observeDial(time.Now(), "network", "", "candidate-a", "tcp", true, time.Millisecond)
	var waitGroup sync.WaitGroup
	for range 20 {
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			first.flushHistory()
		}()
		go func() {
			defer waitGroup.Done()
			second.flushHistory()
		}()
	}
	waitGroup.Wait()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = decodeSmartHistoryTestSnapshot(t, content, "SMART-TEST")
	pattern := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if matches, err := filepath.Glob(pattern); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("temporary history files leaked: %v", matches)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnpublishedSmartDoesNotWriteHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	smart := newSmartHistoryTestInstance(path)
	smart.store.observeDial(time.Now(), "network", "", "candidate-a", "tcp", true, time.Millisecond)
	if err := smart.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unpublished candidate wrote history: %v", err)
	}
}

func newSmartHistoryTestInstance(path string) *Smart {
	return newSmartHistoryTestInstanceWithTag(path, "SMART-TEST")
}

func newSmartHistoryTestInstanceWithTag(path string, tag string) *Smart {
	return &Smart{
		Adapter:           outbound.NewAdapter(C.TypeSmart, tag, []string{N.NetworkTCP, N.NetworkUDP}, nil),
		ctx:               context.Background(),
		control:           &smartControlState{},
		candidateByTag:    make(map[string]adapter.Outbound),
		store:             newSmartStore(time.Hour, 3, time.Minute),
		probeInterval:     time.Hour,
		probeTimeout:      time.Millisecond,
		halfLife:          time.Hour,
		breakerFailures:   3,
		breakerCooldown:   time.Minute,
		historyPath:       path,
		historyRetention:  time.Hour,
		maxHistoryEntries: 100,
	}
}

func setSmartTestControl(smart *Smart, pinned string, temporary string) {
	smart.control.access.Lock()
	smart.control.pinned = pinned
	smart.control.temporary = temporary
	smart.control.temporaryUntil = time.Now().Add(time.Hour)
	smart.control.access.Unlock()
}

func readSmartTestControl(smart *Smart) (string, string) {
	smart.control.access.Lock()
	defer smart.control.access.Unlock()
	return smart.control.pinned, smart.control.temporary
}

func decodeSmartHistoryTestSnapshot(t *testing.T, content []byte, tag string) smartStoreSnapshot {
	t.Helper()
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(content, &header); err != nil {
		t.Fatal(err)
	}
	if header.Version == smartHistoryFileVersion {
		var historyFile smartHistoryFile
		if err := json.Unmarshal(content, &historyFile); err != nil {
			t.Fatal(err)
		}
		return historyFile.Groups[tag]
	}
	var snapshot smartStoreSnapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
