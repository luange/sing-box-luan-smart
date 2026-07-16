package generation

import (
	"sync/atomic"
	"testing"
)

func BenchmarkManagerAcquireRelease(b *testing.B) {
	manager := NewManager()
	var closeCount atomic.Int64
	if _, err := manager.Publish(stressRuntime(&closeCount)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, lease, err := manager.Acquire()
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
	b.StopTimer()
	if err := manager.Close(); err != nil {
		b.Fatal(err)
	}
}
