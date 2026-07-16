package endpoint

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestEndpoint struct {
	adapter.Endpoint
	closeCount atomic.Int32
	closeErr   error
	starts     [4]atomic.Int32
}

func (*managerTestEndpoint) Type() string { return "test" }
func (*managerTestEndpoint) Tag() string  { return "test" }
func (e *managerTestEndpoint) Start(stage adapter.StartStage) error {
	e.starts[stage].Add(1)
	return nil
}
func (e *managerTestEndpoint) Close() error {
	e.closeCount.Add(1)
	return e.closeErr
}

func TestManagerCreateStartsOnlyThroughCurrentStage(t *testing.T) {
	registry := NewRegistry()
	var created *managerTestEndpoint
	Register[struct{}](registry, "test", func(context.Context, adapter.Router, log.ContextLogger, string, struct{}) (adapter.Endpoint, error) {
		created = new(managerTestEndpoint)
		return created, nil
	})
	manager := NewManager(log.NewNOPFactory().NewLogger("endpoint"), registry)
	manager.started = true
	manager.stage = adapter.StartStateStart
	if err := manager.Create(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "dynamic", "test", &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if created.starts[adapter.StartStateInitialize].Load() != 1 || created.starts[adapter.StartStateStart].Load() != 1 {
		t.Fatal("dynamic endpoint did not start through current stage")
	}
	if created.starts[adapter.StartStatePostStart].Load() != 0 || created.starts[adapter.StartStateStarted].Load() != 0 {
		t.Fatal("dynamic endpoint started future lifecycle stages")
	}
}

func TestManagerCloseBeforeStartReturnsEndpointErrors(t *testing.T) {
	expected := errors.New("close failed")
	endpoint := &managerTestEndpoint{closeErr: expected}
	manager := &Manager{
		logger:    log.NewNOPFactory().NewLogger("endpoint"),
		endpoints: []adapter.Endpoint{endpoint},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("endpoint close error was lost: %v", err)
	}
	if endpoint.closeCount.Load() != 1 {
		t.Fatalf("endpoint closed %d times", endpoint.closeCount.Load())
	}
}
