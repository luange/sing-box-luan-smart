package service

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestService struct {
	adapter.Service
	closeCount atomic.Int32
	closeErr   error
}

func (*managerTestService) Type() string { return "test" }
func (*managerTestService) Tag() string  { return "test" }
func (s *managerTestService) Close() error {
	s.closeCount.Add(1)
	return s.closeErr
}

func TestManagerCloseBeforeStartReturnsServiceErrors(t *testing.T) {
	expected := errors.New("close failed")
	service := &managerTestService{closeErr: expected}
	manager := &Manager{
		logger:   log.NewNOPFactory().NewLogger("service"),
		services: []adapter.Service{service},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("service close error was lost: %v", err)
	}
	if service.closeCount.Load() != 1 {
		t.Fatalf("service closed %d times", service.closeCount.Load())
	}
}
