package certificate

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

type managerTestCertificateProvider struct {
	adapter.CertificateProviderService
	closeCount atomic.Int32
	closeErr   error
}

func (*managerTestCertificateProvider) Type() string { return "test" }
func (*managerTestCertificateProvider) Tag() string  { return "test" }
func (p *managerTestCertificateProvider) Close() error {
	p.closeCount.Add(1)
	return p.closeErr
}

func TestManagerCloseBeforeStartReturnsCertificateProviderErrors(t *testing.T) {
	expected := errors.New("close failed")
	provider := &managerTestCertificateProvider{closeErr: expected}
	manager := &Manager{
		logger:    log.NewNOPFactory().NewLogger("certificate-provider"),
		providers: []adapter.CertificateProviderService{provider},
	}
	if err := manager.Close(); !errors.Is(err, expected) {
		t.Fatalf("certificate provider close error was lost: %v", err)
	}
	if provider.closeCount.Load() != 1 {
		t.Fatalf("certificate provider closed %d times", provider.closeCount.Load())
	}
}
