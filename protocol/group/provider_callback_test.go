package group

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/x/list"
)

func TestProviderGroupsUnregisterCallbacksOnClose(t *testing.T) {
	provider := &smartFakeProvider{tag: "pool"}
	tests := []struct {
		name  string
		close func() error
	}{
		{
			name: "selector",
			close: func() error {
				group := &Selector{
					providers:       map[string]adapter.Provider{"pool": provider},
					providerHandles: make(map[string]*list.Element[adapter.ProviderUpdateCallback]),
				}
				group.providerHandles["pool"] = provider.RegisterCallback(group.onProviderUpdated)
				return group.Close()
			},
		},
		{
			name: "urltest",
			close: func() error {
				group := &URLTest{
					providers:       map[string]adapter.Provider{"pool": provider},
					providerHandles: make(map[string]*list.Element[adapter.ProviderUpdateCallback]),
				}
				group.providerHandles["pool"] = provider.RegisterCallback(group.onProviderUpdated)
				return group.Close()
			},
		},
		{
			name: "loadbalance",
			close: func() error {
				group := &LoadBalance{
					providers:       map[string]adapter.Provider{"pool": provider},
					providerHandles: make(map[string]*list.Element[adapter.ProviderUpdateCallback]),
				}
				group.providerHandles["pool"] = provider.RegisterCallback(group.onProviderUpdated)
				return group.Close()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := provider.callbackCount()
			if err := test.close(); err != nil {
				t.Fatal(err)
			}
			if provider.callbackCount() != before {
				t.Fatalf("provider callback leaked: before=%d after=%d", before, provider.callbackCount())
			}
		})
	}
}
