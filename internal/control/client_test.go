package control

import (
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/netbind"
	"strings"
	"testing"
)

func TestNoLoopbackAdvertised(t *testing.T) {
	endpoints := AdvertisedEndpoints([]netbind.Endpoint{
		{Network: "loopback", Address: "127.0.0.1:8091"},
		{Network: "lan", Address: "192.168.39.11:8091"},
		{Network: "tailscale", Address: "100.82.22.45:8091"},
	}, "/ws/stream")
	if len(endpoints) != 2 {
		t.Fatalf("expected 2 externally reachable addresses, got %+v", endpoints)
	}
	for _, ep := range endpoints {
		if strings.Contains(ep.URL, "127.0.0.1") || strings.Contains(ep.URL, "0.0.0.0") {
			t.Fatalf("unroutable %s", ep.URL)
		}
	}
}
