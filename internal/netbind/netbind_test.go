package netbind

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestMediaURLAndPeerRestriction(t *testing.T) {
	e := Endpoint{Network: "lan", Address: "192.168.39.11:8091", IP: netip.MustParseAddr("192.168.39.11"), Allowed: netip.MustParsePrefix("192.168.39.0/24")}
	if got := e.MediaURL("/ws/stream"); got != "ws://192.168.39.11:8091/ws/stream" {
		t.Fatalf("URL: %q", got)
	}
	handler := RestrictPeer(e, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, tc := range []struct {
		remote string
		want   int
	}{{"192.168.39.30:5555", 204}, {"192.168.40.1:5555", 403}, {"127.0.0.1:5555", 403}} {
		r := httptest.NewRequest("GET", "http://localhost/healthz", nil)
		r.RemoteAddr = tc.remote
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("remote %s => %d, want %d", tc.remote, w.Code, tc.want)
		}
	}
}
func TestRejectWildcard(t *testing.T) {
	if _, err := Discover("0.0.0.0", 8091); err == nil {
		t.Fatal("wildcard listener accepted")
	}
}
