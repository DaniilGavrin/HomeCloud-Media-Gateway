package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
)

func TestHealthAndReservedMediaEndpoint(t *testing.T) {
	a := New(config.Config{Server: config.ServerConfig{WebSocketPath: "/ws/stream"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	checks := []struct {
		path     string
		code     int
		contains string
	}{
		{"/healthz", http.StatusOK, `"status":"alive"`},

		{"/not-found", http.StatusNotFound, "404"},
	}
	for _, c := range checks {
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
		if rec.Code != c.code || !strings.Contains(rec.Body.String(), c.contains) {
			t.Fatalf("%s: got %d %q", c.path, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws/stream", nil))
	if rec.Code == http.StatusOK || rec.Code == http.StatusNotImplemented {
		t.Fatalf("media route must require WS upgrade: %d", rec.Code)
	}
}
