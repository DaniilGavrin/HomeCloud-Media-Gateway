package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validYAML = `config_version: 1
server:
  host: "127.0.0.1"
  port: 8091
  websocket_path: "/ws/stream"
  shutdown_timeout: "10s"
backend:
  websocket_url: "ws://127.0.0.1:8080/ws"
heartbeat:
  interval: "20s"
  grace: "10s"
storage:
  cache_dir: "data/media-cache"
  prefetch_limit_bytes: 1073741824
  max_cache_bytes: 10737418240
limits:
  max_rooms: 4
`

func loadText(t *testing.T, text string) (Config, error) {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(filename)
}

func TestLoadValid(t *testing.T) {
	cfg, err := loadText(t, validYAML)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8091 || cfg.Heartbeat.Interval.Duration != 20*time.Second {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}

func TestStrictConfiguration(t *testing.T) {
	tests := []struct{ name, yaml, want string }{
		{"unknown section", validYAML + "unknown_field: true\n", "not found"},
		{"wrong version", strings.Replace(validYAML, "config_version: 1", "config_version: 2", 1), "config_version"},
		{"empty", "", "empty"},
		{"multi-document", validYAML + "---\nconfig_version: 1\n", "multiple YAML documents"},
		{"wildcard", strings.Replace(validYAML, "127.0.0.1\"", "0.0.0.0\"", 1), "server.host"},
		{"public IP", strings.Replace(validYAML, "127.0.0.1\"", "8.8.8.8\"", 1), "server.host"},
		{"bad path", strings.Replace(validYAML, "/ws/stream", "/ws/../stream", 1), "server.websocket_path"},
		{"invalid duration", strings.Replace(validYAML, "20s", "0s", 1), "heartbeat.interval"},
		{"wrong threshold", strings.Replace(validYAML, "1073741824", "1073741825", 1), "prefetch_limit_bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadText(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error with %q; got %v", tc.want, err)
			}
		})
	}
}
