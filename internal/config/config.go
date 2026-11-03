// Package config loads and validates startup-only YAML settings.
// Dynamic rooms, TV devices and playback state never belong in this package.
package config

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

const SupportedVersion = 1

var staticPath = regexp.MustCompile(`^/[a-zA-Z0-9/_-]+$`)
var tailscaleRange = netip.MustParsePrefix("100.64.0.0/10")

// Duration accepts Go-style duration strings such as "10s" and "2m".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		return fmt.Errorf("duration must be a string, e.g. \"10s\"")
	}
	value, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", n.Value, err)
	}
	d.Duration = value
	return nil
}

type Config struct {
	ConfigVersion int             `yaml:"config_version"`
	Server        ServerConfig    `yaml:"server"`
	Backend       BackendConfig   `yaml:"backend"`
	Heartbeat     HeartbeatConfig `yaml:"heartbeat"`
	Storage       StorageConfig   `yaml:"storage"`
	Limits        LimitsConfig    `yaml:"limits"`
}

type ServerConfig struct {
	Host            string   `yaml:"host"`
	Port            int      `yaml:"port"`
	WebSocketPath   string   `yaml:"websocket_path"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

type BackendConfig struct {
	WebSocketURL string `yaml:"websocket_url"`
}

type HeartbeatConfig struct {
	Interval Duration `yaml:"interval"`
	Grace    Duration `yaml:"grace"`
}

type StorageConfig struct {
	CacheDir           string `yaml:"cache_dir"`
	PrefetchLimitBytes int64  `yaml:"prefetch_limit_bytes"`
	MaxCacheBytes      int64  `yaml:"max_cache_bytes"`
}

type LimitsConfig struct {
	MaxRooms int `yaml:"max_rooms"`
}

func Load(filename string) (Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Config{}, fmt.Errorf("open config %q: %w", filename, err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return Config{}, errors.New("config is empty")
		}
		return Config{}, fmt.Errorf("decode config %q: %w", filename, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return Config{}, fmt.Errorf("read trailing YAML: %w", err)
		}
		return Config{}, errors.New("multiple YAML documents are not allowed")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.ConfigVersion != SupportedVersion {
		return fmt.Errorf("config_version must be %d (got %d)", SupportedVersion, c.ConfigVersion)
	}
	if err := validateBindHost(c.Server.Host); err != nil {
		return fmt.Errorf("server.host: %w", err)
	}
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}
	if !staticPath.MatchString(c.Server.WebSocketPath) ||
		path.Clean(c.Server.WebSocketPath) != c.Server.WebSocketPath ||
		strings.Contains(c.Server.WebSocketPath, "//") ||
		c.Server.WebSocketPath == "/" ||
		c.Server.WebSocketPath == "/healthz" {
		return fmt.Errorf("server.websocket_path must be a static path like /ws/stream")
	}
	if c.Server.ShutdownTimeout.Duration < time.Second || c.Server.ShutdownTimeout.Duration > 2*time.Minute {
		return fmt.Errorf("server.shutdown_timeout must be between 1s and 2m")
	}
	if err := validateBackendURL(c.Backend.WebSocketURL); err != nil {
		return fmt.Errorf("backend.websocket_url: %w", err)
	}
	if c.Heartbeat.Interval.Duration < 20*time.Second || c.Heartbeat.Interval.Duration > 30*time.Second {
		return fmt.Errorf("heartbeat.interval must be between 20s and 30s")
	}
	if c.Heartbeat.Grace.Duration != 10*time.Second {
		return fmt.Errorf("heartbeat.grace must equal 10s")
	}
	if err := validateCachePath(c.Storage.CacheDir); err != nil {
		return fmt.Errorf("storage.cache_dir: %w", err)
	}
	if c.Storage.PrefetchLimitBytes != 1<<30 {
		return fmt.Errorf("storage.prefetch_limit_bytes must equal 1073741824 (1 GiB)")
	}
	if c.Storage.MaxCacheBytes < c.Storage.PrefetchLimitBytes {
		return fmt.Errorf("storage.max_cache_bytes must be >= prefetch_limit_bytes")
	}
	if c.Limits.MaxRooms < 1 || c.Limits.MaxRooms > 128 {
		return fmt.Errorf("limits.max_rooms must be between 1 and 128")
	}
	return nil
}

func validateBindHost(value string) error {
	if value == "auto_private" {
		return nil
	}
	host, err := netip.ParseAddr(value)
	if err != nil {
		return errors.New("use an explicit IP address, e.g. 127.0.0.1, 192.168.x.x, or Tailscale IP")
	}
	if host.IsUnspecified() || host.IsMulticast() {
		return errors.New("wildcard or multicast listeners are not allowed")
	}
	if !(host.IsLoopback() || host.IsPrivate() || tailscaleRange.Contains(host)) {
		return errors.New("only loopback, private LAN or explicitly configured Tailscale IP may be bound")
	}
	return nil
}

func validateBackendURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" || u.Path == "/" {
		return errors.New("expected ws://host:port/ws or wss://host:port/ws without credentials, query or fragment")
	}
	return nil
}

func validateCachePath(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("path must not be empty")
	}
	cleaned := filepath.Clean(dir)
	if filepath.IsAbs(cleaned) || cleaned == "." || cleaned == ".." ||
		strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return errors.New("use a relative directory inside the project (not an absolute/traversal path)")
	}
	return nil
}
