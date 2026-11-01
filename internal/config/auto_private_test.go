package config

import (
	"strings"
	"testing"
)

func TestAutoPrivateGatewayBind(t *testing.T) {
	cfg, err := loadText(t, strings.Replace(validYAML, "host: \"127.0.0.1\"", "host: \"auto_private\"", 1))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "auto_private" {
		t.Fatal("auto_private changed unexpectedly")
	}
}
