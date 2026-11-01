// Package netbind discovers explicitly private LAN and Tailscale listeners.
// It never returns 0.0.0.0 or another wildcard, nor advertises loopback to TV.
package netbind

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

var ts4 = netip.MustParsePrefix("100.64.0.0/10")
var ts6 = netip.MustParsePrefix("fd7a:115c:a1e0::/48")

type Endpoint struct {
	Interface string
	Address   string
	Network   string // "lan", "tailscale", or "loopback"
	IP        netip.Addr
	Allowed   netip.Prefix
}

func (e Endpoint) MediaURL(path string) string {
	return (&url.URL{Scheme: "ws", Host: e.Address, Path: path}).String()
}
func Discover(host string, port int) ([]Endpoint, error) {
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid TCP port")
	}
	auto := host == "auto_private"
	if host == "" || host == "0.0.0.0" || host == "::" {
		return nil, errors.New("wildcard address is forbidden")
	}
	var target netip.Addr
	if !auto {
		ip, err := netip.ParseAddr(host)
		if err != nil || ip.IsUnspecified() {
			return nil, errors.New("server.host must be auto_private or a literal private IP")
		}
		target = ip.Unmap()
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	seen := map[netip.Addr]bool{}
	result := []Endpoint{}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		name := strings.ToLower(iface.Name)
		tailscale := strings.Contains(name, "tailscale")
		if iface.Flags&net.FlagLoopback == 0 && len(iface.HardwareAddr) == 0 && !tailscale {
			continue
		}
		addrs, e := iface.Addrs()
		if e != nil {
			continue
		}
		for _, a := range addrs {
			subnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(subnet.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			if ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.Is4In6() {
				continue
			}
			network := "lan"
			if ip.IsLoopback() {
				network = "loopback"
			} else if tailscale {
				if !ts4.Contains(ip) && !ts6.Contains(ip) {
					continue
				}
				network = "tailscale"
			} else if !ip.IsPrivate() || ts4.Contains(ip) || ts6.Contains(ip) {
				continue
			}
			if !auto && ip != target {
				continue
			}
			if seen[ip] {
				continue
			}
			seen[ip] = true
			ones, bits := subnet.Mask.Size()
			if ones < 0 || bits < 0 {
				continue
			}
			var allowed netip.Prefix
			if network == "loopback" {
				allowed = netip.PrefixFrom(ip, ip.BitLen()).Masked()
			}
			if network == "tailscale" {
				if ip.Is4() {
					allowed = ts4
				} else {
					allowed = ts6
				}
			}
			if network == "lan" {
				allowed = netip.PrefixFrom(ip, ones).Masked()
			}
			result = append(result, Endpoint{Interface: iface.Name, Address: net.JoinHostPort(ip.String(), strconv.Itoa(port)), Network: network, IP: ip, Allowed: allowed})
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no eligible interface for %s", host)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Address < result[j].Address })
	return result, nil
}
func RestrictPeer(ep Endpoint, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ip, err := netip.ParseAddr(raw)
		if err != nil || !ep.Allowed.Contains(ip.Unmap()) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
