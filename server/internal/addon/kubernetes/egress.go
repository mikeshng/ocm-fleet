package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"

	"k8s.io/client-go/rest"
)

// egressDialTimeout bounds one connection attempt made by the policy.
// The request's own deadline still applies when it is shorter.
const egressDialTimeout = 10 * time.Second

// ErrEgressRefused reports that a destination was not dialed because it
// is not somewhere a cluster or a hub can legitimately be: loopback,
// link-local (where cloud instance metadata lives), and the other
// ranges in nonClusterPrefixes. An operator permits a destination in
// one of those ranges with [EgressPolicy.Allow].
var ErrEgressRefused = errors.New("destination refused by egress policy")

// EgressPolicy decides which addresses the resource manager may dial
// when it reaches out to a cluster or a hub named by a request. The
// zero value refuses the ranges in nonClusterPrefixes; Allow names
// prefixes an operator permits anyway.
//
// The policy is applied where the connection is dialed, to the
// addresses the name resolves to at that moment, and the connection is
// made to one of the checked addresses. A check made when the request
// arrives would leave a name whose answers change between the check and
// the connection (DNS rebinding) able to reach what the check refused.
type EgressPolicy struct {
	// Allow lists prefixes that are permitted even when they are in a
	// range the policy otherwise refuses.
	Allow []netip.Prefix
}

// nonClusterPrefixes are the ranges a cluster's API server is not at.
// Private ranges (RFC 1918, IPv6 unique-local) are deliberately absent:
// clusters commonly live there, and reaching them is enrollment's
// purpose.
var nonClusterPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),        // "this network"; 0.0.0.0 reaches the host
	netip.MustParsePrefix("127.0.0.0/8"),      // loopback
	netip.MustParsePrefix("169.254.0.0/16"),   // link-local, including instance metadata at 169.254.169.254
	netip.MustParsePrefix("100.64.0.0/10"),    // shared address space, including some clouds' metadata
	netip.MustParsePrefix("168.63.129.16/32"), // Azure's virtual public address for its platform services
	netip.MustParsePrefix("192.0.0.0/24"),     // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"),    // benchmarking
	netip.MustParsePrefix("224.0.0.0/4"),      // multicast
	netip.MustParsePrefix("240.0.0.0/4"),      // reserved, including broadcast
	netip.MustParsePrefix("::/128"),           // unspecified
	netip.MustParsePrefix("::1/128"),          // loopback
	netip.MustParsePrefix("fe80::/10"),        // link-local
	netip.MustParsePrefix("fec0::/10"),        // deprecated site-local
	netip.MustParsePrefix("ff00::/8"),         // multicast
}

// nat64Prefix is the well-known prefix of a NAT64 gateway: the last 32
// bits are the IPv4 destination the gateway forwards to.
var nat64Prefix = netip.MustParsePrefix("64:ff9b::/96")

// Permit reports whether ip may be dialed. A permitted prefix in Allow
// wins over the default refusals, so an operator can enroll a cluster
// that really does listen on a loopback or link-local address.
func (p EgressPolicy) Permit(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()
	if prefixesContain(p.Allow, ip) {
		return true
	}
	if nonClusterAddress(ip) {
		return false
	}
	// A NAT64 address reaches whatever its embedded IPv4 address does.
	if v4, ok := nat64IPv4(ip); ok {
		if prefixesContain(p.Allow, v4) {
			return true
		}
		return !nonClusterAddress(v4)
	}
	return true
}

// DialConfig points a client-go REST config at this policy: connections
// are dialed through [EgressPolicy.DialContext], and never through the
// environment's HTTP proxy, which would make the proxy rather than the
// cluster the address this policy sees and dials.
func (p EgressPolicy) DialConfig(cfg *rest.Config) {
	cfg.Dial = p.DialContext
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
}

// DialContext resolves address and dials one of the addresses the name
// is permitted to resolve to. It is a [rest.Config.Dial] function: the
// transport keeps using the request's host name for TLS, so a
// certificate is still checked against the name the caller gave.
func (p EgressPolicy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("%w: %q is not a host:port address", ErrEgressRefused, address)
	}
	ips, err := resolveEgressHost(ctx, host)
	if err != nil {
		return nil, err
	}
	var permitted []netip.Addr
	for _, ip := range ips {
		if p.Permit(ip) {
			permitted = append(permitted, ip)
		}
	}
	if len(permitted) == 0 {
		return nil, fmt.Errorf("%w: %s resolves only to addresses a cluster is not at", ErrEgressRefused, host)
	}
	dialer := &net.Dialer{Timeout: egressDialTimeout, KeepAlive: 30 * time.Second}
	var lastErr error
	for _, ip := range permitted {
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
		if ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

// resolveEgressHost returns the addresses host stands for: the address
// itself for an IP literal, the resolver's answer otherwise.
func resolveEgressHost(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	seen := make(map[netip.Addr]struct{}, len(ips))
	out := ips[:0]
	for _, ip := range ips {
		if _, dup := seen[ip]; dup {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	return out, nil
}

// nonClusterAddress reports whether ip is in one of the ranges a
// cluster's API server is not at.
func nonClusterAddress(ip netip.Addr) bool {
	return prefixesContain(nonClusterPrefixes, ip)
}

// prefixesContain reports whether any of prefixes contains ip.
func prefixesContain(prefixes []netip.Prefix, ip netip.Addr) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

// nat64IPv4 returns the IPv4 address a NAT64 address stands for.
func nat64IPv4(ip netip.Addr) (netip.Addr, bool) {
	if !ip.Is6() || !nat64Prefix.Contains(ip) {
		return netip.Addr{}, false
	}
	b := ip.As16()
	return netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}), true
}
