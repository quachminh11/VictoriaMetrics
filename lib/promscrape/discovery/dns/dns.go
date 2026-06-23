package dns

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
)

var (
	// dnsResolveTimeout is the timeout for DNS resolution.
	dnsResolveTimeout = time.Second * 30

	// dnsResolveRetryBackoff is the initial backoff duration for retrying failed DNS resolutions.
	dnsResolveRetryBackoff = time.Second * 5

	// dnsResolveMaxBackoff is the maximum backoff duration.
	dnsResolveMaxBackoff = time.Minute * 5

	// dnsResolveBackoffMultiplier is the multiplier for backoff after each consecutive failure.
	dnsResolveBackoffMultiplier = 2.0

	mu sync.Mutex
	// failedTargets stores the last failure time and current backoff for each target.
	failedTargets = make(map[string]*targetBackoff)
)

type targetBackoff struct {
	lastFailureTime time.Time
	currentBackoff  time.Duration
}

// ResolveAddr resolves addr to a list of IP addresses.
func ResolveAddr(ctx context.Context, addr string, limit int) ([]string, error) {
	mu.Lock()
	tb, exists := failedTargets[addr]
	mu.Unlock()

	if exists {
		// Check if enough time has passed since last failure to retry.
		elapsed := time.Since(tb.lastFailureTime)
		if elapsed < tb.currentBackoff {
			return nil, fmt.Errorf("DNS resolution for %q is in backoff; remaining backoff: %v", addr, tb.currentBackoff-elapsed)
		}
	}

	ips, err := resolveAddr(ctx, addr, limit)
	if err != nil {
		logger.Warnf("DNS resolution failed for %q: %s", addr, err)
		mu.Lock()
		if tb == nil {
			tb = &targetBackoff{
				lastFailureTime: time.Now(),
				currentBackoff:  dnsResolveRetryBackoff,
			}
			failedTargets[addr] = tb
		} else {
			tb.lastFailureTime = time.Now()
			tb.currentBackoff = time.Duration(float64(tb.currentBackoff) * dnsResolveBackoffMultiplier)
			if tb.currentBackoff > dnsResolveMaxBackoff {
				tb.currentBackoff = dnsResolveMaxBackoff
			}
		}
		mu.Unlock()
		return nil, err
	}

	// Success: remove from failed targets if present.
	mu.Lock()
	delete(failedTargets, addr)
	mu.Unlock()

	return ips, nil
}

func resolveAddr(ctx context.Context, addr string, limit int) ([]string, error) {
	// Split host and port if present.
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// addr does not contain port, assume it's a hostname.
		host = addr
		port = ""
	}

	// If host is already an IP, return as is.
	if net.ParseIP(host) != nil {
		if port != "" {
			return []string{net.JoinHostPort(host, port)}, nil
		}
		return []string{host}, nil
	}

	// Resolve hostname.
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("no IP addresses found for %q", host)
	}

	// Limit the number of IPs.
	if limit > 0 && len(ips) > limit {
		ips = ips[:limit]
	}

	// Convert to string slice.
	result := make([]string, len(ips))
	for i, ip := range ips {
		if port != "" {
			result[i] = net.JoinHostPort(ip.String(), port)
		} else {
			result[i] = ip.String()
		}
	}

	return result, nil
}

// ResetFailedTargets clears the failed targets map. Used for testing.
func ResetFailedTargets() {
	mu.Lock()
	failedTargets = make(map[string]*targetBackoff)
	mu.Unlock()
}
