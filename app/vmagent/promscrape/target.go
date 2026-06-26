package promscrape

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	DNSResolveFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "vm_promscrape_dns_resolve_failures_total",
			Help: "Total number of DNS resolution failures for scrape targets.",
		},
	)
)

func init() {
	prometheus.MustRegister(DNSResolveFailuresTotal)
}

type Resolver interface {
	LookupIP(ctx context.Context, network, host string) ([]net.IP, error)
}

type ScrapeTarget struct {
	URL      string
	Hostname string

	mu               sync.Mutex
	Active           bool
	PendingResolution bool
	lastFailureTime  time.Time
	failureCount     int
}

func NewScrapeTarget(url, hostname string) *ScrapeTarget {
	return &ScrapeTarget{
		URL:      url,
		Hostname: hostname,
		Active:   false,
		PendingResolution: true,
	}
}

// Resolve DNS for target with backoff
func (t *ScrapeTarget) Resolve(ctx context.Context, resolver Resolver, baseBackoff time.Duration, maxBackoff time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.PendingResolution {
		// Calculate backoff
		backoff := time.Duration(1<<t.failureCount) * baseBackoff
		if backoff > maxBackoff || backoff <= 0 {
			backoff = maxBackoff
		}
		if time.Since(t.lastFailureTime) < backoff {
			return nil // Still in backoff period, skip resolution attempt
		}
	}

	ips, err := resolver.LookupIP(ctx, "ip", t.Hostname)
	if err != nil || len(ips) == 0 {
		DNSResolveFailuresTotal.Inc()
		t.failureCount++
		t.lastFailureTime = time.Now()
		t.Active = false
		t.PendingResolution = true
		log.Printf("WARN: DNS resolution failed for target %s (failure count: %d): %v", t.Hostname, t.failureCount, err)
		return err
	}

	// Success
	t.failureCount = 0
	t.Active = true
	t.PendingResolution = false
	return nil
}
