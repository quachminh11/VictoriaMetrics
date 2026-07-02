package scrape

import (
	"log"
	"net"
	"sync"
	"time"
)

// Resolver handles DNS resolution with recovery from consecutive failures.
type Resolver struct {
	hostname    string
	interval    time.Duration
	maxRetries  int
	backoffBase time.Duration
	
	healthy     bool
	mu          sync.RWMutex
	stopCh      chan struct{}
}

type ResolverConfig struct {
	Hostname    string
	Interval    time.Duration
	MaxRetries  int
	BackoffBase time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig(hostname string) ResolverConfig {
	return ResolverConfig{
		Hostname:    hostname,
		Interval:    30 * time.Second,
		MaxRetries:  5,
		BackoffBase: 2 * time.Second,
	}
}

// NewResolver creates a DNS resolver with automatic recovery.
func NewResolver(cfg ResolverConfig) *Resolver {
	return &Resolver{
		hostname:    cfg.Hostname,
		interval:    cfg.Interval,
		maxRetries:  cfg.MaxRetries,
		backoffBase: cfg.BackoffBase,
		healthy:     true,
		stopCh:      make(chan struct{}),
	}
}

// Start begins periodic DNS health checks.
func (r *Resolver) Start() {
	go r.loop()
}

// Stop terminates the health-check loop.
func (r *Resolver) Stop() { close(r.stopCh) }

func (r *Resolver) loop() {
	failCount := 0
	for {
		select {
		case <-r.stopCh:
			return
		case <-time.After(r.interval):
			_, err := net.LookupHost(r.hostname)
			if err != nil {
				failCount++
				r.mu.Lock()
				r.healthy = false
				r.mu.Unlock()
				backoff := r.backoffBase * (1 << min(failCount-1, r.maxRetries-1))
				log.Printf("DNS resolution failed for %s (%d/%d), backing off %v",
					r.hostname, failCount, r.maxRetries, backoff)
				time.Sleep(backoff)
				continue
			}
			if !r.healthy {
				log.Printf("DNS resolution recovered for %s", r.hostname)
			}
			r.mu.Lock()
			r.healthy = true
			failCount = 0
			r.mu.Unlock()
		}
	}
}

func min(a, b int) int { if a < b { return a }; return b }

// IsHealthy returns whether DNS resolution is currently working.
func (r *Resolver) IsHealthy() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.healthy
}
