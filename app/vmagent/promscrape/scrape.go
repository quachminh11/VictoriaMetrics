package promscrape

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ScrapeWork represents a unit of scrape work containing the HTTP exchange
// with a single target.
type ScrapeWork struct {
	// Target to scrape.
	Target *Target
}

// Manager manages the lifecycle of scrape targets, including DNS resolution
// and periodic scraping.
type Manager struct {
	mu sync.RWMutex

	cfg *Config

	// targets is the full set of configured scrape targets.
	targets []*Target

	// stopCh signals the background goroutines to stop.
	stopCh chan struct{}

	// reloadCh triggers an immediate DNS re-resolution for all pending targets.
	reloadCh chan struct{}

	// wg tracks background goroutines.
	wg sync.WaitGroup

	// httpClient is used for scraping.
	httpClient *http.Client

	// lastLogCache prevents log flooding for repeated DNS failures.
	lastLogCache map[string]time.Time
}

// NewManager creates a new scrape Manager with the given Config.
func NewManager(cfg *Config) *Manager {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Manager{
		cfg:          cfg,
		stopCh:       make(chan struct{}),
		reloadCh:     make(chan struct{}, 1),
		httpClient: &http.Client{
			Timeout: cfg.ScrapeTimeout,
			Transport: &http.Transport{
				DisableKeepAlives:   false,
				MaxIdleConnsPerHost: 2,
			},
		},
		lastLogCache: make(map[string]time.Time),
	}
}

// AddTarget adds a new target to the manager.
func (m *Manager) AddTarget(scheme, hostname, port, metricsPath string, labels map[string]string) {
	t := NewTarget(scheme, hostname, port, metricsPath, labels)
	m.mu.Lock()
	m.targets = append(m.targets, t)
	m.mu.Unlock()

	log.Printf("[INFO] Added scrape target: %s (%s://%s:%s%s)", t.ID(), scheme, hostname, port, metricsPath)
}

// Targets returns a snapshot of all registered targets.
func (m *Manager) Targets() []*Target {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Target, len(m.targets))
	copy(out, m.targets)
	return out
}

// Reload triggers an immediate DNS re-resolution for all pending targets and
// resets their backoff timers. This is the handler for the /-/reload endpoint.
func (m *Manager) Reload() {
	log.Println("[INFO] Config reload triggered: resetting all pending target backoffs")
	m.mu.RLock()
	for _, t := range m.targets {
		if t.State() == TargetStatePending {
			t.ResetBackoff()
		}
	}
	m.mu.RUnlock()

	// Signal the background loop to run an immediate resolution pass.
	select {
	case m.reloadCh <- struct{}{}:
	default:
	}
}

// Run starts the manager's background goroutines and blocks until Stop is
// called or a fatal error occurs.
func (m *Manager) Run(ctx context.Context) {
	log.Println("[INFO] Starting promscrape manager")

	m.wg.Add(2)
	go m.dnsResolutionLoop(ctx)
	go m.scrapeLoop(ctx)

	// Block until context is cancelled.
	<-ctx.Done()
	log.Println("[INFO] Shutting down promscrape manager")
}

// Stop signals graceful shutdown and waits for goroutines to finish.
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
	log.Println("[INFO] Promscrape manager stopped")
}

// dnsResolutionLoop periodically re-resolves DNS for targets in pending state.
func (m *Manager) dnsResolutionLoop(ctx context.Context) {
	defer m.wg.Done()

	resolveTicker := time.NewTicker(m.cfg.DNSReResolveInterval)
	defer resolveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-m.reloadCh:
			m.resolvePendingTargets(context.Background())
		case <-resolveTicker.C:
			m.resolvePendingTargets(context.Background())
		}
	}
}

// resolvePendingTargets iterates all targets and resolves those that are
// pending and due for a retry.
func (m *Manager) resolvePendingTargets(ctx context.Context) {
	m.mu.RLock()
	targets := make([]*Target, len(m.targets))
	copy(targets, m.targets)
	m.mu.RUnlock()

	now := time.Now()
	for _, t := range targets {
		if t.State() != TargetStatePending {
			continue
		}
		if !t.NeedsResolve(now) {
			continue
		}

		resolved := t.Resolve(ctx)
		if !resolved {
			m.logDNSErrRateLimited(t)
		}
	}

	// Update metrics gauges.
	m.updateStateMetrics()
}

// logDNSErrRateLimited logs DNS failures at WARN level, rate-limited per target
// to once per 30 seconds to prevent log flooding.
func (m *Manager) logDNSErrRateLimited(t *Target) {
	now := time.Now()
	lastLog, ok := m.lastLogCache[t.ID()]
	if ok && now.Sub(lastLog) < 30*time.Second {
		return
	}
	m.lastLogCache[t.ID()] = now

	log.Printf("[WARN] DNS resolution failed for target %s: %s (will retry with backoff)",
		t.ID(), t.LastResolveError())
}

// scrapeLoop periodically scrapes all active targets.
func (m *Manager) scrapeLoop(ctx context.Context) {
	defer m.wg.Done()

	scrapeTicker := time.NewTicker(m.cfg.ScrapeInterval)
	defer scrapeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-scrapeTicker.C:
			m.scrapeAllTargets(ctx)
		}
	}
}

// scrapeAllTargets scrapes all targets that are in active (resolved) state.
func (m *Manager) scrapeAllTargets(ctx context.Context) {
	m.mu.RLock()
	targets := make([]*Target, len(m.targets))
	copy(targets, m.targets)
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, t := range targets {
		if t.State() != TargetStateActive {
			continue
		}
		wg.Add(1)
		go func(target *Target) {
			defer wg.Done()
			m.scrapeTarget(ctx, target)
		}(t)
	}
	wg.Wait()
}

// scrapeTarget performs an HTTP GET against the target's resolved URL.
func (m *Manager) scrapeTarget(ctx context.Context, t *Target) {
	url := t.ScrapeURL()
	if url == "" {
		// Target no longer has a resolved IP; skip.
		return
	}

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		ScrapeErrorsTotal.WithLabelValues(t.Hostname, "request_creation").Inc()
		log.Printf("[ERROR] Failed to create scrape request for %s: %v", t.ID(), err)
		return
	}

	resp, err := m.httpClient.Do(req)
	duration := time.Since(start).Seconds()
	ScrapeDurationSeconds.WithLabelValues(t.Hostname).Observe(duration)

	if err != nil {
		ScrapeErrorsTotal.WithLabelValues(t.Hostname, "http_error").Inc()
		log.Printf("[ERROR] Scrape failed for %s: %v", t.ID(), err)

		// Check if the error is likely a DNS/network error and re-resolve.
		errStr := err.Error()
		if isDNSError(errStr) && t.State() == TargetStateActive {
			log.Printf("[WARN] Scrape error suggests DNS issue for %s, marking for re-resolution", t.ID())
			// Force a resolution failure so it goes to pending.
			_ = t.Resolve(ctx)
		}
		return
	}
	defer resp.Body.Close()

	// Read and discard body for now (in production, we'd parse metrics here).
	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		ScrapeErrorsTotal.WithLabelValues(t.Hostname, fmt.Sprintf("http_%d", resp.StatusCode)).Inc()
		log.Printf("[WARN] Scrape target %s returned HTTP %d (%d bytes)", t.ID(), resp.StatusCode, len(bodyBytes))
		return
	}

	log.Printf("[DEBUG] Successfully scraped %s (%d bytes, %.3fs)", t.ID(), len(bodyBytes), duration)
}

// updateStateMetrics refreshes the Prometheus gauges for active/pending targets.
func (m *Manager) updateStateMetrics() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	active := 0
	pending := 0
	for _, t := range m.targets {
		switch t.State() {
		case TargetStateActive:
			active++
		case TargetStatePending:
			pending++
		}
	}
	TargetsActive.Set(float64(active))
	TargetsPending.Set(float64(pending))
}

// isDNSError checks if an error string suggests a DNS resolution problem.
func isDNSError(s string) bool {
	lower := strings.ToLower(s)
	indicators := []string{
		"no such host",
		"dns",
		"lookup",
		"temporary failure in name resolution",
		"nodename nor servname provided",
	}
	for _, ind := range indicators {
		if strings.Contains(lower, ind) {
			return true
		}
	}
	return false
}
