package promscrape

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
)

// Scraper represents a scraper for a single target.
type Scraper struct {
	// target URL
	target string
	// scrape interval
	scrapeInterval time.Duration
	// scrape timeout
	scrapeTimeout time.Duration
	// honorLabels
	honorLabels bool
	// honorTimestamps
	honorTimestamps bool
	// params
	params string
	// labels
	labels string
	// streamParse
	streamParse bool
	// scrapeOffset
	scrapeOffset time.Duration
	// proxyURL
	proxyURL string
	// proxyAuthHeader
	proxyAuthHeader string
	// authHeader
	authHeader string
	// tlsConfig
	tlsConfig string
	// headers
	headers string
	// relabelConfigs
	relabelConfigs string
	// metricRelabelConfigs
	metricRelabelConfigs string

	// dnsResolver is used for DNS resolution
	dnsResolver *net.Resolver

	// dnsFailureCount counts consecutive DNS failures
	dnsFailureCount int

	// dnsBackoffDuration is the current backoff duration for DNS retries
	dnsBackoffDuration time.Duration

	// dnsBackoffMu protects dnsFailureCount and dnsBackoffDuration
	dnsBackoffMu sync.Mutex

	// dnsRetryInterval is the base interval for DNS retry backoff
	dnsRetryInterval time.Duration

	// dnsMaxBackoff is the maximum backoff duration
	dnsMaxBackoff time.Duration

	// dnsRetryJitter is the jitter factor for backoff (0.0 to 1.0)
	dnsRetryJitter float64

	// stopCh is used to signal the scraper to stop
	stopCh chan struct{}

	// scrapeWG is used to wait for the scrape loop to finish
	scrapeWG sync.WaitGroup
}

// NewScraper creates a new Scraper.
func NewScraper(target string, scrapeInterval, scrapeTimeout time.Duration, honorLabels, honorTimestamps bool, params, labels string, streamParse bool, scrapeOffset time.Duration, proxyURL, proxyAuthHeader, authHeader, tlsConfig, headers, relabelConfigs, metricRelabelConfigs string) *Scraper {
	s := &Scraper{
		target:             target,
		scrapeInterval:     scrapeInterval,
		scrapeTimeout:      scrapeTimeout,
		honorLabels:        honorLabels,
		honorTimestamps:    honorTimestamps,
		params:             params,
		labels:             labels,
		streamParse:        streamParse,
		scrapeOffset:       scrapeOffset,
		proxyURL:           proxyURL,
		proxyAuthHeader:    proxyAuthHeader,
		authHeader:         authHeader,
		tlsConfig:          tlsConfig,
		headers:            headers,
		relabelConfigs:     relabelConfigs,
		metricRelabelConfigs: metricRelabelConfigs,
		dnsResolver:        net.DefaultResolver,
		dnsRetryInterval:   5 * time.Second,
		dnsMaxBackoff:      5 * time.Minute,
		dnsRetryJitter:     0.5,
		stopCh:             make(chan struct{}),
	}
	return s
}

// Start starts the scraper loop.
func (s *Scraper) Start(ctx context.Context) {
	s.scrapeWG.Add(1)
	go s.scrapeLoop(ctx)
}

// Stop stops the scraper.
func (s *Scraper) Stop() {
	close(s.stopCh)
	s.scrapeWG.Wait()
}

// scrapeLoop is the main scrape loop.
func (s *Scraper) scrapeLoop(ctx context.Context) {
	defer s.scrapeWG.Done()

	ticker := time.NewTicker(s.scrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scrape(ctx)
		}
	}
}

// scrape performs a single scrape.
func (s *Scraper) scrape(ctx context.Context) {
	// Resolve target hostname
	host, port, err := net.SplitHostPort(s.target)
	if err != nil {
		// If target doesn't have a port, assume it's a hostname
		host = s.target
		port = "80"
	}

	// Check if host is an IP address
	if net.ParseIP(host) != nil {
		// IP address, no DNS resolution needed
		s.scrapeTarget(ctx, s.target)
		return
	}

	// Resolve hostname
	addrs, err := s.dnsResolver.LookupHost(ctx, host)
	if err != nil {
		// DNS resolution failed
		s.handleDNSFailure(ctx, host, err)
		return
	}

	// DNS resolution succeeded
	s.handleDNSSuccess(host)

	// Build target URL with resolved IP (optional, but we keep original target for scraping)
	// Actually, we should use the original target URL because the scraper will resolve again if needed.
	// But for consistency, we can use the first resolved address.
	// However, to keep it simple and avoid changing the scraping logic, we just use the original target.
	// The important thing is that we know the hostname is resolvable now.
	_ = addrs

	s.scrapeTarget(ctx, s.target)
}

// handleDNSFailure handles a DNS resolution failure.
func (s *Scraper) handleDNSFailure(ctx context.Context, host string, err error) {
	s.dnsBackoffMu.Lock()
	s.dnsFailureCount++
	if s.dnsFailureCount == 1 {
		// First failure, set initial backoff
		s.dnsBackoffDuration = s.dnsRetryInterval
	} else {
		// Exponential backoff with jitter
		backoff := s.dnsBackoffDuration * 2
		if backoff > s.dnsMaxBackoff {
			backoff = s.dnsMaxBackoff
		}
		jitter := time.Duration(float64(backoff) * s.dnsRetryJitter * rand.Float64())
		s.dnsBackoffDuration = backoff + jitter
	}
	backoffDuration := s.dnsBackoffDuration
	s.dnsBackoffMu.Unlock()

	logger.Warnf("DNS resolution failed for target %q: %s; will retry in %v", host, err, backoffDuration)

	// Wait for backoff duration before allowing next scrape attempt
	select {
	case <-ctx.Done():
		return
	case <-time.After(backoffDuration):
		// Retry DNS resolution
		s.retryDNS(ctx, host)
	}
}

// handleDNSSuccess resets DNS failure state on success.
func (s *Scraper) handleDNSSuccess(host string) {
	s.dnsBackoffMu.Lock()
	if s.dnsFailureCount > 0 {
		logger.Infof("DNS resolution succeeded for target %q after %d failures", host, s.dnsFailureCount)
		s.dnsFailureCount = 0
		s.dnsBackoffDuration = 0
	}
	s.dnsBackoffMu.Unlock()
}

// retryDNS retries DNS resolution after a backoff.
func (s *Scraper) retryDNS(ctx context.Context, host string) {
	addrs, err := s.dnsResolver.LookupHost(ctx, host)
	if err != nil {
		// Still failing, log and wait for next scrape cycle
		s.dnsBackoffMu.Lock()
		s.dnsFailureCount++
		backoff := s.dnsBackoffDuration * 2
		if backoff > s.dnsMaxBackoff {
			backoff = s.dnsMaxBackoff
		}
		jitter := time.Duration(float64(backoff) * s.dnsRetryJitter * rand.Float64())
		s.dnsBackoffDuration = backoff + jitter
		backoffDuration := s.dnsBackoffDuration
		s.dnsBackoffMu.Unlock()

		logger.Warnf("DNS resolution retry failed for target %q: %s; next retry in %v", host, err, backoffDuration)
		return
	}

	// Success
	s.handleDNSSuccess(host)
	logger.Infof("DNS resolution succeeded for target %q after retry, addresses: %v", host, addrs)
	// The next scrape cycle will use the original target URL, which will be resolved again.
	// No need to trigger an immediate scrape; the next scheduled scrape will pick it up.
}

// scrapeTarget performs the actual scrape of the target.
func (s *Scraper) scrapeTarget(ctx context.Context, target string) {
	// Placeholder for actual scrape logic
	// In real implementation, this would call the HTTP scraper
	logger.Infof("Scraping target %q", target)
}
