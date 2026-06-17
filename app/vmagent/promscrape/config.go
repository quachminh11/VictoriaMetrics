package promscrape

import (
	"time"
)

// Config holds the configuration for the promscrape manager.
type Config struct {
	// ScrapeInterval is how often to scrape targets that are actively resolved.
	ScrapeInterval time.Duration

	// DNSReResolveInterval is how often to retry DNS resolution for targets
	// currently in a pending (unresolved) state.
	DNSReResolveInterval time.Duration

	// DNSResolveBackoffMin is the initial backoff duration after a DNS failure.
	DNSResolveBackoffMin time.Duration

	// DNSResolveBackoffMax is the maximum backoff duration for DNS retries.
	DNSResolveBackoffMax time.Duration

	// DNSResolveBackoffFactor is the multiplier for exponential backoff.
	DNSResolveBackoffFactor float64

	// ScrapeTimeout is the HTTP request timeout when scraping a target.
	ScrapeTimeout time.Duration

	// ListenAddr is the address for the HTTP server (e.g., for /reload and /metrics).
	ListenAddr string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ScrapeInterval:         15 * time.Second,
		DNSReResolveInterval:   30 * time.Second,
		DNSResolveBackoffMin:   5 * time.Second,
		DNSResolveBackoffMax:   60 * time.Second,
		DNSResolveBackoffFactor: 2.0,
		ScrapeTimeout:          10 * time.Second,
		ListenAddr:             ":8429",
	}
}
