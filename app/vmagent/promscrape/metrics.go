package promscrape

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// DNSResolveFailuresTotal counts the total number of DNS resolution failures.
	DNSResolveFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vm_promscrape_dns_resolve_failures_total",
		Help: "Total number of DNS resolution failures for scrape targets",
	})

	// DNSResolveSuccessesTotal counts the total number of successful DNS resolutions.
	DNSResolveSuccessesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vm_promscrape_dns_resolve_successes_total",
		Help: "Total number of successful DNS resolutions for scrape targets",
	})

	// TargetsActive is the current number of targets in the active (resolved) state.
	TargetsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vm_promscrape_targets_active",
		Help: "Current number of scrape targets in active (resolved) state",
	})

	// TargetsPending is the current number of targets in the pending (unresolved) state.
	TargetsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vm_promscrape_targets_pending",
		Help: "Current number of scrape targets in pending (unresolved) state",
	})

	// ScrapeDurationSeconds tracks scrape duration per target.
	ScrapeDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vm_promscrape_scrape_duration_seconds",
		Help:    "Duration of scrape requests",
		Buckets: prometheus.DefBuckets,
	}, []string{"target"})

	// ScrapeErrorsTotal counts scrape errors.
	ScrapeErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vm_promscrape_scrape_errors_total",
		Help: "Total number of scrape errors",
	}, []string{"target", "reason"})
)
