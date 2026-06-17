package promscrape

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

// TargetState represents the DNS resolution state of a scrape target.
type TargetState int

const (
	// TargetStateActive means the target's hostname resolved successfully and
	// it is being scraped.
	TargetStateActive TargetState = iota

	// TargetStatePending means the target's hostname failed to resolve and
	// it is waiting for periodic re-resolution.
	TargetStatePending
)

// Target represents a single scrape target with DNS resolution lifecycle.
type Target struct {
	// Scheme (http or https).
	Scheme string
	// Hostname to resolve (e.g. "example.com").
	Hostname string
	// Port (e.g. "8080").
	Port string
	// MetricsPath is the HTTP path to scrape (e.g. "/metrics").
	MetricsPath string

	// labels are key-value label pairs attached to scraped metrics.
	labels map[string]string

	mu sync.RWMutex

	// state is the current resolution state.
	state TargetState
	// resolvedIP is the last successfully resolved IP address.
	resolvedIP string
	// lastResolveError records the last DNS resolution error.
	lastResolveError string
	// lastResolveAttempt is when DNS was last attempted.
	lastResolveAttempt time.Time
	// backoff is the current backoff duration before next retry.
	backoff time.Duration
	// consecutiveFailures counts consecutive DNS failures.
	consecutiveFailures int

	// scrapeURL is the full URL derived after resolution.
	scrapeURL string
}

// NewTarget creates a new scrape target.
func NewTarget(scheme, hostname, port, metricsPath string, labels map[string]string) *Target {
	if labels == nil {
		labels = make(map[string]string)
	}
	return &Target{
		Scheme:      scheme,
		Hostname:    hostname,
		Port:        port,
		MetricsPath: metricsPath,
		labels:      labels,
		state:       TargetStatePending,
		backoff:     0, // first attempt is immediate
	}
}

// State returns the current target state.
func (t *Target) State() TargetState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// ResolvedIP returns the last resolved IP, or empty string if unresolved.
func (t *Target) ResolvedIP() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.resolvedIP
}

// Labels returns a copy of the target's labels.
func (t *Target) Labels() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]string, len(t.labels))
	for k, v := range t.labels {
		out[k] = v
	}
	return out
}

// ScrapeURL returns the full scrape URL if resolution succeeded, or empty.
func (t *Target) ScrapeURL() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.scrapeURL
}

// LastResolveError returns the last DNS resolution error message.
func (t *Target) LastResolveError() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastResolveError
}

// Resolve attempts to resolve the target's hostname. It updates the internal
// state and returns whether resolution succeeded.
func (t *Target) Resolve(ctx context.Context) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.lastResolveAttempt = time.Now()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, t.Hostname)
	if err != nil {
		t.consecutiveFailures++
		t.lastResolveError = err.Error()
		t.state = TargetStatePending
		t.resolvedIP = ""
		t.scrapeURL = ""

		// Advance backoff: min * factor^(consecutive-1), capped at max.
		b := float64(t.backoff)
		if b == 0 {
			// Initial backoff is already set by the caller via ResetBackoff
			// during first failure; if not set, use min.
			b = float64(DefaultConfig().DNSResolveBackoffMin)
		} else {
			b *= float64(DefaultConfig().DNSResolveBackoffFactor)
		}
		backoffMax := float64(DefaultConfig().DNSResolveBackoffMax)
		t.backoff = time.Duration(math.Min(b, backoffMax))

		DNSResolveFailuresTotal.Inc()
		_ = t.consecutiveFailures // used for backoff calculation
		return false
	}

	// Resolution succeeded.
	if len(ips) == 0 {
		t.consecutiveFailures++
		t.lastResolveError = "DNS returned no addresses"
		t.state = TargetStatePending
		t.resolvedIP = ""
		t.scrapeURL = ""
		DNSResolveFailuresTotal.Inc()
		return false
	}

	// Pick the first IP address.
	ipStr := ips[0].IP.String()

	// Reset failure state on success.
	wasFailure := t.state == TargetStatePending && t.consecutiveFailures > 0
	t.consecutiveFailures = 0
	t.lastResolveError = ""
	t.backoff = 0
	t.state = TargetStateActive
	t.resolvedIP = ipStr
	t.scrapeURL = fmt.Sprintf("%s://%s:%s%s", t.Scheme, ipStr, t.Port, t.MetricsPath)
	DNSResolveSuccessesTotal.Inc()

	// Log recovery.
	if wasFailure {
		log.Printf("[INFO] DNS resolution recovered for target %s -> %s", t.Hostname, ipStr)
	}

	return true
}

// NeedsResolve checks whether the target is due for a re-resolution attempt.
// It returns true if the target is in pending state and its backoff timer has elapsed.
func (t *Target) NeedsResolve(now time.Time) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.state != TargetStatePending {
		return false
	}
	if t.backoff == 0 {
		return true
	}
	return now.Sub(t.lastResolveAttempt) >= t.backoff
}

// ResetBackoff resets the backoff timer to zero, so the next call to
// NeedsResolve returns true immediately. Used on config reload.
func (t *Target) ResetBackoff() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.backoff = 0
}

// ID returns a unique identifier for the target (hostname:port path).
func (t *Target) ID() string {
	return fmt.Sprintf("%s:%s%s", t.Hostname, t.Port, t.MetricsPath)
}

// String returns a human-readable string for the target.
func (t *Target) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	stateStr := "active"
	if t.state == TargetStatePending {
		stateStr = "pending"
	}
	ip := t.resolvedIP
	if ip == "" {
		ip = "(unresolved)"
	}
	return fmt.Sprintf("Target{host=%s, ip=%s, state=%s}", t.Hostname, ip, stateStr)
}
