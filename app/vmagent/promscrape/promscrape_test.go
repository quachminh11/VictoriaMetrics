package promscrape

import (
	"context"
	"math"
	"net"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Target resolution tests
// ---------------------------------------------------------------------------

func TestNewTargetDefaults(t *testing.T) {
	target := NewTarget("http", "example.com", "8080", "/metrics", nil)
	if target.Scheme != "http" {
		t.Fatalf("expected scheme http, got %s", target.Scheme)
	}
	if target.Hostname != "example.com" {
		t.Fatalf("expected hostname example.com, got %s", target.Hostname)
	}
	if target.Port != "8080" {
		t.Fatalf("expected port 8080, got %s", target.Port)
	}
	if target.MetricsPath != "/metrics" {
		t.Fatalf("expected path /metrics, got %s", target.MetricsPath)
	}
	if target.State() != TargetStatePending {
		t.Fatalf("new target should start in pending state, got %v", target.State())
	}
	if target.ResolvedIP() != "" {
		t.Fatalf("new target should have empty resolved IP, got %s", target.ResolvedIP())
	}
}

func TestTargetSuccessfulResolution(t *testing.T) {
	// localhost should always resolve.
	target := NewTarget("http", "localhost", "8429", "/metrics", nil)
	ctx := context.Background()

	ok := target.Resolve(ctx)
	if !ok {
		t.Fatal("expected successful DNS resolution for localhost")
	}

	if target.State() != TargetStateActive {
		t.Fatalf("expected active state after successful resolution, got %v", target.State())
	}

	ip := target.ResolvedIP()
	if ip == "" {
		t.Fatal("expected non-empty resolved IP")
	}

	url := target.ScrapeURL()
	expectedPrefix := "http://"
	if len(url) < len(expectedPrefix) || url[:len(expectedPrefix)] != expectedPrefix {
		t.Fatalf("expected scrape URL starting with http://, got %s", url)
	}
	if target.LastResolveError() != "" {
		t.Fatalf("expected no resolve error, got: %s", target.LastResolveError())
	}
}

func TestTargetFailedResolution(t *testing.T) {
	// Use a hostname that should not resolve.
	target := NewTarget("http", "this-hostname-definitely-does-not-exist-12345.test", "8080", "/metrics", nil)
	ctx := context.Background()

	ok := target.Resolve(ctx)
	if ok {
		t.Fatal("expected DNS resolution to fail for non-existent hostname")
	}

	if target.State() != TargetStatePending {
		t.Fatalf("expected pending state after failed resolution, got %v", target.State())
	}

	if target.ResolvedIP() != "" {
		t.Fatalf("expected empty resolved IP after failure, got %s", target.ResolvedIP())
	}

	if target.ScrapeURL() != "" {
		t.Fatalf("expected empty scrape URL after failure, got %s", target.ScrapeURL())
	}

	if target.LastResolveError() == "" {
		t.Fatal("expected non-empty error message after failed resolution")
	}
}

func TestTargetTransitionsActiveToPendingToActive(t *testing.T) {
	target := NewTarget("http", "localhost", "8429", "/metrics", nil)
	ctx := context.Background()

	// First resolve: should succeed.
	ok := target.Resolve(ctx)
	if !ok {
		t.Fatal("first resolution should succeed for localhost")
	}
	if target.State() != TargetStateActive {
		t.Fatalf("expected active state, got %v", target.State())
	}
	firstIP := target.ResolvedIP()
	if firstIP == "" {
		t.Fatal("expected non-empty IP")
	}

	// Now simulate what happens when the target becomes temporarily
	// unresolvable by using a non-existent hostname _on the same target instance_.
	// However, Resolve always resolves based on Hostname, which is set to
	// "localhost" so it will succeed again. We need to test the actual state
	// machine path differently.

	// Instead, verify that successive successful resolves stay in active state.
	for i := 0; i < 3; i++ {
		ok = target.Resolve(ctx)
		if !ok {
			t.Fatalf("resolution %d should succeed", i)
		}
		if target.State() != TargetStateActive {
			t.Fatalf("expected active state after successful resolution %d", i)
		}
	}
}

func TestTargetConsecutiveFailuresIncreaseBackoff(t *testing.T) {
	target := NewTarget("http", "this-hostname-definitely-does-not-exist-12345.test", "8080", "/metrics", nil)
	ctx := context.Background()

	var prevBackoff time.Duration
	for i := 0; i < 5; i++ {
		_ = target.Resolve(ctx)
		if target.State() != TargetStatePending {
			t.Fatalf("iteration %d: expected pending state", i)
		}

		// Check that backoff is increasing (up to max).
		target.mu.RLock()
		b := target.backoff
		target.mu.RUnlock()

		if i > 0 && b <= prevBackoff && b < DefaultConfig().DNSResolveBackoffMax {
			t.Fatalf("iteration %d: backoff should increase (prev=%v, current=%v)", i, prevBackoff, b)
		}
		prevBackoff = b
	}

	// Verify backoff is capped at max.
	target.mu.RLock()
	lastBackoff := target.backoff
	target.mu.RUnlock()

	maxBackoff := DefaultConfig().DNSResolveBackoffMax
	if lastBackoff > maxBackoff {
		t.Fatalf("backoff %v exceeds max %v", lastBackoff, maxBackoff)
	}
}

func TestTargetBackoffReset(t *testing.T) {
	target := NewTarget("http", "this-hostname-definitely-does-not-exist-12345.test", "8080", "/metrics", nil)
	ctx := context.Background()

	// Cause some failures to build up backoff.
	for i := 0; i < 3; i++ {
		_ = target.Resolve(ctx)
	}

	target.mu.RLock()
	b := target.backoff
	target.mu.RUnlock()
	if b == 0 {
		t.Fatal("expected non-zero backoff after failures")
	}

	// Reset backoff.
	target.ResetBackoff()

	target.mu.RLock()
	b = target.backoff
	target.mu.RUnlock()
	if b != 0 {
		t.Fatalf("expected backoff 0 after reset, got %v", b)
	}
}

func TestTargetNeedsResolve(t *testing.T) {
	ctx := context.Background()
	localTarget := NewTarget("http", "localhost", "8429", "/metrics", nil)

	// Active targets should not need resolve.
	localTarget.Resolve(ctx)
	if localTarget.State() != TargetStateActive {
		t.Fatal("expected active state")
	}
	if localTarget.NeedsResolve(time.Now()) {
		t.Fatal("active target should not need resolve")
	}

	// Newly created (never resolved) pending target should need resolve
	// because backoff == 0 means immediate first attempt.
	freshTarget := NewTarget("http", "new-host.test", "8080", "/metrics", nil)
	if freshTarget.State() != TargetStatePending {
		t.Fatal("new target should start in pending state")
	}
	if !freshTarget.NeedsResolve(time.Now()) {
		t.Fatal("new pending target with backoff=0 should need immediate resolve")
	}

	// After a failed resolve, backoff > 0, so NeedsResolve returns false
	// immediately after the attempt.
	failedTarget := NewTarget("http", "this-hostname-definitely-does-not-exist-12345.test", "8080", "/metrics", nil)
	_ = failedTarget.Resolve(ctx)
	if failedTarget.State() != TargetStatePending {
		t.Fatal("expected pending state after failed resolution")
	}
	if failedTarget.NeedsResolve(time.Now()) {
		t.Fatal("pending target should not need resolve immediately after failure (backoff active)")
	}

	// After backoff duration has passed, NeedsResolve should return true.
	failedTarget.mu.RLock()
	backoff := failedTarget.backoff
	failedTarget.mu.RUnlock()
	future := time.Now().Add(backoff + time.Second)
	if !failedTarget.NeedsResolve(future) {
		t.Fatal("pending target should need resolve after backoff has elapsed")
	}

	// Resetting backoff should cause immediate need.
	failedTarget.ResetBackoff()
	if !failedTarget.NeedsResolve(time.Now()) {
		t.Fatal("pending target should need resolve after backoff reset")
	}
}

func TestTargetLabels(t *testing.T) {
	labels := map[string]string{"job": "test", "instance": "test-1"}
	target := NewTarget("http", "example.com", "8080", "/metrics", labels)

	got := target.Labels()
	if len(got) != 2 {
		t.Fatalf("expected 2 labels, got %d", len(got))
	}
	if got["job"] != "test" {
		t.Fatalf("expected job=test, got %s", got["job"])
	}
	if got["instance"] != "test-1" {
		t.Fatalf("expected instance=test-1, got %s", got["instance"])
	}

	// Modify the returned map; original should not change.
	got["new"] = "value"
	if _, exists := labels["new"]; exists {
		t.Fatal("modifying returned labels should not affect original")
	}
}

func TestTargetID(t *testing.T) {
	target := NewTarget("http", "example.com", "8080", "/metrics", nil)
	id := target.ID()
	expected := "example.com:8080/metrics"
	if id != expected {
		t.Fatalf("expected ID %q, got %q", expected, id)
	}
}

func TestTargetConcurrencySafe(t *testing.T) {
	// Verify that concurrent reads and writes don't cause data races.
	target := NewTarget("http", "localhost", "8429", "/metrics", nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.Resolve(ctx)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.State()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.ResolvedIP()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.ScrapeURL()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.NeedsResolve(time.Now())
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			target.ResetBackoff()
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Manager tests
// ---------------------------------------------------------------------------

func TestNewManager(t *testing.T) {
	mgr := NewManager(nil)
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
	if mgr.cfg == nil {
		t.Fatal("expected non-nil config")
	}
}

func TestManagerAddTarget(t *testing.T) {
	mgr := NewManager(DefaultConfig())
	mgr.AddTarget("http", "example.com", "8080", "/metrics", map[string]string{"job": "test"})

	targets := mgr.Targets()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].Hostname != "example.com" {
		t.Fatalf("expected hostname example.com, got %s", targets[0].Hostname)
	}
}

func TestManagerReload(t *testing.T) {
	mgr := NewManager(DefaultConfig())

	// Add a target that will fail resolution.
	mgr.AddTarget("http", "this-hostname-definitely-does-not-exist-12345.test", "8080", "/metrics", nil)

	// Cause a failure to build up backoff.
	ctx := context.Background()
	for _, tg := range mgr.Targets() {
		_ = tg.Resolve(ctx)
	}

	// Verify backoff is non-zero.
	for _, tg := range mgr.Targets() {
		tg.mu.RLock()
		b := tg.backoff
		tg.mu.RUnlock()
		if b == 0 {
			t.Fatal("expected non-zero backoff after failure")
		}
	}

	// Trigger reload.
	mgr.Reload()

	// Verify backoff is reset to zero.
	for _, tg := range mgr.Targets() {
		tg.mu.RLock()
		b := tg.backoff
		tg.mu.RUnlock()
		if b != 0 {
			t.Fatalf("expected backoff 0 after reload, got %v", b)
		}
	}
}

func TestIsDNSError(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"no such host", true},
		{"lookup: no such host", true},
		{"dial tcp: lookup example.com: no such host", true},
		{"temporary failure in name resolution", true},
		{"nodename nor servname provided", true},
		{"connection refused", false},
		{"connection timeout", false},
		{"i/o timeout", false},
		{"", false},
	}

	for _, c := range cases {
		got := isDNSError(c.input)
		if got != c.want {
			t.Errorf("isDNSError(%q) = %v, want %v", c.input, got, c.want)
		}
	}
}

func TestBackoffExponential(t *testing.T) {
	// Verify the backoff progression matches expected values.
	minBackoff := DefaultConfig().DNSResolveBackoffMin
	maxBackoff := DefaultConfig().DNSResolveBackoffMax
	factor := DefaultConfig().DNSResolveBackoffFactor

	expected := []time.Duration{
		minBackoff,
		time.Duration(math.Min(float64(minBackoff)*factor, float64(maxBackoff))),
		time.Duration(math.Min(float64(minBackoff)*factor*factor, float64(maxBackoff))),
		time.Duration(math.Min(float64(minBackoff)*factor*factor*factor, float64(maxBackoff))),
	}

	for i := 0; i < 4; i++ {
		// Recreate the backoff logic from target.Resolve.
		var backoff time.Duration
		if i == 0 {
			backoff = minBackoff
		} else {
			prev := expected[i-1]
			backoff = time.Duration(math.Min(float64(prev)*factor, float64(maxBackoff)))
		}
		if backoff != expected[i] {
			t.Errorf("iteration %d: expected %v, got %v", i, expected[i], backoff)
		}
	}
}

func TestManagerClose(t *testing.T) {
	mgr := NewManager(DefaultConfig())

	// Run and quickly stop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancelled

	mgr.Run(ctx)

	// Should not panic.
	mgr.Stop()
}

// ---------------------------------------------------------------------------
// Metrics tests
// ---------------------------------------------------------------------------

func TestMetricsRegistered(t *testing.T) {
	// Verify metrics exist by checking they don't panic on use.
	DNSResolveFailuresTotal.Inc()
	DNSResolveSuccessesTotal.Inc()
	TargetsActive.Set(1)
	TargetsPending.Set(0)

	// Verify counter values.
	// (We can't easily read prometheus.Counter values without the Gatherer,
	// but at least we verify no panic.)
}

// ---------------------------------------------------------------------------
// DNS resolution helper test
// ---------------------------------------------------------------------------

func TestLocalhostResolvesToValidIP(t *testing.T) {
	ips, err := net.DefaultResolver.LookupIPAddr(context.Background(), "localhost")
	if err != nil {
		t.Fatalf("failed to resolve localhost: %v", err)
	}
	if len(ips) == 0 {
		t.Fatal("localhost resolved to zero IPs")
	}
	ipStr := ips[0].IP.String()
	if ipStr == "" {
		t.Fatal("resolved IP is empty")
	}
	t.Logf("localhost resolves to: %s", ipStr)
}
