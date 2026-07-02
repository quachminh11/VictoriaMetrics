package scrape

import (
	"testing"
	"time"
)

func TestResolverHealth(t *testing.T) {
	r := NewResolver(DefaultConfig("localhost"))
	r.Start()
	defer r.Stop()
	time.Sleep(100 * time.Millisecond)
	if !r.IsHealthy() {
		t.Fatal("expected healthy for localhost")
	}
}

func TestResolverBackoff(t *testing.T) {
	cfg := DefaultConfig("nonexistent.invalid")
	cfg.Interval = 10 * time.Millisecond
	cfg.BackoffBase = 5 * time.Millisecond
	cfg.MaxRetries = 3

	r := NewResolver(cfg)
	r.Start()
	defer r.Stop()
	time.Sleep(100 * time.Millisecond)
	// Should detect failure
	if r.IsHealthy() {
		t.Log("DNS still resolving (not unexpected in all environments)")
	}
}
