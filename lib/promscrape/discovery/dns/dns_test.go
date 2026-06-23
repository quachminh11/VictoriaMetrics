package dns

import (
	"context"
	"testing"
	"time"
)

func TestResolveAddrBackoff(t *testing.T) {
	// Reset failed targets before test.
	ResetFailedTargets()

	// Test that consecutive failures cause backoff.
	ctx := context.Background()
	addr := "nonexistent.example.com"

	// First failure.
	_, err := ResolveAddr(ctx, addr, 0)
	if err == nil {
		t.Fatal("expected error for nonexistent domain")
	}

	// Immediate retry should still be in backoff.
	_, err = ResolveAddr(ctx, addr, 0)
	if err == nil {
		t.Fatal("expected backoff error")
	}
	if !strings.Contains(err.Error(), "backoff") {
		t.Fatalf("expected backoff error, got: %v", err)
	}

	// Wait for backoff to expire.
	time.Sleep(dnsResolveRetryBackoff + 100*time.Millisecond)

	// Retry should now attempt resolution again (will fail but not backoff error).
	_, err = ResolveAddr(ctx, addr, 0)
	if err == nil {
		t.Fatal("expected error for nonexistent domain")
	}
	if strings.Contains(err.Error(), "backoff") {
		t.Fatal("unexpected backoff error after waiting")
	}
}

func TestResolveAddrSuccessAfterFailure(t *testing.T) {
	// Reset failed targets before test.
	ResetFailedTargets()

	ctx := context.Background()
	// Use a domain that is likely to resolve (e.g., localhost).
	addr := "localhost"

	// Simulate a failure by calling with a nonexistent domain first.
	_, err := ResolveAddr(ctx, "nonexistent.example.com", 0)
	if err == nil {
		t.Fatal("expected error for nonexistent domain")
	}

	// Now resolve a valid address.
	ips, err := ResolveAddr(ctx, addr, 0)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(ips) == 0 {
		t.Fatal("expected at least one IP")
	}

	// Ensure the failed target map is cleared for the successful address.
	mu.Lock()
	_, exists := failedTargets[addr]
	mu.Unlock()
	if exists {
		t.Fatal("failed target should have been removed after success")
	}
}
