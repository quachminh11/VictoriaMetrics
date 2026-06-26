package promscrape

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type MockResolver struct {
	ips []net.IP
	err error
	calls int32
}

func (m *MockResolver) LookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	atomic.AddInt32(&m.calls, 1)
	return m.ips, m.err
}

func (m *MockResolver) SetResult(ips []net.IP, err error) {
	m.ips = ips
	m.err = err
}

func TestScrapeManagerDNSResilience(t *testing.T) {
	mockResolver := &MockResolver{
		err: errors.New("dns failure"),
	}
	
	sm := NewScrapeManager(mockResolver)
	sm.baseBackoff = 50 * time.Millisecond
	sm.maxBackoff = 200 * time.Millisecond
	defer sm.Stop()

	sm.AddTarget("http://unstable-target.local:8080/metrics", "unstable-target.local")

	time.Sleep(200 * time.Millisecond)

	sm.mu.RLock()
	target := sm.targets["http://unstable-target.local:8080/metrics"]
	sm.mu.RUnlock()

	target.mu.Lock()
	if target.Active {
		t.Errorf("expected target to be inactive on DNS failure")
	}
	if !target.PendingResolution {
		t.Errorf("expected target to be pending resolution")
	}
	target.mu.Unlock()

	// Restore DNS
	mockResolver.SetResult([]net.IP{net.ParseIP("192.168.1.100")}, nil)

	// Wait for backoff and retry loop to succeed
	time.Sleep(1 * time.Second)

	target.mu.Lock()
	if !target.Active {
		t.Errorf("expected target to automatically recover and be active")
	}
	if target.PendingResolution {
		t.Errorf("expected target to not be pending anymore")
	}
	target.mu.Unlock()
}

func TestScrapeManagerReloadIntegration(t *testing.T) {
	mockResolver := &MockResolver{
		err: errors.New("dns failure"),
	}
	
	sm := NewScrapeManager(mockResolver)
	sm.baseBackoff = 1 * time.Hour // huge backoff
	sm.maxBackoff = 1 * time.Hour
	defer sm.Stop()

	sm.AddTarget("http://unstable.local", "unstable.local")

	// Trigger failure
	target := sm.targets["http://unstable.local"]
	_ = target.Resolve(sm.ctx, mockResolver, sm.baseBackoff, sm.maxBackoff)

	target.mu.Lock()
	failureCount := target.failureCount
	target.mu.Unlock()

	if failureCount != 1 {
		t.Errorf("expected failureCount 1, got %d", failureCount)
	}

	// Restore DNS
	mockResolver.SetResult([]net.IP{net.ParseIP("192.168.1.100")}, nil)

	// Normal resolve should skip due to huge backoff
	_ = target.Resolve(sm.ctx, mockResolver, sm.baseBackoff, sm.maxBackoff)
	
	target.mu.Lock()
	isActive := target.Active
	target.mu.Unlock()

	if isActive {
		t.Errorf("expected target to remain inactive due to backoff")
	}

	// Reload should force resolution
	sm.Reload()

	target.mu.Lock()
	isActive = target.Active
	target.mu.Unlock()

	if !isActive {
		t.Errorf("expected target to be active after config reload")
	}
}
