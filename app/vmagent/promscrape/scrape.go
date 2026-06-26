package promscrape

import (
	"context"
	"net"
	"sync"
	"time"
)

type ScrapeManager struct {
	targets     map[string]*ScrapeTarget
	mu          sync.RWMutex
	resolver    Resolver
	baseBackoff time.Duration
	maxBackoff  time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewScrapeManager(resolver Resolver) *ScrapeManager {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ctx, cancel := context.WithCancel(context.Background())
	sm := &ScrapeManager{
		targets:     make(map[string]*ScrapeTarget),
		resolver:    resolver,
		baseBackoff: 1 * time.Second,
		maxBackoff:  1 * time.Minute,
		ctx:         ctx,
		cancel:      cancel,
	}
	go sm.loop()
	return sm
}

func (sm *ScrapeManager) AddTarget(url, hostname string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.targets[url] = NewScrapeTarget(url, hostname)
}

func (sm *ScrapeManager) Reload() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// Immediately reset backoffs and trigger a resolution attempt
	for _, target := range sm.targets {
		target.mu.Lock()
		target.failureCount = 0
		target.lastFailureTime = time.Time{}
		target.PendingResolution = true
		target.mu.Unlock()
		_ = target.Resolve(sm.ctx, sm.resolver, sm.baseBackoff, sm.maxBackoff)
	}
}

func (sm *ScrapeManager) Stop() {
	sm.cancel()
}

func (sm *ScrapeManager) loop() {
	ticker := time.NewTicker(100 * time.Millisecond) // Fast ticker for tests, adjustable
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.RLock()
			for _, target := range sm.targets {
				target.mu.Lock()
				isPending := target.PendingResolution
				target.mu.Unlock()
				if isPending {
					_ = target.Resolve(sm.ctx, sm.resolver, sm.baseBackoff, sm.maxBackoff)
				}
			}
			sm.mu.RUnlock()
		}
	}
}
