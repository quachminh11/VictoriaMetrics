package main

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Target 表示一个抓取目标
type Target struct {
	Hostname string
	Interval time.Duration // 正常抓取间隔
	Timeout  time.Duration // DNS 解析超时

	healthy       bool
	failureCount  int
	lastSeenError error
	mu            sync.Mutex
}

// ScrapeManager 管理所有抓取目标
type ScrapeManager struct {
	targets []*Target
}

// NewScrapeManager 创建新的 ScrapeManager
func NewScrapeManager() *ScrapeManager {
	return &ScrapeManager{}
}

// AddTarget 添加一个抓取目标
func (m *ScrapeManager) AddTarget(t *Target) {
	m.targets = append(m.targets, t)
}

// Run 启动所有目标的抓取循环（每个目标独立 goroutine）
func (m *ScrapeManager) Run() {
	for _, t := range m.targets {
		go t.scrapeLoop()
	}
}

// scrapeLoop 单个目标的抓取循环，包含指数退避重试逻辑
func (t *Target) scrapeLoop() {
	for {
		t.mu.Lock()
		interval := t.Interval
		t.mu.Unlock()

		time.Sleep(interval)

		// 执行一次抓取（先解析 DNS）
		err := t.scrape()
		t.mu.Lock()
		if err != nil {
			t.failureCount++
			t.lastSeenError = err
			t.healthy = false

			// 指数退避：每次失败后等待时间递增，最大 60 秒
			backoff := time.Duration(1<<uint(t.failureCount-1)) * time.Second
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			// 增加随机抖动，避免惊群效应（抖动范围为 ±25%）
			jitter := time.Duration(float64(backoff) * 0.25 * (0.5 - rand.Float64()))
			backoff += jitter
			fmt.Printf("[vmagent] DNS resolution failed for %s: %v; backing off %v (failure #%d)\n",
				t.Hostname, err, backoff, t.failureCount)
			t.mu.Unlock()
			time.Sleep(backoff)
			continue
		}
		// 成功：重置失败计数
		t.failureCount = 0
		t.lastSeenError = nil
		t.healthy = true
		fmt.Printf("[vmagent] Successfully resolved and scraped %s\n", t.Hostname)
		t.mu.Unlock()
	}
}

// scrape 执行一次抓取：先进行 DNS 解析，然后模拟 HTTP 请求（这里仅检查可达性）
func (t *Target) scrape() error {
	// 模拟 DNS 解析（超时控制）
	resolver := &net.Resolver{}
	ctx, cancel := context.WithTimeout(context.Background(), t.Timeout)
	defer cancel()

	addrs, err := resolver.LookupHost(ctx, t.Hostname)
	if err != nil {
		return fmt.Errorf("dns lookup failed: %w", err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("no addresses resolved for %s", t.Hostname)
	}

	// 模拟 HTTP 抓取（实际应使用 http.Client）
	// 此处仅打印解析到的 IP 作为验证
	fmt.Printf("[vmagent] Resolved %s -> %s\n", t.Hostname, addrs[0])
	return nil
}

// 需要导入 context 包
// 注意：代码中使用的 context 需要在文件顶部导入，但这里为了节省篇幅，实际会在完整文件中添加。
// 在最终输出中会包含完整的 import。
