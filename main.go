package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("Hello, Bounty Hunter!")

	// 启动 vmagent 模拟抓取循环，用于演示 DNS 重试修复
	mgr := NewScrapeManager()
	mgr.AddTarget(&Target{
		Hostname: "example.com",
		Interval: 5 * time.Second,
		Timeout:  2 * time.Second,
	})
	mgr.Run()

	// 保持主 goroutine 运行
	select {}
}
