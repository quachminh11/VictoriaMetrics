package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sniegul-szam/VictoriaMetrics/app/vmagent/promscrape"
)

var (
	listenAddr         = flag.String("httpListenAddr", ":8429", "HTTP server listen address")
	scrapeInterval     = flag.Duration("scrapeInterval", 15*time.Second, "Interval between scrapes")
	reResolveInterval  = flag.Duration("dnsReResolveInterval", 30*time.Second, "Interval for DNS re-resolution of pending targets")
	scrapeTimeout      = flag.Duration("scrapeTimeout", 10*time.Second, "Timeout for HTTP scrape requests")
)

func main() {
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("[INFO] Starting vmagent")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &promscrape.Config{
		ScrapeInterval:          *scrapeInterval,
		DNSReResolveInterval:    *reResolveInterval,
		DNSResolveBackoffMin:    5 * time.Second,
		DNSResolveBackoffMax:    60 * time.Second,
		DNSResolveBackoffFactor: 2.0,
		ScrapeTimeout:           *scrapeTimeout,
		ListenAddr:              *listenAddr,
	}

	mgr := promscrape.NewManager(cfg)

	// Register example scrape targets. In production, these would come from
	// a configuration file or service discovery.
	mgr.AddTarget("http", "localhost", "8429", "/metrics", map[string]string{"job": "vmagent"})

	// Set up HTTP server for /metrics and /-/reload endpoints.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/-/reload", func(w http.ResponseWriter, r *http.Request) {
		mgr.Reload()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "reload requested")
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
	})
	mux.HandleFunc("/targets", func(w http.ResponseWriter, r *http.Request) {
		for _, t := range mgr.Targets() {
			fmt.Fprintf(w, "%s\n", t.String())
		}
	})

	srv := &http.Server{
		Addr:    *listenAddr,
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("[INFO] HTTP server listening on %s", *listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	go mgr.Run(ctx)

	<-sigCh
	log.Println("[INFO] Received shutdown signal")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[ERROR] HTTP server shutdown error: %v", err)
	}
	mgr.Stop()
	log.Println("[INFO] vmagent stopped")
}
