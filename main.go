package main

import (
	"fmt"
	"time"

	"github.com/quachminh11/VictoriaMetrics/pkg/scrape"
)

func main() {
	fmt.Println("VictoriaMetrics - DNS Recovery Fix")
	fmt.Println("====")
	
	r := scrape.NewResolver(scrape.DefaultConfig("localhost"))
	r.Start()
	defer r.Stop()
	
	fmt.Printf("DNS healthy: %v\n", r.IsHealthy())
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("DNS healthy after check: %v\n", r.IsHealthy())
}
