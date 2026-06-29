package dns

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/metrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promscrape/discoveryutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
)

// SDCheckInterval defines interval for targets refresh.
var SDCheckInterval = flag.Duration("promscrape.dnsSDCheckInterval", 30*time.Second, "Interval for checking for changes in dns. "+
	"This works only if dns_sd_configs is configured in '-promscrape.config' file. "+
	"See https://docs.victoriametrics.com/victoriametrics/sd_configs/#dns_sd_configs for details")

var (
	dnsResolveFailures = metrics.NewCounter(`vm_promscrape_dns_resolve_failures_total`)
	dnsRecoveriesTotal = metrics.NewCounter(`vm_promscrape_dns_recoveries_total`)
	dnsRetriesTotal    = metrics.NewCounter(`vm_promscrape_dns_retries_total`)
)

// SDConfig represents service discovery config for DNS.
//
// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#dns_sd_config
type SDConfig struct {
	Names []string `yaml:"names"`
	Type  string   `yaml:"type,omitempty"`
	Port  *int     `yaml:"port,omitempty"`

	pendingTargetsMu sync.Mutex
	pendingTargets   map[string]*pendingTarget
	lastResolveTime  map[string]time.Time
}

type pendingTarget struct {
	name             string
	consecutiveFails int
	backoff          time.Duration
	nextRetryAt      time.Time
}

// GetLabels returns DNS labels according to sdc.
func (sdc *SDConfig) GetLabels(_ string) ([]*promutil.Labels, error) {
	if len(sdc.Names) == 0 {
		return nil, fmt.Errorf("`names` cannot be empty in `dns_sd_config`")
	}

	if sdc.pendingTargets == nil {
		sdc.pendingTargets = make(map[string]*pendingTarget)
		sdc.lastResolveTime = make(map[string]time.Time)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	typ := sdc.Type
	if typ == "" {
		typ = "SRV"
	}
	typ = strings.ToUpper(typ)

	now := time.Now()

	var allLabels []*promutil.Labels
	hasActiveTargets := false

	for _, name := range sdc.Names {
		sdc.pendingTargetsMu.Lock()
		pt := sdc.pendingTargets[name]
		// Skip if in backoff and not yet time to retry
		if pt != nil && now.Before(pt.nextRetryAt) {
			sdc.pendingTargetsMu.Unlock()
			continue
		}
		sdc.pendingTargetsMu.Unlock()

		var labels []*promutil.Labels
		var err error

		switch typ {
		case "SRV":
			labels, err = getSRVAddrLabels(ctx, sdc, name)
		case "MX":
			labels, err = getMXAddrLabels(ctx, sdc, name)
		case "A", "AAAA":
			labels, err = getAAddrLabels(ctx, sdc, name, typ)
		default:
			return nil, fmt.Errorf("unexpected `type` in `dns_sd_config`: %q; supported values: SRV, A, AAAA", typ)
		}

		if err != nil {
			// Handle DNS failure - add to pending with exponential backoff
			sdc.pendingTargetsMu.Lock()
			if pt := sdc.pendingTargets[name]; pt != nil {
				pt.consecutiveFails++
				pt.backoff = minDuration(pt.backoff*2, 30*time.Second)
				if pt.backoff == 0 {
					pt.backoff = 1 * time.Second
				}
				pt.nextRetryAt = now.Add(pt.backoff)
			} else {
				sdc.pendingTargets[name] = &pendingTarget{
					name:             name,
					consecutiveFails: 1,
					backoff:          1 * time.Second,
					nextRetryAt:      now.Add(1 * time.Second),
				}
			}
			sdc.lastResolveTime[name] = now
			sdc.pendingTargetsMu.Unlock()

			dnsResolveFailures.Inc()
			logger.Warnf("dns_sd_config: DNS resolution failed for %q (attempt %d, next retry in %v): %s",
				name, sdc.pendingTargets[name].consecutiveFails, pt.backoff, err)
			continue
		}

		// Success - clear pending state and add labels
		sdc.pendingTargetsMu.Lock()
		if sdc.pendingTargets[name] != nil {
			dnsRecoveriesTotal.Inc()
			logger.Infof("dns_sd_config: DNS resolution recovered for %q after %d failures",
				name, sdc.pendingTargets[name].consecutiveFails)
		}
		delete(sdc.pendingTargets, name)
		delete(sdc.lastResolveTime, name)
		sdc.pendingTargetsMu.Unlock()

		allLabels = append(allLabels, labels...)
		hasActiveTargets = true
	}

	// Increment retry counter for targets that are ready to retry
	sdc.pendingTargetsMu.Lock()
	for name, pt := range sdc.pendingTargets {
		if now.After(pt.nextRetryAt) {
			dnsRetriesTotal.Inc()
		}
	}
	sdc.pendingTargetsMu.Unlock()

	if !hasActiveTargets && len(sdc.Names) > 0 {
		// All targets are in backoff - return nil to not create scrapers
		return nil, nil
	}

	return allLabels, nil
}

// MustStop stops further usage for sdc.
func (sdc *SDConfig) MustStop() {
	sdc.pendingTargetsMu.Lock()
	sdc.pendingTargets = nil
	sdc.lastResolveTime = nil
	sdc.pendingTargetsMu.Unlock()
}

// ResetBackoff resets backoff state for all targets (called on config reload)
func (sdc *SDConfig) ResetBackoff() {
	sdc.pendingTargetsMu.Lock()
	for name := range sdc.pendingTargets {
		delete(sdc.pendingTargets, name)
		delete(sdc.lastResolveTime, name)
	}
	sdc.pendingTargetsMu.Unlock()
	logger.Infof("dns_sd_config: Reset DNS backoff state for all targets")
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func getMXAddrLabels(ctx context.Context, sdc *SDConfig, name string) ([]*promutil.Labels, error) {
	port := 25
	if sdc.Port != nil {
		port = *sdc.Port
	}

	mx, err := netutil.Resolver.LookupMX(ctx, name)
	if err != nil {
		return nil, err
	}

	var ms []*promutil.Labels
	for _, mxRec := range mx {
		target := mxRec.Host
		for strings.HasSuffix(target, ".") {
			target = target[:len(target)-1]
		}
		ms = appendMXLabels(ms, name, target, port)
	}
	return ms, nil
}

func getSRVAddrLabels(ctx context.Context, sdc *SDConfig, name string) ([]*promutil.Labels, error) {
	_, as, err := netutil.Resolver.LookupSRV(ctx, "", "", name)
	if err != nil {
		return nil, err
	}

	var ms []*promutil.Labels
	for _, a := range as {
		target := a.Target
		for strings.HasSuffix(target, ".") {
			target = target[:len(target)-1]
		}
		ms = appendAddrLabels(ms, name, target, int(a.Port))
	}
	return ms, nil
}

func getAAddrLabels(ctx context.Context, sdc *SDConfig, name, lookupType string) ([]*promutil.Labels, error) {
	if sdc.Port == nil {
		return nil, fmt.Errorf("missing `port` in `dns_sd_config` for `type: %s`", lookupType)
	}
	port := *sdc.Port

	ips, err := netutil.Resolver.LookupIPAddr(ctx, name)
	if err != nil {
		return nil, err
	}

	var ms []*promutil.Labels
	for _, ip := range ips {
		isIPv4 := ip.IP.To4() != nil
		if lookupType == "AAAA" && isIPv4 || lookupType == "A" && !isIPv4 {
			continue
		}
		ms = appendAddrLabels(ms, name, ip.IP.String(), port)
	}
	return ms, nil
}

func appendMXLabels(ms []*promutil.Labels, name, target string, port int) []*promutil.Labels {
	addr := discoveryutil.JoinHostPort(target, port)
	m := promutil.NewLabels(3)
	m.Add("__address__", addr)
	m.Add("__meta_dns_name", name)
	m.Add("__meta_dns_mx_record_target", target)
	return append(ms, m)
}

func appendAddrLabels(ms []*promutil.Labels, name, target string, port int) []*promutil.Labels {
	addr := discoveryutil.JoinHostPort(target, port)
	m := promutil.NewLabels(4)
	m.Add("__address__", addr)
	m.Add("__meta_dns_name", name)
	m.Add("__meta_dns_srv_record_target", target)
	m.Add("__meta_dns_srv_record_port", strconv.Itoa(port))
	return append(ms, m)
}