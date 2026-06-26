package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	StaticEntryFailures  = "cluster_static_entry_failures"
	EntryListServerCalls = "entry_list_server_calls"
	EntryListCacheHits   = "entry_list_cache_hits"
)

var (
	PromCounters = map[string]prometheus.Counter{
		StaticEntryFailures: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: StaticEntryFailures,
				Help: "Number of cluster static entry render failures",
			},
		),
		EntryListServerCalls: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: EntryListServerCalls,
				Help: "Number of ListEntries RPCs issued to the SPIRE server (in-memory entry list cache miss or cache disabled)",
			},
		),
		EntryListCacheHits: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: EntryListCacheHits,
				Help: "Number of entry list reconciles served from the in-memory entry list cache",
			},
		),
	}
)
