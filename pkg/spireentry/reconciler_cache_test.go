/*
Copyright 2021 SPIRE Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spireentry

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
	"github.com/spiffe/spire-controller-manager/pkg/metrics"
	"github.com/spiffe/spire-controller-manager/pkg/spireapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// fakeEntryClient is an in-package fake of spireapi.EntryClient that records
// calls and returns scriptable per-operation status codes (zero value = OK).
type fakeEntryClient struct {
	listCalls int
	listHints [][]string
	list      []spireapi.Entry

	createCode codes.Code
	updateCode codes.Code
	deleteCode codes.Code

	createErr error
	updateErr error
	deleteErr error

	created []spireapi.Entry
	updated []spireapi.Entry
	deleted []string
}

func (f *fakeEntryClient) ListEntries(_ context.Context, hints ...string) ([]spireapi.Entry, error) {
	f.listCalls++
	f.listHints = append(f.listHints, append([]string(nil), hints...))
	out := make([]spireapi.Entry, len(f.list))
	copy(out, f.list)
	return out, nil
}

func (f *fakeEntryClient) CreateEntries(_ context.Context, entries []spireapi.Entry) ([]spireapi.Status, error) {
	f.created = append(f.created, entries...)
	if f.createErr != nil {
		return nil, f.createErr
	}
	return statuses(len(entries), f.createCode), nil
}

func (f *fakeEntryClient) UpdateEntries(_ context.Context, entries []spireapi.Entry) ([]spireapi.Status, error) {
	f.updated = append(f.updated, entries...)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return statuses(len(entries), f.updateCode), nil
}

func (f *fakeEntryClient) DeleteEntries(_ context.Context, ids []string) ([]spireapi.Status, error) {
	f.deleted = append(f.deleted, ids...)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return statuses(len(ids), f.deleteCode), nil
}

func (f *fakeEntryClient) GetUnsupportedFields(context.Context, string, string) (map[spireapi.Field]struct{}, error) {
	return map[spireapi.Field]struct{}{}, nil
}

func statuses(n int, code codes.Code) []spireapi.Status {
	out := make([]spireapi.Status, n)
	for i := range out {
		out[i] = spireapi.Status{Code: code}
	}
	return out
}

// fakeByObject is a no-op byObject so the apply helpers can record counters
// without a real CRD.
type fakeByObject struct{}

func (fakeByObject) GetObjectKind() schema.ObjectKind   { return schema.EmptyObjectKind }
func (fakeByObject) GetUID() types.UID                  { return "" }
func (fakeByObject) GetCreationTimestamp() metav1.Time  { return metav1.Time{} }
func (fakeByObject) GetDeletionTimestamp() *metav1.Time { return nil }
func (fakeByObject) IncrementEntriesToSet()             {}
func (fakeByObject) IncrementEntriesMasked()            {}
func (fakeByObject) IncrementEntrySuccess()             {}
func (fakeByObject) IncrementEntryFailures()            {}

func testEntry(id string) spireapi.Entry {
	return spireapi.Entry{
		ID:        id,
		SPIFFEID:  spiffeid.RequireFromString("spiffe://domain.test/workload/" + id),
		ParentID:  spiffeid.RequireFromString("spiffe://domain.test/parent"),
		Selectors: []spireapi.Selector{{Type: "k8s", Value: "id:" + id}},
	}
}

func declared(id string) declaredEntry {
	return declaredEntry{Entry: testEntry(id), By: fakeByObject{}}
}

func newCacheReconciler(fake *fakeEntryClient, cfg ReconcilerConfig) *entryReconciler {
	cfg.EntryClient = fake
	r := &entryReconciler{config: cfg}
	if cfg.EnableEntryListCache {
		r.entryCache = &entryListCache{reloadAfter: time.Minute}
	}
	return r
}

func testCtx() context.Context {
	return ctrllog.IntoContext(context.Background(), logr.Discard())
}

func cacheCfg() ReconcilerConfig {
	return ReconcilerConfig{EntryIDPrefix: "test.", EnableEntryListCache: true}
}

// bubble runs a subtest inside a testing/synctest bubble, where time.Now() uses
// a fake clock that only advances on time.Sleep. This lets the production code
// keep calling time.Now() directly (no injected clock) while tests stay
// deterministic.
func bubble(t *testing.T, name string, f func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		synctest.Test(t, f)
	})
}

func TestEntryListCache(t *testing.T) {
	bubble(t, "serves from cache without re-listing", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a"), testEntry("test.b")}}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		cur1, _, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Len(t, cur1, 2)

		cur2, _, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Len(t, cur2, 2)

		require.Equal(t, 1, fake.listCalls, "second reconcile must be served from cache")
	})

	bubble(t, "disabled cache lists every time", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		cfg := cacheCfg()
		cfg.EnableEntryListCache = false
		r := newCacheReconciler(fake, cfg)
		require.Nil(t, r.entryCache)

		_, _, _ = r.listEntries(testCtx(), nil)
		_, _, _ = r.listEntries(testCtx(), nil)
		require.Equal(t, 2, fake.listCalls)
	})

	bubble(t, "cold cache lists (leader failover)", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		r := newCacheReconciler(fake, cacheCfg())
		require.Nil(t, r.entryCache.entries, "cache starts empty")

		_, _, err := r.listEntries(testCtx(), nil)
		require.NoError(t, err)
		require.Equal(t, 1, fake.listCalls)
		require.NotNil(t, r.entryCache.entries)
	})

	bubble(t, "reloads after the reload interval", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		r := newCacheReconciler(fake, cacheCfg()) // reloadAfter = 1m
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 1, fake.listCalls)

		time.Sleep(2 * time.Minute) // fake clock advances past nextReload
		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 2, fake.listCalls, "must re-list after reload interval elapses")
	})

	bubble(t, "create OK updates cache, no extra list", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		r.createEntries(ctx, []declaredEntry{declared("test.new")})

		cur, _, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 1, fake.listCalls, "create result must be reflected in cache, not re-listed")
		require.ElementsMatch(t, []string{"test.a", "test.new"}, ids(cur))
	})

	bubble(t, "create AlreadyExists triggers resync", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}, createCode: codes.AlreadyExists}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		r.createEntries(ctx, []declaredEntry{declared("test.new")})
		require.False(t, r.entryCache.fresh(""), "drift must invalidate the cache")

		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 2, fake.listCalls)
		require.True(t, r.entryCache.fresh(""), "cache fresh again after reload")
	})

	bubble(t, "update NotFound drops entry and triggers resync", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}, updateCode: codes.NotFound}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		r.updateEntries(ctx, []declaredEntry{declared("test.a")})
		require.False(t, r.entryCache.fresh(""))
		require.NotContains(t, r.entryCache.entries, "test.a")

		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 2, fake.listCalls)
	})

	bubble(t, "delete OK drops from cache", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a"), testEntry("test.b")}}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		r.deleteEntries(ctx, []spireapi.Entry{testEntry("test.a")})

		cur, _, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 1, fake.listCalls)
		require.ElementsMatch(t, []string{"test.b"}, ids(cur))
	})

	bubble(t, "apply RPC error invalidates cache (partial batch success)", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}, createErr: errors.New("rpc boom")}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		require.True(t, r.entryCache.fresh(""))

		r.createEntries(ctx, []declaredEntry{declared("test.new")})
		require.False(t, r.entryCache.fresh(""), "RPC error must invalidate the cache")

		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 2, fake.listCalls, "next reconcile must reload from server")
	})

	bubble(t, "delete NotFound drops and resyncs without re-delete loop", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}, deleteCode: codes.NotFound}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		r.deleteEntries(ctx, []spireapi.Entry{testEntry("test.a")})
		require.False(t, r.entryCache.fresh(""))
		require.NotContains(t, r.entryCache.entries, "test.a")
		require.Len(t, fake.deleted, 1, "must not re-issue the delete")
	})

	bubble(t, "deleteOnly partition preserved across cached reconcile", func(t *testing.T) {
		cleanup := "old."
		cfg := cacheCfg()
		cfg.EntryIDPrefixCleanup = &cleanup
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a"), testEntry("old.x")}}
		r := newCacheReconciler(fake, cfg)
		ctx := testCtx()

		_, del1, _ := r.listEntries(ctx, nil)
		require.ElementsMatch(t, []string{"old.x"}, ids(del1))

		cur2, del2, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 1, fake.listCalls)
		require.ElementsMatch(t, []string{"test.a"}, ids(cur2))
		require.ElementsMatch(t, []string{"old.x"}, ids(del2), "cleanup partition must survive cached reads")
	})
}

// counterValue reads the current value of a prometheus counter.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

func TestEntryListMetrics(t *testing.T) {
	newCounters := func() map[string]prometheus.Counter {
		return map[string]prometheus.Counter{
			metrics.EntryListServerCalls: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_entry_list_server_calls"}),
			metrics.EntryListCacheHits:   prometheus.NewCounter(prometheus.CounterOpts{Name: "test_entry_list_cache_hits"}),
		}
	}

	bubble(t, "cache miss counts a server call, subsequent hit counts a cache hit", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		r := newCacheReconciler(fake, cacheCfg())
		r.promCounter = newCounters()
		ctx := testCtx()

		// First reconcile: cold cache -> server list.
		_, _, err := r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 1.0, counterValue(t, r.promCounter[metrics.EntryListServerCalls]))
		require.Equal(t, 0.0, counterValue(t, r.promCounter[metrics.EntryListCacheHits]))

		// Second reconcile: served from the fresh cache -> cache hit.
		_, _, err = r.listEntries(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 1.0, counterValue(t, r.promCounter[metrics.EntryListServerCalls]))
		require.Equal(t, 1.0, counterValue(t, r.promCounter[metrics.EntryListCacheHits]))
	})

	bubble(t, "disabled cache counts every reconcile as a server call", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		cfg := cacheCfg()
		cfg.EnableEntryListCache = false
		r := newCacheReconciler(fake, cfg)
		r.promCounter = newCounters()
		ctx := testCtx()

		_, _, _ = r.listEntries(ctx, nil)
		_, _, _ = r.listEntries(ctx, nil)
		require.Equal(t, 2.0, counterValue(t, r.promCounter[metrics.EntryListServerCalls]))
		require.Equal(t, 0.0, counterValue(t, r.promCounter[metrics.EntryListCacheHits]))
	})
}

func TestEntryListHintFilter(t *testing.T) {
	t.Run("collects unique hints from managed resources", func(t *testing.T) {
		r := newCacheReconciler(&fakeEntryClient{}, ReconcilerConfig{EnableEntryListHintFilter: true})
		hints := r.entryListHints(
			[]*ClusterSPIFFEID{
				{ClusterSPIFFEID: spirev1alpha1.ClusterSPIFFEID{Spec: spirev1alpha1.ClusterSPIFFEIDSpec{Hint: "cluster-a"}}},
				{ClusterSPIFFEID: spirev1alpha1.ClusterSPIFFEID{Spec: spirev1alpha1.ClusterSPIFFEIDSpec{Hint: "cluster-b"}}},
				{ClusterSPIFFEID: spirev1alpha1.ClusterSPIFFEID{Spec: spirev1alpha1.ClusterSPIFFEIDSpec{Hint: "cluster-a"}}},
			},
			[]*ClusterStaticEntry{
				{ClusterStaticEntry: spirev1alpha1.ClusterStaticEntry{Spec: spirev1alpha1.ClusterStaticEntrySpec{Hint: "static"}}},
			},
		)

		require.Equal(t, []string{"cluster-a", "cluster-b", "static"}, hints)
	})

	t.Run("falls back to full list when a managed resource has no hint", func(t *testing.T) {
		r := newCacheReconciler(&fakeEntryClient{}, ReconcilerConfig{EnableEntryListHintFilter: true})
		hints := r.entryListHints(
			[]*ClusterSPIFFEID{
				{ClusterSPIFFEID: spirev1alpha1.ClusterSPIFFEID{Spec: spirev1alpha1.ClusterSPIFFEIDSpec{Hint: "cluster-a"}}},
				{ClusterSPIFFEID: spirev1alpha1.ClusterSPIFFEID{}},
			},
			nil,
		)

		require.Nil(t, hints)
	})

	t.Run("passes hint filter to server list and reloads when hints change", func(t *testing.T) {
		fake := &fakeEntryClient{list: []spireapi.Entry{testEntry("test.a")}}
		r := newCacheReconciler(fake, cacheCfg())
		ctx := testCtx()

		_, _, err := r.listEntries(ctx, []string{"cluster-a"})
		require.NoError(t, err)
		require.Equal(t, [][]string{{"cluster-a"}}, fake.listHints)

		_, _, err = r.listEntries(ctx, []string{"cluster-a"})
		require.NoError(t, err)
		require.Equal(t, 1, fake.listCalls)

		_, _, err = r.listEntries(ctx, []string{"cluster-b"})
		require.NoError(t, err)
		require.Equal(t, [][]string{{"cluster-a"}, {"cluster-b"}}, fake.listHints)
	})
}

func ids(entries []spireapi.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.ID)
	}
	return out
}
