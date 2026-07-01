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
	"time"

	"github.com/spiffe/spire-controller-manager/pkg/spireapi"
)

// entryListCache is an authoritative in-memory mirror of the SPIRE entries the
// reconciler tracks, so reconciles do not issue a full ListEntries RPC every
// time. It is accessed only from the single reconcile goroutine (see
// pkg/reconciler), so it needs no locking.
//
// The cache is keyed by entry ID. A nil entries map means "never loaded"; a
// zero nextReload means "reload on the next read" (used to force a resync after
// drift). Both are derived state, so no separate validity/resync flags are
// needed.
type entryListCache struct {
	reloadAfter time.Duration
	entries     map[string]spireapi.Entry
	nextReload  time.Time
	filterKey   string
}

// fresh reports whether the cache can be served without listing from the server.
func (c *entryListCache) fresh(filterKey string) bool {
	return c.entries != nil && c.filterKey == filterKey && time.Now().Before(c.nextReload)
}

// snapshot returns the cached entries as a slice.
func (c *entryListCache) snapshot() []spireapi.Entry {
	entries := make([]spireapi.Entry, 0, len(c.entries))
	for _, entry := range c.entries {
		entries = append(entries, entry)
	}
	return entries
}

// replace rebuilds the cache from a fresh server list and arms the next reload.
func (c *entryListCache) replace(filterKey string, entries []spireapi.Entry) {
	c.entries = make(map[string]spireapi.Entry, len(entries))
	for _, entry := range entries {
		c.entries[entry.ID] = entry
	}
	c.filterKey = filterKey
	c.nextReload = time.Now().Add(c.reloadAfter)
}

// upsert records a successfully created/updated entry.
func (c *entryListCache) upsert(entry spireapi.Entry) {
	if c.entries == nil {
		return
	}
	c.entries[entry.ID] = entry
}

// drop removes an entry known to no longer exist on the server.
func (c *entryListCache) drop(id string) {
	delete(c.entries, id)
}

// invalidate forces the next read to reload from the server. Used when an apply
// reports drift (AlreadyExists on create, NotFound on update/delete).
func (c *entryListCache) invalidate() {
	c.nextReload = time.Time{}
}
