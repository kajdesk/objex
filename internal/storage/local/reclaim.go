package local

import (
	"log/slog"
	"sync"
	"time"
)

// leases tracks blobs that open readers depend on. A GET opens segments
// lazily, so the blobs of an object it has started reading must survive an
// overwrite or delete until the reader closes.
//
// Taking a lease races with reclamation: a reader reads the metadata, then
// leases the blobs it names; if a writer removes the object in between, the
// reclaimer could delete a blob the reader is about to lease. gate closes that
// window: readers hold it shared from before the metadata read until the lease
// is taken, and the reclaimer holds it exclusively while it decides what to
// delete. Any reader that saw the old metadata has leased its blobs by then.
type leases struct {
	gate sync.RWMutex
	mu   sync.Mutex
	held map[string]int
}

func newLeases() *leases { return &leases{held: map[string]int{}} }

func (l *leases) acquire(ids []string) {
	l.mu.Lock()
	for _, id := range ids {
		l.held[id]++
	}
	l.mu.Unlock()
}

func (l *leases) release(ids []string) {
	l.mu.Lock()
	for _, id := range ids {
		if l.held[id]--; l.held[id] <= 0 {
			delete(l.held, id)
		}
	}
	l.mu.Unlock()
}

func (l *leases) leased(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held[id] > 0
}

// unleased filters ids to those no reader holds, deciding under the exclusive
// gate (see above).
func (l *leases) unleased(ids []string) (free, busy []string) {
	l.gate.Lock()
	defer l.gate.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, id := range ids {
		if l.held[id] > 0 {
			busy = append(busy, id)
		} else {
			free = append(free, id)
		}
	}
	return free, busy
}

const (
	reclaimTick        = 200 * time.Millisecond
	reclaimConcurrency = 16
)

// reclaimer deletes blobs that metadata no longer references, in the
// background so deletes and overwrites return as soon as metadata commits.
// Blobs still leased by readers wait. Anything left undeleted at a crash is
// found later by GC.
type reclaimer struct {
	s       *Store
	mu      sync.Mutex
	pending []string
	wake    chan struct{}
	flush   chan chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

func newReclaimer(s *Store) *reclaimer {
	r := &reclaimer{s: s, wake: make(chan struct{}, 1), flush: make(chan chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	go r.run()
	return r
}

func (r *reclaimer) queue(segs []segment) {
	if len(segs) == 0 {
		return
	}
	r.mu.Lock()
	for _, sg := range segs {
		r.pending = append(r.pending, sg.Blob)
	}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Flush deletes everything queued and unleased now.
func (r *reclaimer) Flush() {
	ch := make(chan struct{})
	select {
	case r.flush <- ch:
		<-ch
	case <-r.done:
	}
}

func (r *reclaimer) close() {
	close(r.stop)
	<-r.done
}

func (r *reclaimer) run() {
	defer close(r.done)
	t := time.NewTicker(reclaimTick)
	defer t.Stop()
	for {
		select {
		case <-r.wake:
		case <-t.C:
		case ch := <-r.flush:
			r.pass()
			close(ch)
			continue
		case <-r.stop:
			r.pass()
			return
		}
		r.pass()
	}
}

func (r *reclaimer) pass() {
	r.mu.Lock()
	ids := r.pending
	r.pending = nil
	r.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	free, busy := r.s.leases.unleased(ids)
	if len(busy) > 0 {
		r.mu.Lock()
		r.pending = append(r.pending, busy...)
		r.mu.Unlock()
	}
	sem := make(chan struct{}, reclaimConcurrency)
	var wg sync.WaitGroup
	for _, id := range free {
		sem <- struct{}{}
		wg.Add(1)
		go func(id string) {
			defer func() { <-sem; wg.Done() }()
			if err := r.s.removeBlob(id); err != nil {
				slog.Warn("reclaim: deleting blob failed; GC will retry", "blob", id, "err", err)
			}
		}(id)
	}
	wg.Wait()
}
