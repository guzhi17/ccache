package ccache

import (
	"sync"
	"sync/atomic"
	"time"
)

type bucketInt64 struct {
	sync.RWMutex
	lookup map[int64]*ItemInt64
}

func (b *bucketInt64) itemCount() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.lookup)
}

func (b *bucketInt64) get(key int64) *ItemInt64 {
	b.RLock()
	defer b.RUnlock()
	return b.lookup[key]
}

func (b *bucketInt64) set(key int64, value interface{}, duration time.Duration, track bool) (*ItemInt64, *ItemInt64) {
	expires := time.Now().Add(duration).UnixNano()
	item := newItemInt64(key, value, expires, track)
	b.Lock()
	existing := b.lookup[key]
	b.lookup[key] = item
	b.Unlock()
	return item, existing
}


func (b *bucketInt64) getIncrVal(key int64) (r int64, exis *ItemInt64) {
	now := time.Now()
	tm := now.UnixNano()
	existing := b.get(key)
	if existing != nil && existing.expires > tm {
		if v, ok := existing.value.(*int64);ok{
			return *v, existing
		}
	}
	return 0, nil
}
func (b *bucketInt64) incr(key int64, value int64, duration time.Duration, track bool) (r int64, pro *ItemInt64, exis *ItemInt64) {
	return b.incrNow(key, value, time.Now(), duration, track)
}
func (b *bucketInt64) incrNow(key int64, value int64, now time.Time, duration time.Duration, track bool) (r int64, pro *ItemInt64, exis *ItemInt64) {
	tm := now.UnixNano()
	expires := now.Add(duration).UnixNano()
	item := newItemInt64(key, &value, expires, track)
	b.Lock()
	defer b.Unlock()
	existing := b.lookup[key]
	if existing != nil && existing.expires > tm {
		if v, ok := existing.value.(*int64);ok{
			*v += value
			return *v, nil, existing
		}
	}
	b.lookup[key] = item
	return value, item, nil
}

func (b *bucketInt64) incrNowPromote(key int64, value int64, now time.Time, duration time.Duration, track bool) (r int64, pro *ItemInt64, exis *ItemInt64) {
	tm := now.UnixNano()
	expires := now.Add(duration).UnixNano()
	item := newItemInt64(key, &value, expires, track)
	b.Lock()
	defer b.Unlock()
	existing := b.lookup[key]
	if existing != nil && existing.expires > tm {
		if v, ok := existing.value.(*int64);ok{
			*v += value
			atomic.StoreInt64(&existing.expires, expires)
			return *v, nil, existing
		}
	}
	b.lookup[key] = item
	return value, item, nil
}

func (b *bucketInt64) delete(key int64) *ItemInt64 {
	b.Lock()
	item := b.lookup[key]
	delete(b.lookup, key)
	b.Unlock()
	return item
}

// This is an expensive operation, so we do what we can to optimize it and limit
// the impact it has on concurrent operations. Specifically, we:
// 1 - Do an initial iteration to collect matches. This allows us to do the
//     "expensive" prefix check (on all values) using only a read-lock
// 2 - Do a second iteration, under write lock, for the matched results to do
//     the actual deletion

// Also, this is the only place where the Bucket is aware of cache detail: the
// deletables channel. Passing it here lets us avoid iterating over matched items
// again in the cache. Further, we pass item to deletables BEFORE actually removing
// the item from the map. I'm pretty sure this is 100% fine, but it is unique.
// (We do this so that the write to the channel is under the read lock and not the
// write lock)
func (b *bucketInt64) deleteFunc(matches func(key int64, item *ItemInt64) bool, deletables chan *ItemInt64) int {
	lookup := b.lookup
	items := make([]*ItemInt64, 0)

	b.RLock()
	for key, item := range lookup {
		if matches(key, item) {
			deletables <- item
			items = append(items, item)
		}
	}
	b.RUnlock()

	if len(items) == 0 {
		// avoid the write lock if we can
		return 0
	}

	b.Lock()
	for _, item := range items {
		delete(lookup, item.key)
	}
	b.Unlock()
	return len(items)
}

func (b *bucketInt64) deletePrefix(prefix int64, deletables chan *ItemInt64) int {
	return b.deleteFunc(func(key int64, item *ItemInt64) bool {
		return (key & prefix) == prefix
	}, deletables)
}

func (b *bucketInt64) clear() {
	b.Lock()
	b.lookup = make(map[int64]*ItemInt64)
	b.Unlock()
}
