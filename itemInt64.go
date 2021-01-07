package ccache

import (
	"container/list"
	"sync/atomic"
	"time"
)



type ItemInt64 struct {
	key        int64
	group      string
	promotions int32
	refCount   int32
	expires    int64
	size       int64
	value      interface{}
	element    *list.Element
}

func newItemInt64(key int64, value interface{}, expires int64, track bool) *ItemInt64 {
	size := int64(1)
	if sized, ok := value.(Sized); ok {
		size = sized.Size()
	}
	item := &ItemInt64{
		key:        key,
		value:      value,
		promotions: 0,
		size:       size,
		expires:    expires,
	}
	if track {
		item.refCount = 1
	}
	return item
}

func (i *ItemInt64) shouldPromote(getsPerPromote int32) bool {
	i.promotions += 1
	return i.promotions == getsPerPromote
}

func (i *ItemInt64) Value() interface{} {
	return i.value
}

func (i *ItemInt64) track() {
	atomic.AddInt32(&i.refCount, 1)
}

func (i *ItemInt64) Release() {
	atomic.AddInt32(&i.refCount, -1)
}

func (i *ItemInt64) Expired() bool {
	expires := atomic.LoadInt64(&i.expires)
	return expires < time.Now().UnixNano()
}

func (i *ItemInt64) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-time.Now().UnixNano())
}

func (i *ItemInt64) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *ItemInt64) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, time.Now().Add(duration).UnixNano())
}
