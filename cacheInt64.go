// An LRU cached aimed at high concurrency
package ccache

import (
	"container/list"
	"sync/atomic"
	"time"
)

// The cache has a generic 'control' channel that is used to send
// messages to the worker. These are the messages that can be sent to it
type CacheInt64 struct {
	*ConfigurationInt64
	list        *list.List
	size        int64
	buckets     []*bucketInt64
	bucketMask  int64
	deletables  chan *ItemInt64
	promotables chan *ItemInt64
	control     chan interface{}
}

// Create a new cache with the specified configuration
// See ccache.Configure() for creating a configuration
func NewCacheInt64(config *ConfigurationInt64) *CacheInt64 {
	bks := NormalTo2N(int64(config.buckets))
	c := &CacheInt64{
		list:          list.New(),
		ConfigurationInt64: config,
		bucketMask:    int64(bks) - 1,
		buckets:       make([]*bucketInt64, bks),
		control:       make(chan interface{}),
	}
	for i := 0; i < int(bks); i++ {
		c.buckets[i] = &bucketInt64{
			lookup: make(map[int64]*ItemInt64),
		}
	}
	c.restart()
	return c
}

func (c *CacheInt64) ItemCount() int {
	count := 0
	for _, b := range c.buckets {
		count += b.itemCount()
	}
	return count
}

func (c *CacheInt64) DeletePrefix(prefix int64) int {
	count := 0
	for _, b := range c.buckets {
		count += b.deletePrefix(prefix, c.deletables)
	}
	return count
}

// Deletes all items that the matches func evaluates to true.
func (c *CacheInt64) DeleteFunc(matches func(key int64, item *ItemInt64) bool) int {
	count := 0
	for _, b := range c.buckets {
		count += b.deleteFunc(matches, c.deletables)
	}
	return count
}

// Get an item from the cache. Returns nil if the item wasn't found.
// This can return an expired item. Use item.Expired() to see if the item
// is expired and item.TTL() to see how long until the item expires (which
// will be negative for an already expired item).
func (c *CacheInt64) Get(key int64) *ItemInt64 {
	item := c.bucket(key).get(key)
	if item == nil {
		return nil
	}
	if !item.Expired() {
		c.promote(item)
	}
	return item
}

func (c *CacheInt64) GetWithNow(key int64, now time.Time) *ItemInt64 {
	item := c.bucket(key).get(key)
	if item == nil {
		return nil
	}
	if !item.IsExpired(now) {
		c.promote(item)
	}
	return item
}

//return item and expired
func (c *CacheInt64) GetWithNowNoPromote(key int64, now time.Time) (*ItemInt64, bool) {
	item := c.bucket(key).get(key)
	if item == nil {
		return nil, true
	}
	if item.expires > now.UnixNano() {
		//c.promote(item)
		return item, false
	}
	return item, true
}

func (c *CacheInt64) GetItem(key int64) (*ItemInt64) {
	return c.bucket(key).get(key)
	//if item == nil {
	//	return nil, true
	//}
	//if item.expires > now.UnixNano() {
	//	//c.promote(item)
	//	return item, false
	//}
	//return item, true
}

// Used when the cache was created with the Track() configuration option.
// Avoid otherwise
func (c *CacheInt64) TrackingGet(key int64) TrackedItem {
	item := c.Get(key)
	if item == nil {
		return NilTracked
	}
	item.track()
	return item
}

// Used when the cache was created with the Track() configuration option.
// Sets the item, and returns a tracked reference to it.
func (c *CacheInt64) TrackingSet(key int64, value interface{}, duration time.Duration) TrackedItem {
	return c.set(key, value, duration, true)
}

// Set the value in the cache for the specified duration
func (c *CacheInt64) Set(key int64, value interface{}, duration time.Duration) {
	c.set(key, value, duration, false)
}

// Replace the value if it exists, does not set if it doesn't.
// Returns true if the item existed an was replaced, false otherwise.
// Replace does not reset item's TTL
func (c *CacheInt64) Replace(key int64, value interface{}) bool {
	item := c.bucket(key).get(key)
	if item == nil {
		return false
	}
	c.Set(key, value, item.TTL())
	return true
}

// Attempts to get the value from the cache and calles fetch on a miss (missing
// or stale item). If fetch returns an error, no value is cached and the error
// is returned back to the caller.
func (c *CacheInt64) Fetch(key int64, duration time.Duration, fetch func() (interface{}, error)) (*ItemInt64, error) {
	item := c.Get(key)
	if item != nil && !item.Expired() {
		return item, nil
	}
	value, err := fetch()
	if err != nil {
		return nil, err
	}
	return c.set(key, value, duration, false), nil
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *CacheInt64) Delete(key int64) bool {
	item := c.bucket(key).delete(key)
	if item != nil {
		c.deletables <- item
		return true
	}
	return false
}

// Clears the cache
func (c *CacheInt64) Clear() {
	done := make(chan struct{})
	c.control <- clear{done: done}
	<-done
}

// Stops the background worker. Operations performed on the cache after Stop
// is called are likely to panic
func (c *CacheInt64) Stop() {
	close(c.promotables)
	<-c.control
}

// Gets the number of items removed from the cache due to memory pressure since
// the last time GetDropped was called
func (c *CacheInt64) GetDropped() int {
	res := make(chan int)
	c.control <- getDropped{res: res}
	return <-res
}

// Sets a new max size. That can result in a GC being run if the new maxium size
// is smaller than the cached size
func (c *CacheInt64) SetMaxSize(size int64) {
	c.control <- setMaxSize{size}
}

func (c *CacheInt64) restart() {
	c.deletables = make(chan *ItemInt64, c.deleteBuffer)
	c.promotables = make(chan *ItemInt64, c.promoteBuffer)
	c.control = make(chan interface{})
	go c.worker()
}

func (c *CacheInt64) deleteItem(bucket *bucketInt64, item *ItemInt64) {
	bucket.delete(item.key) //stop other GETs from getting it
	c.deletables <- item
}

func (c *CacheInt64) set(key int64, value interface{}, duration time.Duration, track bool) *ItemInt64 {
	item, existing := c.bucket(key).set(key, value, duration, track)
	if existing != nil {
		c.deletables <- existing
	}
	c.promote(item)
	return item
}

func (c *CacheInt64) GetIncrVal(key int64) ( r int64) {
	r,  _ = c.bucket(key).getIncrVal(key)
	return
}
//just incr no renew timeout
func (c *CacheInt64) Incr(key int64, n int64, duration time.Duration) int64 {
	r, item, _ := c.bucket(key).incr(key, n, duration, false)
	if item != nil{
		c.promote(item)
	}
	return r
}
//incr and then renew ttl
func (c *CacheInt64) IncrPromote(key int64, n int64, duration time.Duration) int64 {
	r, item, exi := c.bucket(key).incr(key, n, duration, false)
	if item != nil{
		c.promote(item)
	}else if exi != nil{
		c.promote(exi)
	}
	return r
}

func (c *CacheInt64) bucket(key int64) *bucketInt64 {
	return c.buckets[key&c.bucketMask]
}

func (c *CacheInt64) Promote(item *ItemInt64) {
	select {
	case c.promotables <- item:
	default:
	}

}
func (c *CacheInt64) promote(item *ItemInt64) {
	select {
	case c.promotables <- item:
	default:
	}
		
}

func (c *CacheInt64) worker() {
	defer close(c.control)
	dropped := 0
	for {
		select {
		case item, ok := <-c.promotables:
			if ok == false {
				goto drain
			}
			if c.doPromote(item) && c.size > c.maxSize {
				dropped += c.gc()
			}
		case item := <-c.deletables:
			c.doDelete(item)
		case control := <-c.control:
			switch msg := control.(type) {
			case getDropped:
				msg.res <- dropped
				dropped = 0
			case setMaxSize:
				c.maxSize = msg.size
				if c.size > c.maxSize {
					dropped += c.gc()
				}
			case clear:
				for _, bucket := range c.buckets {
					bucket.clear()
				}
				c.size = 0
				c.list = list.New()
				msg.done <- struct{}{}
			}
		}
	}

drain:
	for {
		select {
		case item := <-c.deletables:
			c.doDelete(item)
		default:
			close(c.deletables)
			return
		}
	}
}

func (c *CacheInt64) doDelete(item *ItemInt64) {
	if item.element == nil {
		item.promotions = -2
	} else {
		c.size -= item.size
		if c.onDelete != nil {
			c.onDelete(item)
		}
		c.list.Remove(item.element)
	}
}

func (c *CacheInt64) doPromote(item *ItemInt64) bool {
	//already deleted
	if item.promotions == -2 {
		return false
	}
	if item.element != nil { //not a new item
		if item.shouldPromote(c.getsPerPromote) {
			c.list.MoveToFront(item.element)
			item.promotions = 0
		}
		return false
	}

	c.size += item.size
	item.element = c.list.PushFront(item)
	return true
}

func (c *CacheInt64) gc() int {
	dropped := 0
	element := c.list.Back()
	for i := 0; i < c.itemsToPrune; i++ {
		if element == nil {
			return dropped
		}
		prev := element.Prev()
		item := element.Value.(*ItemInt64)
		if c.tracking == false || atomic.LoadInt32(&item.refCount) == 0 {
			c.bucket(item.key).delete(item.key)
			c.size -= item.size
			c.list.Remove(element)
			if c.onDelete != nil {
				c.onDelete(item)
			}
			dropped += 1
			item.promotions = -2
		}
		element = prev
	}
	return dropped
}
