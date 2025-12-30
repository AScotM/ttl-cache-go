package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type HeapItem struct {
	key        string
	expiration time.Time
	index      int
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].expiration.Before(h[j].expiration) }
func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *MinHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*HeapItem)
	item.index = n
	*h = append(*h, item)
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

type OptimizedTTLCache struct {
	capacity    int
	defaultTTL  time.Duration
	items       map[string]*cacheItem
	expiryHeap  MinHeap
	mu          sync.RWMutex
	stats       CacheStats
	stopChan    chan struct{}
	stopped     bool
}

type cacheItem struct {
	value      interface{}
	heapItem   *HeapItem
	created    time.Time
	accessTime time.Time
}

type CacheStats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Expirations int64
}

func NewOptimizedTTLCache(capacity int, ttl time.Duration) *OptimizedTTLCache {
	if capacity <= 0 {
		capacity = 1000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	cache := &OptimizedTTLCache{
		capacity:   capacity,
		defaultTTL: ttl,
		items:      make(map[string]*cacheItem),
		expiryHeap: MinHeap{},
		stopChan:   make(chan struct{}),
	}

	heap.Init(&cache.expiryHeap)
	go cache.cleanupWorker()
	return cache
}

func (c *OptimizedTTLCache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

func (c *OptimizedTTLCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	expiration := now.Add(ttl)

	if existing, exists := c.items[key]; exists {
		existing.value = value
		existing.accessTime = now
		existing.heapItem.expiration = expiration
		heap.Fix(&c.expiryHeap, existing.heapItem.index)
		return
	}

	if len(c.items) >= c.capacity {
		c.evictOne()
	}

	heapItem := &HeapItem{
		key:        key,
		expiration: expiration,
	}

	item := &cacheItem{
		value:      value,
		heapItem:   heapItem,
		created:    now,
		accessTime: now,
	}

	c.items[key] = item
	heap.Push(&c.expiryHeap, heapItem)
}

func (c *OptimizedTTLCache) evictOne() {
	if c.expiryHeap.Len() == 0 {
		return
	}

	heapItem := heap.Pop(&c.expiryHeap).(*HeapItem)
	delete(c.items, heapItem.key)
	c.stats.Evictions++
}

func (c *OptimizedTTLCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return nil, false
	}

	now := time.Now()
	if now.After(item.heapItem.expiration) {
		heap.Remove(&c.expiryHeap, item.heapItem.index)
		delete(c.items, key)
		c.stats.Expirations++
		c.stats.Misses++
		return nil, false
	}

	item.accessTime = now
	c.stats.Hits++
	return item.value, true
}

func (c *OptimizedTTLCache) cleanupWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.CleanupExpired()
		case <-c.stopChan:
			c.mu.Lock()
			c.stopped = true
			c.mu.Unlock()
			return
		}
	}
}

func (c *OptimizedTTLCache) CleanupExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	now := time.Now()

	for c.expiryHeap.Len() > 0 {
		heapItem := c.expiryHeap[0]
		if now.Before(heapItem.expiration) {
			break
		}

		heap.Pop(&c.expiryHeap)
		delete(c.items, heapItem.key)
		count++
		c.stats.Expirations++
	}

	return count
}

func (c *OptimizedTTLCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *OptimizedTTLCache) Stop() {
	c.mu.Lock()
	if !c.stopped {
		close(c.stopChan)
		c.stopped = true
	}
	c.mu.Unlock()
}

func (c *OptimizedTTLCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	heap.Remove(&c.expiryHeap, item.heapItem.index)
	delete(c.items, key)
	return true
}

func (c *OptimizedTTLCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheItem)
	c.expiryHeap = MinHeap{}
	heap.Init(&c.expiryHeap)
	c.stats = CacheStats{}
}

func (c *OptimizedTTLCache) GetStats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *OptimizedTTLCache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.items))
	now := time.Now()

	for key, item := range c.items {
		if now.Before(item.heapItem.expiration) {
			keys = append(keys, key)
		}
	}

	return keys
}

func (c *OptimizedTTLCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	return time.Now().Before(item.heapItem.expiration)
}

func (c *OptimizedTTLCache) GetWithExpiry(key string) (interface{}, time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return nil, time.Time{}, false
	}

	now := time.Now()
	if now.After(item.heapItem.expiration) {
		c.stats.Misses++
		c.stats.Expirations++
		return nil, time.Time{}, false
	}

	c.stats.Hits++
	return item.value, item.heapItem.expiration, true
}

func main() {
	fmt.Println("=== Optimized TTL Cache with Heap ===")
	fmt.Println()

	cache := NewOptimizedTTLCache(5, 2*time.Second)
	defer cache.Stop()

	cache.SetWithTTL("fast", "expires in 1s", 1*time.Second)
	cache.SetWithTTL("medium", "expires in 3s", 3*time.Second)
	cache.SetWithTTL("slow", "expires in 5s", 5*time.Second)

	fmt.Printf("Initial size: %d\n", cache.Size())

	time.Sleep(2 * time.Second)

	items := []string{"fast", "medium", "slow"}
	for _, key := range items {
		if val, found := cache.Get(key); found {
			fmt.Printf("%s: %v\n", key, val)
		} else {
			fmt.Printf("%s: expired\n", key)
		}
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("item%d", i)
		cache.SetWithTTL(key, i, time.Duration(i+1)*time.Second)
	}

	fmt.Printf("Size after adding: %d\n", cache.Size())

	time.Sleep(6 * time.Second)

	fmt.Printf("Size after cleanup: %d\n", cache.Size())

	stats := cache.GetStats()
	fmt.Printf("Stats - Hits: %d, Misses: %d, Evictions: %d, Expirations: %d\n",
		stats.Hits, stats.Misses, stats.Evictions, stats.Expirations)

	keys := cache.Keys()
	fmt.Printf("Active keys: %v\n", keys)

	fmt.Println("\n=== Demo Complete ===")
}
