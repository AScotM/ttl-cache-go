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

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	return h[i].expiration.Before(h[j].expiration)
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *MinHeap) Push(x interface{}) {
	item := x.(*HeapItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
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
	stopOnce    sync.Once
	cleanupDone chan struct{}
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
	HitRate     float64
}

func NewOptimizedTTLCache(capacity int, ttl time.Duration) *OptimizedTTLCache {
	if capacity <= 0 {
		capacity = 1000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	cache := &OptimizedTTLCache{
		capacity:    capacity,
		defaultTTL:  ttl,
		items:       make(map[string]*cacheItem),
		expiryHeap:  MinHeap{},
		stopChan:    make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}

	heap.Init(&cache.expiryHeap)
	go cache.cleanupWorker()

	return cache
}

func isExpired(now time.Time, expiration time.Time) bool {
	return !now.Before(expiration)
}

func (c *OptimizedTTLCache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

func (c *OptimizedTTLCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	if key == "" {
		return
	}
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

	c.cleanupExpiredLocked(now)

	for len(c.items) >= c.capacity {
		c.evictOneLocked(now)
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

func (c *OptimizedTTLCache) evictOneLocked(now time.Time) {
	if c.expiryHeap.Len() == 0 {
		return
	}

	if isExpired(now, c.expiryHeap[0].expiration) {
		heapItem := heap.Pop(&c.expiryHeap).(*HeapItem)
		if _, exists := c.items[heapItem.key]; exists {
			delete(c.items, heapItem.key)
			c.stats.Expirations++
		}
		return
	}

	var oldestKey string
	var oldestTime time.Time

	for key, item := range c.items {
		if oldestKey == "" || item.accessTime.Before(oldestTime) {
			oldestKey = key
			oldestTime = item.accessTime
		}
	}

	if oldestKey == "" {
		return
	}

	item := c.items[oldestKey]
	if item.heapItem.index >= 0 {
		heap.Remove(&c.expiryHeap, item.heapItem.index)
	}
	delete(c.items, oldestKey)
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

	if isExpired(now, item.heapItem.expiration) {
		if item.heapItem.index >= 0 {
			heap.Remove(&c.expiryHeap, item.heapItem.index)
		}
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(c.cleanupDone)

	for {
		select {
		case <-ticker.C:
			c.CleanupExpired()
		case <-c.stopChan:
			return
		}
	}
}

func (c *OptimizedTTLCache) CleanupExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cleanupExpiredLocked(time.Now())
}

func (c *OptimizedTTLCache) cleanupExpiredLocked(now time.Time) int {
	count := 0

	for c.expiryHeap.Len() > 0 {
		heapItem := c.expiryHeap[0]
		if !isExpired(now, heapItem.expiration) {
			break
		}

		heap.Pop(&c.expiryHeap)

		if _, exists := c.items[heapItem.key]; exists {
			delete(c.items, heapItem.key)
			count++
		}
	}

	c.stats.Expirations += int64(count)
	return count
}

func (c *OptimizedTTLCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())

	return len(c.items)
}

func (c *OptimizedTTLCache) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
		<-c.cleanupDone
	})
}

func (c *OptimizedTTLCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	if item.heapItem.index >= 0 {
		heap.Remove(&c.expiryHeap, item.heapItem.index)
	}
	delete(c.items, key)

	return true
}

func (c *OptimizedTTLCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, item := range c.items {
		if item.heapItem != nil {
			item.heapItem.index = -1
		}
	}

	c.items = make(map[string]*cacheItem)
	c.expiryHeap = MinHeap{}
	heap.Init(&c.expiryHeap)
	c.stats = CacheStats{}
}

func (c *OptimizedTTLCache) GetStats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := c.stats
	total := stats.Hits + stats.Misses

	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	return stats
}

func (c *OptimizedTTLCache) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())

	keys := make([]string, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}

	return keys
}

func (c *OptimizedTTLCache) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	now := time.Now()

	if isExpired(now, item.heapItem.expiration) {
		if item.heapItem.index >= 0 {
			heap.Remove(&c.expiryHeap, item.heapItem.index)
		}
		delete(c.items, key)
		c.stats.Expirations++
		return false
	}

	return true
}

func (c *OptimizedTTLCache) GetWithExpiry(key string) (interface{}, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return nil, time.Time{}, false
	}

	now := time.Now()

	if isExpired(now, item.heapItem.expiration) {
		if item.heapItem.index >= 0 {
			heap.Remove(&c.expiryHeap, item.heapItem.index)
		}
		delete(c.items, key)
		c.stats.Expirations++
		c.stats.Misses++
		return nil, time.Time{}, false
	}

	item.accessTime = now
	c.stats.Hits++

	return item.value, item.heapItem.expiration, true
}

func (c *OptimizedTTLCache) Peek(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return nil, false
	}

	now := time.Now()
	if isExpired(now, item.heapItem.expiration) {
		if item.heapItem.index >= 0 {
			heap.Remove(&c.expiryHeap, item.heapItem.index)
		}
		delete(c.items, key)
		c.stats.Expirations++
		c.stats.Misses++
		return nil, false
	}

	c.stats.Hits++
	return item.value, true
}

func (c *OptimizedTTLCache) Resize(newCapacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if newCapacity <= 0 {
		newCapacity = 1000
	}

	now := time.Now()
	c.cleanupExpiredLocked(now)

	for len(c.items) > newCapacity {
		c.evictOneLocked(now)
	}

	c.capacity = newCapacity
}

func (c *OptimizedTTLCache) VerifyHeap() (bool, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.expiryHeap) > len(c.items) {
		return false, fmt.Sprintf("heap has more items than map: heap=%d items=%d",
			len(c.expiryHeap), len(c.items))
	}

	for i, item := range c.expiryHeap {
		if item.index != i {
			return false, fmt.Sprintf("index mismatch at %d: expected %d, got %d", i, i, item.index)
		}

		_, exists := c.items[item.key]
		if !exists {
			continue
		}

		left := 2*i + 1
		right := 2*i + 2

		if left < len(c.expiryHeap) && c.expiryHeap[left].expiration.Before(item.expiration) {
			return false, fmt.Sprintf("heap order violation at %d and left child %d", i, left)
		}

		if right < len(c.expiryHeap) && c.expiryHeap[right].expiration.Before(item.expiration) {
			return false, fmt.Sprintf("heap order violation at %d and right child %d", i, right)
		}
	}

	return true, "heap is valid"
}

func (c *OptimizedTTLCache) Capacity() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.capacity
}

func (c *OptimizedTTLCache) GetMultiple(keys []string) map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[string]interface{}, len(keys))
	now := time.Now()

	for _, key := range keys {
		item, exists := c.items[key]
		if !exists {
			c.stats.Misses++
			continue
		}

		if isExpired(now, item.heapItem.expiration) {
			if item.heapItem.index >= 0 {
				heap.Remove(&c.expiryHeap, item.heapItem.index)
			}
			delete(c.items, key)
			c.stats.Expirations++
			c.stats.Misses++
			continue
		}

		item.accessTime = now
		result[key] = item.value
		c.stats.Hits++
	}

	return result
}

func main() {
	fmt.Println("=== Optimized TTL Cache with Heap ===")
	fmt.Println()

	cache := NewOptimizedTTLCache(5, 2*time.Second)
	defer cache.Stop()

	fmt.Printf("Initial capacity: %d\n", cache.Capacity())

	cache.SetWithTTL("fast", "expires in 1s", time.Second)
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

	fmt.Println("\nAdding 10 items with capacity 5...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("item%d", i)
		cache.SetWithTTL(key, i, time.Duration(i+1)*time.Second)
	}

	fmt.Printf("Size after adding: %d\n", cache.Size())

	cache.Resize(8)
	fmt.Printf("Capacity after resize: %d\n", cache.Capacity())

	heapValid, message := cache.VerifyHeap()
	fmt.Printf("Heap integrity check: %v - %s\n", heapValid, message)

	time.Sleep(6 * time.Second)

	fmt.Printf("Size after cleanup: %d\n", cache.Size())

	stats := cache.GetStats()
	fmt.Printf("Stats - Hits: %d, Misses: %d, Evictions: %d, Expirations: %d, HitRate: %.2f\n",
		stats.Hits, stats.Misses, stats.Evictions, stats.Expirations, stats.HitRate)

	keys := cache.Keys()
	fmt.Printf("Active keys: %v\n", keys)

	multipleKeys := []string{"item5", "item6", "item7", "nonexistent"}
	multipleResults := cache.GetMultiple(multipleKeys)
	fmt.Printf("Multiple get results: %v\n", multipleResults)

	heapValid, message = cache.VerifyHeap()
	fmt.Printf("Final heap integrity check: %v - %s\n", heapValid, message)

	fmt.Println("\n=== Demo Complete ===")
}
