package main

import (
	"container/heap"
	"container/list"
	"fmt"
	"sort"
	"sync"
	"time"
)

type expiryItem[K comparable] struct {
	key        K
	expiration time.Time
	index      int
}

type expiryHeap[K comparable] []*expiryItem[K]

func (h expiryHeap[K]) Len() int {
	return len(h)
}

func (h expiryHeap[K]) Less(i, j int) bool {
	return h[i].expiration.Before(h[j].expiration)
}

func (h expiryHeap[K]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *expiryHeap[K]) Push(x any) {
	item := x.(*expiryItem[K])
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *expiryHeap[K]) Pop() any {
	old := *h
	n := len(old)

	if n == 0 {
		return nil
	}

	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]

	return item
}

type cacheItem[K comparable, V any] struct {
	value      V
	expiry     *expiryItem[K]
	lruElement *list.Element
}

type CacheStats struct {
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Expirations uint64
	Deletes     uint64
	Sets        uint64
	Updates     uint64
	HitRate     float64
}

type CacheSnapshot struct {
	Size         int
	Capacity     int
	DefaultTTL   time.Duration
	NextExpiryIn time.Duration
	Stats        CacheStats
}

type Cache[K comparable, V any] struct {
	mu         sync.RWMutex
	capacity   int
	defaultTTL time.Duration
	items      map[K]*cacheItem[K, V]
	expiries   expiryHeap[K]
	lru        *list.List
	stats      CacheStats

	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func NewCache[K comparable, V any](capacity int, defaultTTL time.Duration) *Cache[K, V] {
	if capacity <= 0 {
		capacity = 1000
	}

	if defaultTTL <= 0 {
		defaultTTL = 5 * time.Minute
	}

	c := &Cache[K, V]{
		capacity:   capacity,
		defaultTTL: defaultTTL,
		items:      make(map[K]*cacheItem[K, V], capacity),
		expiries:   make(expiryHeap[K], 0, capacity),
		lru:        list.New(),
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}

	heap.Init(&c.expiries)
	go c.cleanupWorker()

	return c
}

func expired(now, expiration time.Time) bool {
	return !now.Before(expiration)
}

func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}

	now := time.Now()
	expiration := now.Add(ttl)

	c.mu.Lock()

	c.cleanupExpiredLocked(now)

	if item, exists := c.items[key]; exists {
		oldEarliest := c.earliestExpirationLocked()

		item.value = value
		item.expiry.expiration = expiration
		heap.Fix(&c.expiries, item.expiry.index)
		c.lru.MoveToFront(item.lruElement)

		c.stats.Sets++
		c.stats.Updates++

		newEarliest := c.earliestExpirationLocked()
		c.mu.Unlock()

		if deadlineChanged(oldEarliest, newEarliest) {
			c.signalWake()
		}

		return
	}

	for len(c.items) >= c.capacity {
		c.evictLRULocked()
	}

	oldEarliest := c.earliestExpirationLocked()

	expiry := &expiryItem[K]{
		key:        key,
		expiration: expiration,
		index:      -1,
	}

	lruElement := c.lru.PushFront(key)

	c.items[key] = &cacheItem[K, V]{
		value:      value,
		expiry:     expiry,
		lruElement: lruElement,
	}

	heap.Push(&c.expiries, expiry)
	c.stats.Sets++

	newEarliest := c.earliestExpirationLocked()
	c.mu.Unlock()

	if deadlineChanged(oldEarliest, newEarliest) {
		c.signalWake()
	}
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V

	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return zero, false
	}

	now := time.Now()
	if expired(now, item.expiry.expiration) {
		c.removeItemLocked(key, item, true, false)
		c.stats.Misses++
		return zero, false
	}

	c.lru.MoveToFront(item.lruElement)
	c.stats.Hits++

	return item.value, true
}

func (c *Cache[K, V]) GetWithExpiry(key K) (V, time.Time, bool) {
	var zero V

	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.stats.Misses++
		return zero, time.Time{}, false
	}

	now := time.Now()
	if expired(now, item.expiry.expiration) {
		c.removeItemLocked(key, item, true, false)
		c.stats.Misses++
		return zero, time.Time{}, false
	}

	c.lru.MoveToFront(item.lruElement)
	c.stats.Hits++

	return item.value, item.expiry.expiration, true
}

func (c *Cache[K, V]) Peek(key K) (V, bool) {
	var zero V

	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return zero, false
	}

	if expired(time.Now(), item.expiry.expiration) {
		c.removeItemLocked(key, item, true, false)
		return zero, false
	}

	return item.value, true
}

func (c *Cache[K, V]) Contains(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		return false
	}

	if expired(time.Now(), item.expiry.expiration) {
		c.removeItemLocked(key, item, true, false)
		return false
	}

	return true
}

func (c *Cache[K, V]) Delete(key K) bool {
	c.mu.Lock()

	item, exists := c.items[key]
	if !exists {
		c.mu.Unlock()
		return false
	}

	wasEarliest := item.expiry.index == 0
	c.removeItemLocked(key, item, false, true)
	c.mu.Unlock()

	if wasEarliest {
		c.signalWake()
	}

	return true
}

func (c *Cache[K, V]) Clear() {
	c.mu.Lock()

	for _, item := range c.items {
		item.expiry.index = -1
	}

	c.items = make(map[K]*cacheItem[K, V], c.capacity)
	c.expiries = make(expiryHeap[K], 0, c.capacity)
	heap.Init(&c.expiries)
	c.lru.Init()
	c.stats = CacheStats{}

	c.mu.Unlock()
	c.signalWake()
}

func (c *Cache[K, V]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())
	return len(c.items)
}

func (c *Cache[K, V]) Capacity() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.capacity
}

func (c *Cache[K, V]) DefaultTTL() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.defaultTTL
}

func (c *Cache[K, V]) SetDefaultTTL(ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}

	c.mu.Lock()
	c.defaultTTL = ttl
	c.mu.Unlock()

	return true
}

func (c *Cache[K, V]) Resize(newCapacity int) bool {
	if newCapacity <= 0 {
		return false
	}

	c.mu.Lock()

	c.cleanupExpiredLocked(time.Now())
	c.capacity = newCapacity

	for len(c.items) > c.capacity {
		c.evictLRULocked()
	}

	c.mu.Unlock()

	return true
}

func (c *Cache[K, V]) Keys() []K {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())

	keys := make([]K, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}

	return keys
}

func (c *Cache[K, V]) KeysByRecency() []K {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanupExpiredLocked(time.Now())

	keys := make([]K, 0, len(c.items))
	for element := c.lru.Front(); element != nil; element = element.Next() {
		keys = append(keys, element.Value.(K))
	}

	return keys
}

func (c *Cache[K, V]) GetMultiple(keys []K) map[K]V {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[K]V, len(keys))
	now := time.Now()

	for _, key := range keys {
		item, exists := c.items[key]
		if !exists {
			c.stats.Misses++
			continue
		}

		if expired(now, item.expiry.expiration) {
			c.removeItemLocked(key, item, true, false)
			c.stats.Misses++
			continue
		}

		c.lru.MoveToFront(item.lruElement)
		c.stats.Hits++
		result[key] = item.value
	}

	return result
}

func (c *Cache[K, V]) CleanupExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cleanupExpiredLocked(time.Now())
}

func (c *Cache[K, V]) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := c.stats
	total := stats.Hits + stats.Misses

	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	return stats
}

func (c *Cache[K, V]) Snapshot() CacheSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	c.cleanupExpiredLocked(now)

	stats := c.stats
	total := stats.Hits + stats.Misses
	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	var nextExpiry time.Duration
	if len(c.expiries) > 0 {
		nextExpiry = time.Until(c.expiries[0].expiration)
		if nextExpiry < 0 {
			nextExpiry = 0
		}
	}

	return CacheSnapshot{
		Size:         len(c.items),
		Capacity:     c.capacity,
		DefaultTTL:   c.defaultTTL,
		NextExpiryIn: nextExpiry,
		Stats:        stats,
	}
}

func (c *Cache[K, V]) Verify() (bool, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.items) != len(c.expiries) {
		return false, fmt.Sprintf(
			"map/heap size mismatch: map=%d heap=%d",
			len(c.items),
			len(c.expiries),
		)
	}

	if len(c.items) != c.lru.Len() {
		return false, fmt.Sprintf(
			"map/LRU size mismatch: map=%d lru=%d",
			len(c.items),
			c.lru.Len(),
		)
	}

	heapKeys := make(map[K]struct{}, len(c.expiries))

	for i, expiry := range c.expiries {
		if expiry == nil {
			return false, fmt.Sprintf("nil heap item at index %d", i)
		}

		if expiry.index != i {
			return false, fmt.Sprintf(
				"heap index mismatch at %d: stored=%d",
				i,
				expiry.index,
			)
		}

		if _, duplicate := heapKeys[expiry.key]; duplicate {
			return false, fmt.Sprintf("duplicate heap key at index %d", i)
		}

		heapKeys[expiry.key] = struct{}{}

		item, exists := c.items[expiry.key]
		if !exists {
			return false, fmt.Sprintf("heap key missing from map at index %d", i)
		}

		if item.expiry != expiry {
			return false, fmt.Sprintf("heap pointer mismatch at index %d", i)
		}

		left := 2*i + 1
		right := 2*i + 2

		if left < len(c.expiries) &&
			c.expiries[left].expiration.Before(expiry.expiration) {
			return false, fmt.Sprintf(
				"heap order violation between %d and %d",
				i,
				left,
			)
		}

		if right < len(c.expiries) &&
			c.expiries[right].expiration.Before(expiry.expiration) {
			return false, fmt.Sprintf(
				"heap order violation between %d and %d",
				i,
				right,
			)
		}
	}

	lruKeys := make(map[K]struct{}, c.lru.Len())

	for element := c.lru.Front(); element != nil; element = element.Next() {
		key, ok := element.Value.(K)
		if !ok {
			return false, "invalid key type in LRU list"
		}

		if _, duplicate := lruKeys[key]; duplicate {
			return false, "duplicate key in LRU list"
		}

		lruKeys[key] = struct{}{}

		item, exists := c.items[key]
		if !exists {
			return false, "LRU key missing from map"
		}

		if item.lruElement != element {
			return false, "LRU element pointer mismatch"
		}
	}

	for key, item := range c.items {
		if item == nil {
			return false, "nil cache item"
		}

		if item.expiry == nil {
			return false, "cache item has nil expiry"
		}

		if item.lruElement == nil {
			return false, "cache item has nil LRU element"
		}

		if _, exists := heapKeys[key]; !exists {
			return false, "map key missing from expiry heap"
		}

		if _, exists := lruKeys[key]; !exists {
			return false, "map key missing from LRU list"
		}
	}

	return true, "cache structures are valid"
}

func (c *Cache[K, V]) Stop() {
	c.stopOnce.Do(func() {
		close(c.stop)
		<-c.done
	})
}

func (c *Cache[K, V]) cleanupWorker() {
	defer close(c.done)

	var timer *time.Timer
	var timerChannel <-chan time.Time

	for {
		delay, hasExpiry := c.nextCleanupDelay()

		if hasExpiry {
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

				timer.Reset(delay)
			}

			timerChannel = timer.C
		} else {
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}

			timerChannel = nil
		}

		select {
		case <-timerChannel:
			c.CleanupExpired()

		case <-c.wake:

		case <-c.stop:
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			return
		}
	}
}

func (c *Cache[K, V]) nextCleanupDelay() (time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.expiries) == 0 {
		return 0, false
	}

	delay := time.Until(c.expiries[0].expiration)
	if delay < 0 {
		delay = 0
	}

	return delay, true
}

func (c *Cache[K, V]) cleanupExpiredLocked(now time.Time) int {
	count := 0

	for len(c.expiries) > 0 {
		expiry := c.expiries[0]

		if !expired(now, expiry.expiration) {
			break
		}

		item, exists := c.items[expiry.key]

		if !exists {
			heap.Pop(&c.expiries)
			continue
		}

		c.removeItemLocked(expiry.key, item, true, false)
		count++
	}

	return count
}

func (c *Cache[K, V]) removeItemLocked(
	key K,
	item *cacheItem[K, V],
	expiration bool,
	deletion bool,
) {
	if item.expiry != nil && item.expiry.index >= 0 {
		heap.Remove(&c.expiries, item.expiry.index)
	}

	if item.lruElement != nil {
		c.lru.Remove(item.lruElement)
	}

	delete(c.items, key)

	if expiration {
		c.stats.Expirations++
	}

	if deletion {
		c.stats.Deletes++
	}
}

func (c *Cache[K, V]) evictLRULocked() bool {
	element := c.lru.Back()
	if element == nil {
		return false
	}

	key := element.Value.(K)
	item, exists := c.items[key]

	if !exists {
		c.lru.Remove(element)
		return false
	}

	c.removeItemLocked(key, item, false, false)
	c.stats.Evictions++

	return true
}

func (c *Cache[K, V]) earliestExpirationLocked() time.Time {
	if len(c.expiries) == 0 {
		return time.Time{}
	}

	return c.expiries[0].expiration
}

func deadlineChanged(a, b time.Time) bool {
	if a.IsZero() != b.IsZero() {
		return true
	}

	if a.IsZero() {
		return false
	}

	return !a.Equal(b)
}

func (c *Cache[K, V]) signalWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func main() {
	cache := NewCache[string, int](5, 5*time.Second)
	defer cache.Stop()

	fmt.Println("TTL + LRU Cache")
	fmt.Println()

	cache.SetWithTTL("alpha", 10, 2*time.Second)
	cache.SetWithTTL("beta", 20, 4*time.Second)
	cache.SetWithTTL("gamma", 30, 6*time.Second)
	cache.Set("delta", 40)
	cache.Set("epsilon", 50)

	fmt.Printf("Size: %d\n", cache.Size())
	fmt.Printf("Capacity: %d\n", cache.Capacity())
	fmt.Printf("LRU order: %v\n", cache.KeysByRecency())

	if value, ok := cache.Get("alpha"); ok {
		fmt.Printf("alpha: %d\n", value)
	}

	if value, ok := cache.Get("gamma"); ok {
		fmt.Printf("gamma: %d\n", value)
	}

	fmt.Printf("LRU after reads: %v\n", cache.KeysByRecency())

	cache.Set("zeta", 60)

	fmt.Printf("LRU after capacity eviction: %v\n", cache.KeysByRecency())

	if _, ok := cache.Peek("beta"); ok {
		fmt.Println("beta is still present")
	} else {
		fmt.Println("beta is not present")
	}

	time.Sleep(3 * time.Second)

	if _, ok := cache.Get("alpha"); !ok {
		fmt.Println("alpha expired")
	}

	cache.SetWithTTL("short", 100, time.Second)
	cache.SetWithTTL("long", 200, 10*time.Second)

	results := cache.GetMultiple([]string{
		"gamma",
		"delta",
		"short",
		"missing",
	})

	resultKeys := make([]string, 0, len(results))
	for key := range results {
		resultKeys = append(resultKeys, key)
	}
	sort.Strings(resultKeys)

	fmt.Println("Multiple get:")
	for _, key := range resultKeys {
		fmt.Printf("%s=%d\n", key, results[key])
	}

	valid, message := cache.Verify()
	fmt.Printf("Integrity: %v - %s\n", valid, message)

	snapshot := cache.Snapshot()

	fmt.Printf("Size: %d/%d\n", snapshot.Size, snapshot.Capacity)
	fmt.Printf("Default TTL: %s\n", snapshot.DefaultTTL)
	fmt.Printf("Next expiry: %s\n", snapshot.NextExpiryIn.Round(time.Millisecond))
	fmt.Printf(
		"Stats: hits=%d misses=%d evictions=%d expirations=%d deletes=%d sets=%d updates=%d hit-rate=%.2f%%\n",
		snapshot.Stats.Hits,
		snapshot.Stats.Misses,
		snapshot.Stats.Evictions,
		snapshot.Stats.Expirations,
		snapshot.Stats.Deletes,
		snapshot.Stats.Sets,
		snapshot.Stats.Updates,
		snapshot.Stats.HitRate*100,
	)

	fmt.Printf("Keys: %v\n", cache.KeysByRecency())

	cache.Resize(3)

	fmt.Printf("After resize: %v\n", cache.KeysByRecency())

	valid, message = cache.Verify()
	fmt.Printf("Final integrity: %v - %s\n", valid, message)
}
