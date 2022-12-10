package cache

import (
	"sync"

	"github.com/wang-zm001/DistributedDB/cache/lru"
)

type Cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
}

func (c *Cache) add(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = lru.NewCache(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

func (c *Cache) get(key string) (value []byte, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}

	if v, ok := c.get(key); ok {
		return v, true
	}
	return nil, false
}
