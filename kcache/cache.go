package kcache

import (
	"kcache/kcache/lru"
	"sync"
)

type cache struct {
	mu       sync.Mutex
	lru      *lru.Cache
	capacity int64 // 缓存最大容量
}

func newCache(capacity int64) *cache {
	return &cache{capacity: capacity}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = lru.New(c.capacity, nil)
	}
	c.lru.Add(key, value)
}

func (c *cache) get(key string) (ByteView, bool) {
	if c.lru == nil {
		return ByteView{}, false
	}
	// 注意：Get操作需要修改lru中的双向链表，需要使用互斥锁。
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), true
	}
	return ByteView{}, false
}
