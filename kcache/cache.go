package kcache

import (
	"fmt"
	"kcache/kcache/lru"
	"log"
	"math/rand"
	"sync"
	"time"
)

type cache struct {
	mu       sync.Mutex
	lru      *lru.Cache
	capacity int64 // 缓存最大容量
	exmp     map[string]interface{}
}

func newCache(capacity int64) *cache {
	return &cache{capacity: capacity, exmp: make(map[string]interface{})}
}

func (c *cache) add(key string, value ByteView, expire time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.lru = lru.New(c.capacity, nil)
	}
	c.lru.Add(key, value, expire)

	if expire.IsZero() {
		delete(c.exmp, key)
	} else {
		c.exmp[key] = 1
	}

}

func (c *cache) get(key string) (ByteView, int, bool) {
	if c.lru == nil {
		return ByteView{}, -1, false
	}
	// 注意：Get操作需要修改lru中的双向链表，需要使用互斥锁。
	c.mu.Lock()
	defer c.mu.Unlock()

	v, ex, ok := c.lru.Get(key)

	if ex == -1 && !ok {
		delete(c.exmp, key)
	}

	if ok {
		return v.(ByteView), ex, true
	}

	return ByteView{}, -1, false
}

func (c *cache) del(key string) bool {
	if c.lru == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.lru.Hashmap[key]; ok {
		c.lru.RemoveElement(ele)
		delete(c.exmp, key)
		return true
	}

	return true
}

func (c *cache) CheckEx() {
	for {
		time.Sleep(5 * time.Second)

		l := len(c.exmp)

		if l == 0 {
			continue
		}

		keys := make([]string, 0, l)
		for k := range c.exmp {
			keys = append(keys, k)
		}

		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		ll := l/5 + 1

		cl := keys[:ll]

		exk := make([]string, 0)
		for _, key := range cl {
			if c.lru.IsEx(key) {
				c.del(key)
				exk = append(exk, key)
			}
		}

		if len(exk) != 0 {
			log.Println("Del ex keys:")
			fmt.Println(exk)
		}

	}
}
