package lru

// lru 包实现了使用最近最久未使用使用算法的缓存功能
// 用于cache内存不足情况下 移除相应缓存记录
// Warning: lru包不提供并发一致机制
// TODO: 实现lru-k算法

import (
	"container/list"
	"log"
	"time"
	//"unsafe"
)

// Lengthable 接口指明对象可以获取自身占有内存空间大小 以字节为单位
type Lengthable interface {
	Len() int
}

// Value 定义双向链表节点所存储的对象
type Value struct {
	key    string
	value  Lengthable
	expire time.Time
}

// OnEliminated 当key-value被淘汰时 执行的处理函数
type OnEliminated func(key string, value Lengthable)

// Cache 是LRU算法实现的缓存
// 参考Leetcode使用哈希表+双向链表实现LRU
type Cache struct {
	capacity         int64 // Cache 最大容量(Byte)
	length           int64 // Cache 当前容量(Byte)
	Hashmap          map[string]*list.Element
	doublyLinkedList *list.List // 链头表示最近使用

	callback OnEliminated
}

// New 创建指定最大容量的LRU缓存。
// 当maxBytes为0时，代表cache无内存限制，无限存放。
func New(maxBytes int64, callback OnEliminated) *Cache {
	return &Cache{
		capacity:         maxBytes,
		Hashmap:          make(map[string]*list.Element),
		doublyLinkedList: list.New(),
		callback:         callback,
	}
}

func (c *Cache) IsEx(key string) bool {
	if elem, ok := c.Hashmap[key]; ok {
		entry := elem.Value.(*Value)
		if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
			return true
		}

	}
	return false
}

// Get 从缓存获取对应key的value。
// ok 指明查询结果 false代表查无此key
func (c *Cache) Get(key string) (value Lengthable, ex int, ok bool) {
	if elem, ok := c.Hashmap[key]; ok {
		entry := elem.Value.(*Value)

		if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
			c.RemoveElement(elem)
			log.Println(key + " is expired")
			return nil, -1, false
		}

		c.doublyLinkedList.MoveToFront(elem)
		if entry.expire.IsZero() {
			return entry.value, 0, true
		}

		return entry.value, (int(entry.expire.Sub(time.Now()).Seconds())), true
	}
	return
}

func (c *Cache) Add(key string, value Lengthable, expire time.Time) {
	kvSize := int64(len(key)) + int64(value.Len()) + 24 //int64(unsafe.Sizeof(time.Time{}))
	// cache 容量检查
	for c.capacity != 0 && c.length+kvSize > c.capacity {
		c.Remove()
	}

	if elem, ok := c.Hashmap[key]; ok {
		// 更新缓存key值
		c.doublyLinkedList.MoveToFront(elem)
		oldEntry := elem.Value.(*Value)
		// 先更新写入字节 再更新
		c.length += int64(value.Len()) - int64(oldEntry.value.Len())
		oldEntry.value = value
		oldEntry.expire = expire
	} else {
		// 新增缓存key
		elem := c.doublyLinkedList.PushFront(&Value{key: key, value: value, expire: expire})
		c.Hashmap[key] = elem
		c.length += kvSize
	}

}

// Remove 淘汰一枚最近最不常用缓存
func (c *Cache) Remove() {
	tailElem := c.doublyLinkedList.Back()
	if tailElem != nil {
		entry := tailElem.Value.(*Value)
		k, v := entry.key, entry.value
		delete(c.Hashmap, k)                            // 移除映射
		c.doublyLinkedList.Remove(tailElem)             // 移除缓存
		c.length -= int64(len(k)) + int64(v.Len()) + 24 // 更新占用内存情况
		// 移除后的善后处理
		if c.callback != nil {
			c.callback(k, v)
		}
	}
}

func (c *Cache) RemoveElement(e *list.Element) {
	c.doublyLinkedList.Remove(e)
	entry := e.Value.(*Value)
	delete(c.Hashmap, entry.key)
	c.length -= int64(len(entry.key)) + int64(entry.value.Len())
}
