package lru

import (
	"container/list"
	"fmt"
)

type Node struct {
	key int
	val int
}

type LruCache struct {
	ca int
	ll *list.List
	mp map[int]*list.Element
}

func (c *LruCache) Add(k, v int) {
	if ok := c.Get(k); ok {
		c.mp[k].Value.(*Node).val = v
	} else {
		nd := &Node{k, v}
		c.ll.PushFront(nd)
		if c.ll.Len() > c.ca {
			c.ll.Remove(c.ll.Back())
		}
	}
	c.Print()
}

func (c *LruCache) Get(k int) bool {
	if el, ok := c.mp[k]; ok {
		c.ll.MoveToFront(el)
		return ok
	} else {
		return ok
	}
}

func (c *LruCache) Print() {
	for e := c.ll.Front(); e != nil; e = e.Next() {
		nd := e.Value.(*Node)
		fmt.Println(nd.key, nd.val)
	}
	fmt.Println(" ")
}

func MyLruTest() {
	var c = LruCache{3, list.New(), make(map[int]*list.Element)}

	c.Add(1, 1)
	c.Add(2, 2)
	c.Add(3, 3)
	c.Add(1, 2)
	c.Add(4, 4)
}
