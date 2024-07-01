package kcache

import (
	"fmt"
	"kcache/kcache/singleflight"
	"log"
	"sync"
	"time"
)

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

type Retriever interface {
	retrieve(string) ([]byte, error)
}

type RetrieverFunc func(key string) ([]byte, error)

// RetrieverFunc 通过实现retrieve方法，使得任意匿名函数func
// 通过被RetrieverFunc(func)类型强制转换后，实现了 Retriever 接口的能力
func (f RetrieverFunc) retrieve(key string) ([]byte, error) {
	return f(key)
}

// Group 提供命名管理缓存/填充缓存的能力
type Group struct {
	name      string
	cache     *cache
	hotcache  *cache
	retriever Retriever
	server    Picker
	flight    *singleflight.Flight
}

// NewGroup 创建一个新的缓存空间
func NewGroup(addr string, name string, maxBytes int64, retriever Retriever) *Group {
	if retriever == nil {
		panic("Group retriever must be existed!")
	}

	svr, err := NewServer(addr)
	if err != nil {
		log.Fatal(err)
	}

	g := &Group{
		name:      name,
		cache:     newCache(maxBytes),
		hotcache:  newCache(maxBytes / 10),
		retriever: retriever,
		flight:    &singleflight.Flight{},
		server:    svr, // 将服务与cache绑定 因为cache和server是解耦合的
	}

	mu.Lock()
	groups[name] = g
	mu.Unlock()

	// 启动服务(注册服务至etcd/计算一致性哈希...)
	go func() {
		// Start将不会return 除非服务stop或者抛出error
		err = svr.Start()
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return g
}

func (g *Group) RegisterSvr(p Picker) {
	if g.server != nil {
		panic("group had been registered server")
	}
	g.server = p
}

// GetGroup 获取对应命名空间的缓存
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func DestroyGroup(name string) {
	g := GetGroup(name)
	if g != nil {
		svr := g.server.(*Server)
		svr.Stop()
		delete(groups, name)
		log.Printf("Destroy cache [%s %s]", name, svr.addr)
	}
}

// 先看本地缓存
func (g *Group) Get(key string) (ByteView, error) {
	log.Printf("Get " + key)

	if key == "" {
		return ByteView{}, fmt.Errorf("key required")
	}
	if value, ok := g.cache.get(key); ok {
		log.Println("cache hit")
		return value, nil
	}
	if value, ok := g.hotcache.get(key); ok {
		log.Println("hot cache hit")
		return value, nil
	}

	log.Println("local cache missing, get it from remote")

	return g.load(key)
}

// 从peer获取
func (g *Group) load(key string) (ByteView, error) {
	view, err := g.flight.Fly(key, func() (interface{}, error) {
		if g.server != nil {
			if fetcher, ok := g.server.Pick(key); ok {
				bytes, err := fetcher.Fetch(g.name, key)
				if err == nil {
					//return ByteView{b: cloneBytes(bytes)}, nil
					g.hotcache.add(key, ByteView{b: bytes})
					return ByteView{b: bytes}, nil
				}
				log.Printf("fail to get *%s* from peer, %s.\n", key, err.Error())
			}
		} else {
			fmt.Print("g.server == nil\n")
		}

		return g.getLocally(key)
	})
	if err == nil {
		return view.(ByteView), err
	}
	return ByteView{}, err
}

// 本地向Retriever取回数据并填充缓存
func (g *Group) getLocally(key string) (ByteView, error) {
	log.Printf("Get from retriever")

	bytes, err := g.retriever.retrieve(key)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b: cloneBytes(bytes)}

	g.cache.add(key, value)
	return value, nil
}
