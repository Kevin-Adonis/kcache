package singleflight

import (
	"sync"
)

type packet struct {
	wg  sync.WaitGroup
	val interface{}
	ex  int
	err error
}

type Flight struct {
	mu     sync.Mutex
	gflight map[string]*packet
	sflight map[string]*packet
}

func (f *Flight) Fly(key string, fn func() (interface{},int, error)) (interface{},int, error) {
	f.mu.Lock()
	if f.gflight == nil {
		f.gflight = make(map[string]*packet)
	}

	if p, ok := f.gflight[key]; ok {
		f.mu.Unlock()
		p.wg.Wait() //后入的会进入这个if，然后阻塞在wait
		return p.val,p.ex,p.err
	}

	//先入的会解决这个请求
	p := new(packet)
	p.wg.Add(1)
	f.gflight[key] = p
	f.mu.Unlock()

	p.val,p.ex, p.err = fn()
	p.wg.Done()

	f.mu.Lock()
	delete(f.gflight, key) // 航班已完成
	f.mu.Unlock()

	return p.val,p.ex,p.err
}

func (f *Flight) SetFly(key string, fn func() (error)) (error) {
	f.mu.Lock()
	if f.sflight == nil {
		f.sflight = make(map[string]*packet)
	}

	if p, ok := f.sflight[key]; ok {
		f.mu.Unlock()
		p.wg.Wait() //后入的会进入这个if，然后阻塞在wait
		return p.err
	}

	//先入的会解决这个请求
	p := new(packet)
	p.wg.Add(1)
	f.sflight[key] = p
	f.mu.Unlock()

	p.err = fn()
	p.wg.Done()

	f.mu.Lock()
	delete(f.sflight, key) // 航班已完成
	f.mu.Unlock()

	return p.err
}