// Copyright 2021 Peanutzhen. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kcache

import (
	"context"
	"fmt"
	"kcache/kcache/consistenthash"
	pb "kcache/kcache/kcachepb"
	"kcache/kcache/registry"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/peer"
)

// server 模块为peanutcache之间提供通信能力
// 这样部署在其他机器上的cache可以通过访问server获取缓存
// 至于找哪台主机 那是一致性哈希的工作了

const (
	defaultAddr     = "127.0.0.1:6324"
	defaultReplicas = 50
)

var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
)

// Server 和 Group 是解耦合的 所以Server要自己实现并发控制
type Server struct {
	pb.UnimplementedKCacheServer

	addr       string     // format: ip:port
	status     bool       // true: running false: stop
	stopSignal chan error // 通知registry revoke服务
	mu         sync.Mutex
	consHash   *consistenthash.Consistency
	clients    map[string]*client
}

// NewServer 创建cache的svr 若addr为空 则使用defaultAddr
func NewServer(addr string) (*Server, error) {
	if addr == "" {
		addr = defaultAddr
	}
	//判断地址是否合法
	if !validPeerAddr(addr) {
		return nil, fmt.Errorf("invalid addr %s, it should be x.x.x.x:port", addr)
	}
	return &Server{addr: addr}, nil
}

// Get 实现PeanutCache service的Get接口
func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {

	//这一段是获取请求服务的客户度的信息
	//pr, _ := peer.FromContext(ctx)
	//fmt.Print("coming addr: ")
	//fmt.Println(pr.Addr)

	group, key := in.GetGroup(), in.GetKey()
	resp := &pb.GetResponse{}

	log.Printf("[kcache_svr %s] Recv RPC Request - (%s)/(%s)", s.addr, group, key)
	if key == "" {
		return resp, fmt.Errorf("key required")
	}

	g := GetGroup(group)
	if g == nil {
		return resp, fmt.Errorf("group not found")
	}

	view, err := g.Get(key)
	if err != nil {
		return resp, err
	}

	resp.Value = view.ByteSlice()
	return resp, nil
}

// Start 启动cache服务
func (s *Server) Start() error {
	s.mu.Lock()
	if s.status == true {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	// -----------------启动服务----------------------
	// 1. 设置status为true 表示服务器已在运行
	// 2. 初始化stop channal,这用于通知registry stop keep alive
	// 3. 初始化tcp socket并开始监听
	// 4. 注册rpc服务至grpc 这样grpc收到request可以分发给server处理
	// 5. 将自己的服务名/Host地址注册至etcd 这样client可以通过etcd
	//    获取服务Host地址 从而进行通信。这样的好处是client只需知道服务名
	//    以及etcd的Host即可获取对应服务IP 无需写死至client代码中
	// ----------------------------------------------
	s.status = true
	s.stopSignal = make(chan error)

	log.Println("Kcache is running at", s.addr)

	port := strings.Split(s.addr, ":")[1]
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKCacheServer(grpcServer, s)

	s.mu.Unlock()
	//****************
	// 注册服务至etcd
	//****************
	go func() {
		err := registry.Register("kcache", s.addr, s.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}

		close(s.stopSignal)

		err = lis.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		log.Printf("[%s] Revoke service and close tcp socket ok.", s.addr)
	}()

	go s.UpdatePeers()

	//log.Printf("[%s] register service ok\n", s.addr)
	if err := grpcServer.Serve(lis); s.status && err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

// SetPeers 将各个远端主机IP配置到Server里
// 这样Server就可以Pick他们了
// 注意: 此操作是*覆写*操作！
// 注意: peersIP必须满足 x.x.x.x:port的格式
func (s *Server) SetPeers(peersAddr ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.consHash = consistenthash.New(defaultReplicas, nil)
	s.consHash.Register(peersAddr...)
	s.clients = make(map[string]*client)
	for idx, peerAddr := range peersAddr {
		if !validPeerAddr(peerAddr) {
			panic(fmt.Sprintf("[peer %s] invalid address format, it should be x.x.x.x:port", peerAddr))
		}
		service := fmt.Sprintf("kcache/%s", peerAddr)
		fmt.Print(idx)
		fmt.Println(" " + peerAddr)
		s.clients[peerAddr] = NewClient(service)
	}
}

func (s *Server) UpdatePeers() {
	c, _ := clientv3.New(defaultEtcdConfig)
	defer c.Close()

	for {
		watchChan := c.Watch(context.Background(), "kcache", clientv3.WithPrefix())
		for watchResp := range watchChan {
			for _, ev := range watchResp.Events {
				if ev.Type == clientv3.EventTypePut || ev.Type == clientv3.EventTypeDelete {
					//log.Printf("%s %q %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)

					em, _ := endpoints.NewManager(c, "kcache")
					mp, _ := em.List(c.Ctx())

					keys := make([]string, 0, len(mp))
					for k := range mp {
						keys = append(keys, strings.Split(k, "/")[1])
					}

					//表示拆开切片
					s.SetPeers(keys...)
				}
			}
		}

		//s.SetPeers()
	}
}

// Pick 根据一致性哈希选举出key应存放在的cache
// return false 代表从本地获取cache
func (s *Server) Pick(key string) (Fetcher, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	peerAddr := s.consHash.GetPeer(key)
	// Pick itself
	if peerAddr == s.addr {
		log.Printf("ooh! pick myself, I am %s\n", s.addr)
		return nil, false
	}
	log.Printf("[cache %s] pick remote peer: %s\n", s.addr, peerAddr)
	return s.clients[peerAddr], true
}

// Stop 停止server运行 如果server没有运行 这将是一个no-op
func (s *Server) Stop() {
	s.mu.Lock()
	if s.status == false {
		s.mu.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止keepalive信号
	s.status = false    // 设置server运行状态为stop
	s.clients = nil     // 清空一致性哈希信息 有助于垃圾回收
	s.consHash = nil
	s.mu.Unlock()
}

// 测试Server是否实现了Picker接口
var _ Picker = (*Server)(nil)
