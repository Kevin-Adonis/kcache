package main

import (
	"fmt"
	"kcache/kcache"
	"log"
	"os"
	//clientv3 "go.etcd.io/etcd/client/v3"
	//"go.etcd.io/etcd/client/v3/naming/endpoints"
	//"go.etcd.io/etcd/client/v3/naming/resolver"
	//"google.golang.org/grpc"
	//"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var addr string = "localhost:" + os.Args[1]

	// 模拟MySQL数据库 用于peanutcache从数据源获取值
	var mysql = map[string]string{
		"Tom":  "630",
		"Jack": "589",
		"Sam":  "567",
	}

	// 新建cache实例
	group := kcache.NewGroup(addr, "scores", 2<<10, kcache.RetrieverFunc(
		//如果所有主机缓存中没有，则从数据库中取
		func(key string) ([]byte, error) {
			log.Println("[Mysql] search key", key)
			if v, ok := mysql[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	var key string
	for key != "0" {
		fmt.Scan(&key)
		GetTomScore(key, group)
		fmt.Println("==============================")
	}
}

func GetTomScore(key string, group *kcache.Group) {

	log.Printf("Get " + key)
	view, err := group.Get(key)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(view.String())
}
