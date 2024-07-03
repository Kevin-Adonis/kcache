package main

import (
	"context"
	"fmt"
	"kcache/kcache"
	"log"
	"os"

	//"time"
	//"unsafe"

	"github.com/redis/go-redis/v9"
	//clientv3 "go.etcd.io/etcd/client/v3"
	//"go.etcd.io/etcd/client/v3/naming/endpoints"
	//"go.etcd.io/etcd/client/v3/naming/resolver"
	//"google.golang.org/grpc"
	//"google.golang.org/grpc/credentials/insecure"
)

func DataBaseInit() {
	cli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // redis地址
		Password: "",               // 密码
		DB:       0,                // 使用默认数据库
	})

	ctx := context.Background()
	cli.Set(ctx, "Tom", "630", 0)
	cli.Set(ctx, "Sam", "567", 0)
	cli.Set(ctx, "YKH", "999", 0)
	cli.Set(ctx, "Jack", "589", 0)
}

func main() {

	DataBaseInit()

	var addr string = "localhost:" + os.Args[1]

	// 新建cache实例
	group := kcache.NewGroup(addr, "scores", 2<<10, kcache.RetrieverFunc(
		//如果所有主机缓存中没有，则从数Redis中取
		func(key string) ([]byte, error) {
			log.Println("[Redis] search key", key)

			client := redis.NewClient(&redis.Options{
				Addr:     "localhost:6379", // redis地址
				Password: "",               // 密码
				DB:       0,                // 使用默认数据库
			})

			ctx := context.Background()
			if val, err := client.Get(ctx, key).Result(); err == nil {
				return []byte(val), nil
			}

			return nil, fmt.Errorf("%s not exist", key)
		}))

	var key string
	for {
		fmt.Println("==========================================================================================")
		fmt.Print("输入Key: ")
		fmt.Scan(&key)
		if key == "q" {
			break
		} else {
			GetTomScore(key, group)
		}
	}
}

func GetTomScore(key string, group *kcache.Group) {

	view, err := group.Get(key)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Print("Value: ")
	fmt.Println(view.String())
}
