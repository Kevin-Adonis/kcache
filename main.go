package main

import (
	"bufio"
	"context"
	"fmt"
	"kcache/kcache"
	"log"
	"os"
	"strconv"
	"strings"

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

	for {
		fmt.Println("==========================================================================================")
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		bytes, _, _ := reader.ReadLine()
		str := string(bytes)

		if str == "q" {
			break
		} else {

			slice := strings.Split(str, " ")
			if slice[0] == "get" {
				GetTomScore(slice[1], group)
			}
			if slice[0] == "set" {
				if len(slice) == 3 {
					group.Set(slice[1], slice[2], 0)
				} else if len(slice) == 4 {
					i, _ := strconv.Atoi(slice[3])
					group.Set(slice[1], slice[2], i)
				} else {

				}
			}
			if slice[0] == "del" {

			}
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
