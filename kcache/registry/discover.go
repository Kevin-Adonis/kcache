package registry

import (
	"errors"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"

	//"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EtcdDial 向grpc请求一个服务
// 通过提供一个etcd client和service name即可获得Connection
func EtcdDial(c *clientv3.Client, service string) (*grpc.ClientConn, error) {
	/*
		etcdResolver, err := resolver.NewBuilder(c)
		if err != nil {
			return nil, err
		}*/
	/*
		return grpc.Dial(
			"etcd:///"+service,
			grpc.WithResolvers(etcdResolver),
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)*/

	em, _ := endpoints.NewManager(c, "kcache")

	mp, _ := em.List(c.Ctx())

	if ep, ok := mp[service]; ok {
		log.Println("grpc.NewClient")
		log.Println(mp[service].Addr)
		return grpc.NewClient(
			ep.Addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
	}

	return nil, errors.New("Not in")
	/*
		return grpc.NewClient(
			"etcd:///kcache",
			//"etcd:///"+service,
			grpc.WithResolvers(etcdResolver),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)*/
}

