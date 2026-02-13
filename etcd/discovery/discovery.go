package discovery

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nellac64/common-sdk/windowsmock/config"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Service struct {
	Name     string
	IP       string
	Port     string
	Protocol string
}

type ServiceCluster struct {
	services map[string]*Service
	sync.RWMutex
}

var serviceCluster = &ServiceCluster{
	services: map[string]*Service{},
}

const (
	SuffixName     = ".Name"
	SuffixIP       = ".IP"
	SuffixPort     = ".Port"
	SuffixProtocol = ".Protocol"
)

// ServiceRegister
/**
首次注册：
	创建租约 -> 更新信息 -> 持续续约
多次注册：
	后续服务重启，仍然重新注册
	创建新租约 -> 更新信息 -> 持续续约

*/

func ServiceRegister(s *Service) {
	// 此处创建的 cli 不能关闭 持续作用于后续的保活
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetETCDEndpoint(),
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 创建租约 ttl = 10 10s 需要续约 客户端 10/3 发送保活
	ctx := context.Background()
	var leaseId clientv3.LeaseID
	leaseRes, err := cli.Grant(ctx, 10)
	if err != nil {
		log.Fatal(err)
	}
	leaseId = leaseRes.ID

	// 开启 etcd 事务 更新本服务相关的 kv
	kv := clientv3.NewKV(cli)
	txn := kv.Txn(ctx)

	_, err = txn.
		Then(
			// if 条件成立 创建次数为 0
			// 没有租约 创建租约
			clientv3.OpPut(s.Name+SuffixName, s.Name, clientv3.WithLease(leaseId)),
			clientv3.OpPut(s.Name+SuffixIP, s.IP, clientv3.WithLease(leaseId)),
			clientv3.OpPut(s.Name+SuffixPort, s.Port, clientv3.WithLease(leaseId)),
			clientv3.OpPut(s.Name+SuffixProtocol, s.Protocol, clientv3.WithLease(leaseId)),
		).
		Commit()

	if err != nil {
		log.Fatal(err)
	}

	// 定期续租
	leaseKeepalive, err := cli.KeepAlive(ctx, leaseId)
	if err != nil {
		log.Fatal(err)
	}
	// 消费掉续约成功后，ETCD Server 返回给本端的信息
	// 阻塞接收 方法内部起协程接收 或协程调用方法 此处选择方法内部起协程接收
	go func() {
		for lease := range leaseKeepalive {
			fmt.Printf("[%v] : lease keep alive %v\n", time.Now(), lease)
		}
	}()

}

// ServiceDiscoveryStarter 服务发现启动入口 需要起协程调用
func ServiceDiscoveryStarter(srvName string) {
	s := ServiceDiscoveryFromETCD(srvName)
	for s == nil {
		time.Sleep(5 * time.Second)
		s = ServiceDiscoveryFromETCD(srvName)
	}

	// s != nil
	WatchService(srvName)
}

// ServiceDiscovery 根据 svcName 实现服务发现
func ServiceDiscovery(svcName string) *Service {
	var s *Service
	serviceCluster.RLock()
	defer serviceCluster.RUnlock()
	s, _ = serviceCluster.services[svcName]
	return s
}

// ServiceDiscoveryFromETCD 同步调用 etcd 获取 svcName 的服务信息
func ServiceDiscoveryFromETCD(svcName string) *Service {
	var s *Service = &Service{}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetETCDEndpoint(),
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		fmt.Println("err ", err)
		return nil
	}
	defer cli.Close()

	ctx := context.Background()
	srvRes, err := cli.Get(ctx, svcName, clientv3.WithPrefix())
	if err != nil {
		fmt.Println("err ", err)
		return nil
	}
	for _, kv := range srvRes.Kvs {
		etcdKey := string(kv.Key)
		switch etcdKey {
		case svcName + SuffixName:
			s.Name = string(kv.Value)
		case svcName + SuffixIP:
			s.IP = string(kv.Value)
		case svcName + SuffixPort:
			s.Port = string(kv.Value)
		case svcName + SuffixProtocol:
			s.Protocol = string(kv.Value)
		default:
			fmt.Println("do not have this param: ", etcdKey, string(kv.Value))
		}
	}
	RefreshLocalSrvCache(s)
	return s
}

// RefreshLocalSrvCache 刷新本地缓存
func RefreshLocalSrvCache(srv *Service) {
	copySrv := *srv

	serviceCluster.Lock()
	defer serviceCluster.Unlock()

	serviceCluster.services[srv.Name] = &copySrv
}

// WatchService 监听服务名称
func WatchService(svcName string) {
	// 此处创建的 cli 不能关闭 持续作用于后续的监听实践
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetETCDEndpoint(),
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	watchChan := cli.Watch(ctx, svcName, clientv3.WithPrefix())

	for watchResp := range watchChan {
		if watchResp.Canceled {
			fmt.Println("watch cancel")
			return
		}
		if watchResp.Err() != nil {
			fmt.Println("watch err ", watchResp.Err())
			continue
		}

		// 检查是否有事件
		for _, event := range watchResp.Events {
			fmt.Println(fmt.Sprintf("%v: %v : %v",
				event.Type, string(event.Kv.Key), string(event.Kv.Value)))
			switch event.Type {
			case clientv3.EventTypePut:
				// 更新事件 需要刷新缓存
				kv := event.Kv
				UpdateLocalSrv(svcName, kv)
			case clientv3.EventTypeDelete:
				// 删除事件 需要删除缓存
				DeleteLocalSrv(svcName)
			}
		}
	}
}

// UpdateLocalSrv 更新本地缓存
func UpdateLocalSrv(svcName string, kv *mvccpb.KeyValue) {
	key := string(kv.Key)

	serviceCluster.Lock()
	switch {
	case strings.HasSuffix(key, SuffixName):
		serviceCluster.services[svcName].Name = string(kv.Value)
	case strings.HasSuffix(key, SuffixIP):
		serviceCluster.services[svcName].IP = string(kv.Value)
	case strings.HasSuffix(key, SuffixPort):
		serviceCluster.services[svcName].Port = string(kv.Value)
	case strings.HasSuffix(key, SuffixProtocol):
		serviceCluster.services[svcName].Protocol = string(kv.Value)
	default:
		fmt.Println("do not have this param: ", key)
	}
	serviceCluster.Unlock()
}

// DeleteLocalSrv 删除本地缓存
func DeleteLocalSrv(srvName string) {
	serviceCluster.Lock()
	defer serviceCluster.Unlock()

	delete(serviceCluster.services, srvName)
}
