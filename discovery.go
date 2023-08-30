package etcd

import (
	"context"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zhang2092/etcd/balance"
)

const (
	RoundRobin int = iota
	Random
	Weight
	Hash
)

type RemoteService struct {
	name    string
	balance balance.Balancer
	mutex   sync.Mutex
}

type Resolver struct {
	v3       *clientv3.Client
	endpoint []string
}

// NewResolver 构造 resolver 对象
func NewResolver(endpoint []string) (*Resolver, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Resolver{v3: client, endpoint: endpoint}, nil
}

// Close 关闭client
func (r *Resolver) Close() error {
	return r.v3.Close()
}

// Discovery 发现服务
func (r *Resolver) Discovery(serviceName string, balanceType int) (*RemoteService, error) {
	service := &RemoteService{
		name: serviceName,
	}

	kv := clientv3.NewKV(r.v3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := kv.Get(ctx, serviceName, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	processorBalance(service, resp.Kvs, balanceType)

	go r.watchServiceUpdate(service)

	return service, nil
}

// watchServiceUpdate 监控服务目录下的事件
func (r *Resolver) watchServiceUpdate(service *RemoteService) {
	watcher := clientv3.NewWatcher(r.v3)
	// Watch 服务目录下的更新
	watchChan := watcher.Watch(context.TODO(), service.name, clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			service.mutex.Lock()
			switch event.Type {
			case mvccpb.PUT: // PUT事件，目录下有了新key
				_ = service.balance.Add(balance.Addr{
					Key:   string(event.Kv.Key),
					Value: string(event.Kv.Value),
				})
			case mvccpb.DELETE: // DELETE事件，目录中有key被删掉(Lease过期，key 也会被删掉)
				service.balance.Delete(string(event.Kv.Key))
			}
			service.mutex.Unlock()
		}
	}
}

func processorBalance(service *RemoteService, kvs []*mvccpb.KeyValue, balanceType int) {
	switch balanceType {
	case RoundRobin:
		_ = genRoundRobin(service, kvs)
	case Random:
		_ = genRandom(service, kvs)
	case Weight:
		log.Println("weight develop ...")
	case Hash:
		log.Println("hash develop ...")
	}
}

func genAddr(kvs []*mvccpb.KeyValue) []balance.Addr {
	var address []balance.Addr
	for _, kv := range kvs {
		address = append(address, balance.Addr{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}
	return address
}

func genRoundRobin(service *RemoteService, kvs []*mvccpb.KeyValue) error {
	service.balance = balance.NewRoundRobin()
	return service.balance.Add(genAddr(kvs)...)
}

func genRandom(service *RemoteService, kvs []*mvccpb.KeyValue) error {
	service.balance = balance.NewRandom()
	return service.balance.Add(genAddr(kvs)...)
}

// GetName 获取服务名称
func (r *RemoteService) GetName() string {
	return r.name
}

// Next 根据算法获取下个值
func (r *RemoteService) Next() string {
	return r.balance.Next()
}
