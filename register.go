package etcd

import (
	"context"
	"net"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Client struct {
	v3          *clientv3.Client
	lease       *clientv3.LeaseGrantResponse
	interval    int32
	serviceName string
}

func NewClient(endpoint []string) (*Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Client{v3: client, interval: 5}, nil
}

// Close 关闭client
func (e *Client) Close() error {
	return e.v3.Close()
}

// Register 注册服务
func (e *Client) Register(serviceName string, serviceAddress string) error {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	e.lease, err = e.v3.Grant(ctx, int64(e.interval+1))
	if err != nil {
		return err
	}

	e.serviceName = serviceName + "/" + strconv.Itoa(int(e.lease.ID))
	_, err = e.v3.Put(ctx, e.serviceName, getValue(serviceAddress), clientv3.WithLease(e.lease.ID))
	if err != nil {
		return err
	}

	e.keepAlive(serviceName, serviceAddress)
	return nil
}

// Deregister 注销服务
func (e *Client) Deregister() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, err := e.v3.Delete(ctx, e.serviceName)
	if err != nil {
		return err
	}

	return nil
}

// keepAlive 异步续约
func (e *Client) keepAlive(name string, addr string) {
	// 永久续约，续约成功后，etcd客户端和服务器会保持通讯，通讯成功会写数据到返回的通道中
	// 停止进程后，服务器链接不上客户端，相应key租约到期会被服务器自动删除
	c, err := e.v3.KeepAlive(e.v3.Ctx(), e.lease.ID)
	go func() {
		if err == nil {
			for {
				select {
				case _, ok := <-c:
					if !ok { // 续约失败
						_, _ = e.v3.Revoke(e.v3.Ctx(), e.lease.ID)
						_ = e.Register(name, addr)
						return
					}
				}
			}
			// defer e.v3.Revoke(e.v3.Ctx(), e.lease.ID)
		}
	}()
}

// getValue etcd服务发现时，底层解析的是一个json串，且包含Addr字段
func getValue(addr string) string {
	// return "{\"Addr\":\"" + localIP() + addr + "\"}"

	return localIP() + addr
}

// localIP 获取当前ip
func localIP() string {
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrList {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}
	return ""
}
