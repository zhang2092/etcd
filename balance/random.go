package balance

import (
	"errors"
	"math/rand"
	"sync"
)

// Random 随机负载均衡
type Random struct {
	curIndex int
	rss      []Addr
	mutex    sync.Mutex
}

func NewRandom() Balancer {
	return &Random{}
}

func (r *Random) Add(params ...Addr) error {
	if len(params) == 0 {
		return errors.New("params len 1 at least")
	}

	r.mutex.Lock()
	r.rss = append(r.rss, params...)
	r.mutex.Unlock()

	return nil
}

func (r *Random) Delete(params string) {
	r.mutex.Lock()
	lens := len(r.rss) - 1
	for i := lens; i >= 0; i-- {
		if params == r.rss[i].Key {
			r.rss = append(r.rss[:i], r.rss[i+1:]...)
		}
	}
	r.mutex.Unlock()
}

func (r *Random) Next() string {
	if len(r.rss) == 0 {
		return ""
	}
	if len(r.rss) == 1 {
		return r.rss[0].Value
	}

	r.mutex.Lock()
	r.curIndex = rand.Intn(len(r.rss))
	r.mutex.Unlock()

	return r.rss[r.curIndex].Value
}
