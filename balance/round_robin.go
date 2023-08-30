package balance

import (
	"errors"
	"sync"
)

// RoundRobin 轮询负载均衡
type RoundRobin struct {
	curIndex int
	rss      []Addr
	mutex    sync.Mutex
}

func NewRoundRobin() Balancer {
	return &RoundRobin{}
}

func (r *RoundRobin) Add(params ...Addr) error {
	if len(params) == 0 {
		return errors.New("params len 1 at least")
	}

	r.mutex.Lock()
	r.rss = append(r.rss, params...)
	r.mutex.Unlock()
	return nil
}

func (r *RoundRobin) Delete(params string) {
	r.mutex.Lock()
	lens := len(r.rss) - 1
	for i := lens; i >= 0; i-- {
		if params == r.rss[i].Key {
			r.rss = append(r.rss[:i], r.rss[i+1:]...)
		}
	}
	r.mutex.Unlock()
}

func (r *RoundRobin) Next() string {
	lens := len(r.rss)
	if lens == 0 {
		return ""
	}

	if r.curIndex >= lens {
		r.curIndex = 0
	}

	r.mutex.Lock()
	curAddr := r.rss[r.curIndex]
	r.curIndex = (r.curIndex + 1) % lens
	r.mutex.Unlock()
	return curAddr.Value
}
