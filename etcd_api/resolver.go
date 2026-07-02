package etcd_api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/teamones-open/etcd-discovery/common"
	"go.etcd.io/etcd/clientv3"
)

type Discovery struct {
	cli   *clientv3.Client
	info  *NodeInfo
	Nodes *NodesManager

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func NewDiscovery(info *NodeInfo, conf clientv3.Config, mgr *NodesManager) (dis *Discovery, err error) {
	if mgr == nil {
		return nil, fmt.Errorf("[Discovery] mgr == nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	d := &Discovery{
		info:   info,
		Nodes:  mgr,
		ctx:    ctx,
		cancel: cancel,
	}

	d.cli, err = clientv3.New(conf)
	if err != nil {
		cancel()
		return nil, err
	}

	return d, nil
}

func (d *Discovery) RunFromRevision(rev int64) {
	for {
		if rev <= 0 {
			log.Printf("[Discovery] invalid revision:%d, pull again", rev)
			rev = d.Pull()

			if rev <= 0 {
				select {
				case <-d.ctx.Done():
					log.Printf("[Discovery] RunFromRevision exit")
					return
				case <-time.After(3 * time.Second):
					continue
				}
			}
		}

		d.Watch(rev)

		select {
		case <-d.ctx.Done():
			log.Printf("[Discovery] RunFromRevision exit")
			return

		case <-time.After(3 * time.Second):
			log.Printf("[Discovery] watch stopped, pull and restart watch")
			rev = d.Pull()
		}
	}
}

func (d *Discovery) Run() {
	for {
		rev := d.Pull()
		d.Watch(rev)

		select {
		case <-d.ctx.Done():
			log.Printf("[Discovery] Run exit")
			return

		case <-time.After(3 * time.Second):
			log.Printf("[Discovery] restart watch")
		}
	}
}

func (d *Discovery) Pull() int64 {
	kv := clientv3.NewKV(d.cli)

	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	resp, err := kv.Get(ctx, "discovery/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("[Discovery] kv.Get err:%+v", err)
		return 0
	}

	all := map[string]map[string]*NodeInfo{}

	for _, v := range resp.Kvs {
		node := &NodeInfo{}
		err = json.Unmarshal(v.Value, node)
		if err != nil {
			log.Printf(
				"[Discovery] json.Unmarshal err:%+v key:%s value:%s",
				err,
				string(v.Key),
				string(v.Value),
			)
			continue
		}

		if node.Name == "" || node.UniqueId == "" {
			log.Printf(
				"[Discovery] invalid node key:%s value:%s",
				string(v.Key),
				string(v.Value),
			)
			continue
		}

		if _, ok := all[node.Name]; !ok {
			all[node.Name] = map[string]*NodeInfo{}
		}

		all[node.Name][node.UniqueId] = node

		if common.Mode() == common.DebugMode {
			log.Printf("[Discovery] pull node key:%s value:%s", string(v.Key), string(v.Value))
		}
	}

	d.Nodes.ReplaceAll(all)

	log.Printf("[Discovery] pull completed, nodes:%d", d.Nodes.Count())

	if resp.Header == nil {
		return 0
	}

	return resp.Header.Revision
}

func (d *Discovery) Watch(startRevision int64) {
	watcher := clientv3.NewWatcher(d.cli)
	defer watcher.Close()

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
	}

	if startRevision > 0 {
		opts = append(opts, clientv3.WithRev(startRevision+1))
	}

	watchChan := watcher.Watch(d.ctx, "discovery/", opts...)

	log.Printf("[Discovery] Watch start revision:%d", startRevision)

	for {
		select {
		case <-d.ctx.Done():
			log.Printf("[Discovery] Watch exit")
			return

		case resp, ok := <-watchChan:
			if !ok {
				log.Printf("[Discovery] watch channel closed")
				return
			}

			if err := resp.Err(); err != nil {
				log.Printf("[Discovery] watch response err:%+v", err)
				return
			}

			if resp.Canceled {
				log.Printf("[Discovery] watch canceled compactRevision:%d", resp.CompactRevision)
				return
			}

			d.watchEvent(resp.Events)
		}
	}
}

func (d *Discovery) watchEvent(evs []*clientv3.Event) {
	for _, ev := range evs {
		switch ev.Type {
		case clientv3.EventTypePut:
			node := &NodeInfo{}
			err := json.Unmarshal(ev.Kv.Value, node)
			if err != nil {
				log.Printf(
					"[Discovery] json.Unmarshal err:%+v key:%s value:%s",
					err,
					string(ev.Kv.Key),
					string(ev.Kv.Value),
				)
				continue
			}

			d.Nodes.AddNode(node)

			if common.Mode() == common.DebugMode {
				log.Printf("[Discovery] new node key:%s value:%s", string(ev.Kv.Key), string(ev.Kv.Value))
			}

		case clientv3.EventTypeDelete:
			key := string(ev.Kv.Key)
			d.Nodes.DelNode(key)

			if common.Mode() == common.DebugMode {
				prevValue := ""
				if ev.PrevKv != nil {
					prevValue = string(ev.PrevKv.Value)
				}

				log.Printf("[Discovery] del node key:%s prevValue:%s", key, prevValue)
			}
		}
	}
}

func (d *Discovery) Stop() {
	d.stopOnce.Do(func() {
		if d.cancel != nil {
			d.cancel()
		}

		if d.cli != nil {
			_ = d.cli.Close()
		}

		log.Printf("[Discovery] stopped")
	})
}
