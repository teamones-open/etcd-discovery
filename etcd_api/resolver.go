package etcd_api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/teamones-open/etcd-discovery/common"
	"go.etcd.io/etcd/clientv3"
	"log"
)

type Discovery struct {
	cli   *clientv3.Client
	info  *NodeInfo
	Nodes *NodesManager
}

func NewDiscovery(info *NodeInfo, conf clientv3.Config, mgr *NodesManager) (dis *Discovery, err error) {
	d := &Discovery{}
	d.info = info
	if mgr == nil {
		return nil, fmt.Errorf("[Discovery] mgr == nil")
	}
	d.Nodes = mgr
	d.cli, err = clientv3.New(conf)
	return d, err
}

func (d *Discovery) Pull() {
	kv := clientv3.NewKV(d.cli)
	resp, err := kv.Get(context.TODO(), "discovery/", clientv3.WithPrefix())
	if err != nil {
		if common.Mode() == common.DebugMode {
			log.Printf("[Discovery] kv.Get err:%+v", err)
		}
		return
	}
	for _, v := range resp.Kvs {
		node := &NodeInfo{}
		err = json.Unmarshal(v.Value, node)
		if err != nil {
			if common.Mode() == common.DebugMode {
				log.Printf("[Discovery] json.Unmarshal err:%+v", err)
			}
			continue
		}
		d.Nodes.AddNode(node)
		if common.Mode() == common.DebugMode {
			log.Printf("[Discovery] pull node:%+v", node)
		}
	}
}

func (d *Discovery) Watch() {
	watcher := clientv3.NewWatcher(d.cli)
	watchChan := watcher.Watch(context.TODO(), "discovery/", clientv3.WithPrefix())
	for {
		select {
		case resp := <-watchChan:
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
				log.Printf("[Discovery] json.Unmarshal err:%+v", err)
				continue
			}
			d.Nodes.AddNode(node)
			if common.Mode() == common.DebugMode {
				log.Printf(fmt.Sprintf("[Discovery] new node:%s", string(ev.Kv.Value)))
			}
		case clientv3.EventTypeDelete:
			d.Nodes.DelNode(string(ev.Kv.Key))
			if common.Mode() == common.DebugMode {
				log.Printf(fmt.Sprintf("[Discovery] del node:%s data:%s", string(ev.Kv.Key), string(ev.Kv.Value)))
			}
		}
	}
}
