package etcd_api

import (
	"github.com/teamones-open/etcd-discovery/common"
	"log"
	"math/rand"
	"strings"
	"sync"
)

type NodeInfo struct {
	Addr     string `json:"server_host"`
	Name     string `json:"server_name"`
	UniqueId string `json:"server_id"`
}

type NodesManager struct {
	sync.RWMutex
	// <name,<id,node>>
	nodes map[string]map[string]*NodeInfo
}

func NewNodeManager() (m *NodesManager) {
	return &NodesManager{
		nodes: map[string]map[string]*NodeInfo{},
	}
}

func (n *NodesManager) AddNode(node *NodeInfo) {
	if node == nil {
		return
	}
	n.Lock()
	defer n.Unlock()
	if _, exist := n.nodes[node.Name]; !exist {
		n.nodes[node.Name] = map[string]*NodeInfo{}
	}
	n.nodes[node.Name][node.UniqueId] = node
}

func (n *NodesManager) DelNode(id string) {
	sli := strings.Split(id, "/")
	name := sli[len(sli)-2]
	n.Lock()
	defer n.Unlock()
	if _, exist := n.nodes[name]; exist {
		delete(n.nodes[name], id)
	}
}

func (n *NodesManager) Pick(name string) *NodeInfo {
	n.RLock()
	defer n.RUnlock()
	if nodes, exist := n.nodes[name]; !exist {
		return nil
	} else {
		// 纯随机取节点
		var idx int
		nodeLen := len(nodes)
		if nodeLen == 0 {
			idx = 0
		} else {
			idx = rand.Intn(nodeLen)
		}
		for _, v := range nodes {
			if idx == 0 {
				return v
			}
			idx--
		}
	}
	return nil
}

func (n *NodesManager) Dump() {
	for k, v := range n.nodes {
		for kk, vv := range v {
			if common.Mode() == common.DebugMode {
				log.Printf("[NodesManager] Name:%s Id:%s Node:%+v", k, kk, vv)
			}
		}
	}
}
