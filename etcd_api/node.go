package etcd_api

import (
	"log"
	"math/rand"
	"strings"
	"sync"

	"github.com/teamones-open/etcd-discovery/common"
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

func NewNodeManager() *NodesManager {
	return &NodesManager{
		nodes: map[string]map[string]*NodeInfo{},
	}
}

func (n *NodesManager) AddNode(node *NodeInfo) {
	if node == nil {
		return
	}

	if node.Name == "" || node.UniqueId == "" {
		log.Printf("[NodesManager] invalid node:%+v", node)
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
	parts := strings.Split(id, "/")
	if len(parts) < 3 {
		log.Printf("[NodesManager] invalid node id:%s", id)
		return
	}

	name := parts[len(parts)-2]

	n.Lock()
	defer n.Unlock()

	if group, exist := n.nodes[name]; exist {
		delete(group, id)

		if len(group) == 0 {
			delete(n.nodes, name)
		}
	}
}

func (n *NodesManager) Pick(name string) *NodeInfo {
	n.RLock()
	defer n.RUnlock()

	nodes, exist := n.nodes[name]
	if !exist || len(nodes) == 0 {
		return nil
	}

	idx := rand.Intn(len(nodes))

	for _, v := range nodes {
		if idx == 0 {
			return v
		}
		idx--
	}

	return nil
}

func (n *NodesManager) ReplaceAll(all map[string]map[string]*NodeInfo) {
	n.Lock()
	defer n.Unlock()

	if all == nil {
		n.nodes = map[string]map[string]*NodeInfo{}
		return
	}

	n.nodes = all
}

func (n *NodesManager) Count() int {
	n.RLock()
	defer n.RUnlock()

	total := 0
	for _, group := range n.nodes {
		total += len(group)
	}

	return total
}

func (n *NodesManager) Dump() {
	n.RLock()
	defer n.RUnlock()

	for k, v := range n.nodes {
		for kk, vv := range v {
			if common.Mode() == common.DebugMode {
				log.Printf("[NodesManager] Name:%s Id:%s Node:%+v", k, kk, vv)
			}
		}
	}
}
