package etcd_discovery

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/teamones-open/etcd-discovery/common"
	"github.com/teamones-open/etcd-discovery/etcd_api"
	"go.etcd.io/etcd/clientv3"
)

type RegisterParam struct {
	UUID string `json:"uuid"` // 服务唯一id，当前版本暂不使用，保留兼容
	Name string `json:"name"` // 服务名
	Port string `json:"port"` // 服务端口
}

var (
	// 防止同一个服务实例重复启动注册协程
	registerMu sync.Mutex
	registers  = make(map[string]*etcd_api.Register)

	// 全局服务发现缓存
	discoveryCacheMu sync.Mutex
	globalNodes      = etcd_api.NewNodeManager()
	globalDiscovery  *etcd_api.Discovery
	globalEtcdHost   string
)

// RegisterServer 注册服务。
// 注意：
// 1. 不再使用 uuid.NewV4() 作为 server_id。
// 2. 改成 discovery/{服务名}/{ip:port}，保证同一个服务实例 key 稳定。
// 3. 同一个 key 重复注册时，不再重复 go reg.Run()。
func RegisterServer(host string, port string, serverName string) error {
	if host == "" {
		return errors.New("Etcd host does not exist.")
	}

	if port == "" {
		return errors.New("server port does not exist.")
	}

	if serverName == "" {
		return errors.New("server name does not exist.")
	}

	var nodeInfo etcd_api.NodeInfo

	nodeInfo.Name = serverName

	ip, ipErr := common.GetLocalIPv4(nodeInfo.Name)
	if ipErr != nil {
		return ipErr
	}

	nodeInfo.Addr = ip + ":" + port

	// 关键优化：
	// 原来是 uuid.NewV4()，每次注册都会生成新 key。
	// 现在改成稳定实例 key。
	nodeInfo.UniqueId = "discovery/" + nodeInfo.Name + "/" + nodeInfo.Addr

	registerMu.Lock()
	defer registerMu.Unlock()

	if _, ok := registers[nodeInfo.UniqueId]; ok {
		log.Printf("[RegisterServer] already registered key:%s", nodeInfo.UniqueId)
		return nil
	}

	reg, err := etcd_api.NewRegister(&nodeInfo, clientv3.Config{
		Endpoints:   []string{host},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	registers[nodeInfo.UniqueId] = reg

	go reg.Run()

	log.Printf(
		"[RegisterServer] register started key:%s addr:%s",
		nodeInfo.UniqueId,
		nodeInfo.Addr,
	)

	return nil
}

// InitDiscoveryCache 初始化全局服务发现缓存。
// PHP 端不需要改，第一次 GetServerNodeByName 时会自动初始化。
// 初始化后：
// 1. Go 端 Pull 一次 etcd 全量节点。
// 2. 后台 Watch discovery/ 前缀变化。
// 3. 后续 PHP 每秒请求 discovery 时，只查 globalNodes 内存缓存，不再每次访问 etcd。
func InitDiscoveryCache(host string) error {
	if host == "" {
		return errors.New("Etcd host does not exist.")
	}

	discoveryCacheMu.Lock()
	defer discoveryCacheMu.Unlock()

	if globalDiscovery != nil && globalEtcdHost == host {
		return nil
	}

	// 如果 host 发生变化，先关闭旧 discovery。
	if globalDiscovery != nil {
		globalDiscovery.Stop()
		globalDiscovery = nil
		globalEtcdHost = ""
	}

	rand.Seed(time.Now().UnixNano())

	dis, err := etcd_api.NewDiscovery(&etcd_api.NodeInfo{}, clientv3.Config{
		Endpoints:   []string{host},
		DialTimeout: 5 * time.Second,
	}, globalNodes)
	if err != nil {
		return err
	}

	// 同步 Pull 一次，保证缓存初始化完成后再对外提供 Pick。
	rev := dis.Pull()
	if rev <= 0 {
		dis.Stop()
		return errors.New("discovery cache init failed: etcd pull failed")
	}

	globalDiscovery = dis
	globalEtcdHost = host

	go globalDiscovery.RunFromRevision(rev)

	log.Printf(
		"[DiscoveryCache] initialized host:%s rev:%d nodes:%d",
		host,
		rev,
		globalNodes.Count(),
	)

	return nil
}

// GetServerNodeByName 通过服务名称获取节点。
// 函数签名保持不变，兼容 PHP 端原有调用。
// 优化前：每次 NewDiscovery + clientv3.New + Pull etcd。
// 优化后：第一次初始化缓存，后续只从内存 globalNodes.Pick()。
func GetServerNodeByName(Name string, Host string) (nodeInfo etcd_api.NodeInfo, err error) {
	if Host == "" {
		return nodeInfo, errors.New("Etcd host does not exist.")
	}

	if Name == "" {
		return nodeInfo, errors.New("server name does not exist.")
	}

	if err := InitDiscoveryCache(Host); err != nil {
		return nodeInfo, err
	}

	nodeInfoData := globalNodes.Pick(Name)
	if nodeInfoData != nil {
		return *nodeInfoData, nil
	}

	return nodeInfo, errors.New("server node not found: " + Name)
}

// StopDiscoveryCache 停止全局服务发现缓存。
// 如果你的 Go 网关有退出钩子，可以调用这个。
func StopDiscoveryCache() {
	discoveryCacheMu.Lock()
	defer discoveryCacheMu.Unlock()

	if globalDiscovery != nil {
		globalDiscovery.Stop()
		globalDiscovery = nil
		globalEtcdHost = ""
	}

	log.Printf("[DiscoveryCache] stopped")
}

// StopAllRegisters 停止所有注册。
// 如果你的 Go 网关有退出钩子，可以调用这个。
func StopAllRegisters() {
	registerMu.Lock()
	defer registerMu.Unlock()

	for key, reg := range registers {
		if reg != nil {
			reg.Stop()
		}
		delete(registers, key)
	}

	log.Printf("[RegisterServer] all registers stopped")
}
