package etcd_discovery

import (
	"errors"
	uuid "github.com/satori/go.uuid"
	"github.com/teamones-open/etcd-discovery/common"
	"github.com/teamones-open/etcd-discovery/etcd_api"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type RegisterParam struct {
	UUID string `json:"uuid"` // 服务唯一id
	Name string `json:"name"` // 服务名
	Port string `json:"port"` // 服务端口
}

func RegisterServer(host string, port string, serverName string) (err error) {

	var nodeInfo etcd_api.NodeInfo

	if host == "" {
		err = errors.New("Etcd host does not exist.")
		return
	}

	nodeInfo.Name = serverName

	ip, ipErr := common.GetLocalIPv4(nodeInfo.Name)

	if ipErr != nil {
		return ipErr
	}

	nodeInfo.Addr = ip + ":" + port
	nodeInfo.UniqueId = "discovery/" + nodeInfo.Name + "/" + uuid.NewV4().String()

	reg, RegisterErr := etcd_api.NewRegister(&nodeInfo, clientv3.Config{
		Endpoints:   []string{host},
		DialTimeout: 5 * time.Second,
	})

	if RegisterErr != nil {
		return RegisterErr
	}

	if reg != nil {
		go reg.Run()
	}

	return
}

// 通过服务名称获取ETCD节点配置
func GetServerNodeByName(Name string, Host string) (nodeInfo etcd_api.NodeInfo, err error) {

	if Host == "" {
		err = errors.New("Etcd host does not exist.")
		return
	}

	nodes := etcd_api.NewNodeManager()

	dis, discoveryErr := etcd_api.NewDiscovery(&etcd_api.NodeInfo{
		Name: Name,
	}, clientv3.Config{
		Endpoints:   []string{Host},
		DialTimeout: 5 * time.Second,
	}, nodes)

	if discoveryErr != nil {
		return nodeInfo, discoveryErr
	}

	dis.Pull()
	nodeInfoData := nodes.Pick(Name)

	if nodeInfoData != nil {
		return *nodeInfoData, err
	}

	return
}
