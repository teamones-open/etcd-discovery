package common

import (
	"errors"
	"log"
	"net"
)

// 通过DNS获取指定服务地址或者获取本地IP地址
func GetLocalIPv4(serviceName string) (ip string, err error) {

	// 优先尝试通过服务名来获取IPV4
	serviceIPAddr, serviceIPErr := net.ResolveIPAddr("ip", serviceName)

	if serviceIPErr == nil {
		return serviceIPAddr.String(), err
	} else {
		if Mode() == DebugMode {
			log.Printf("[Get IP by service name] error:%s", serviceIPErr.Error())
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}

			if Mode() == DebugMode {
				log.Printf("[Local Ip]:%s", ip.String())
			}

			return ip.String(), nil
		}
	}
	return "", errors.New("No network connected.")
}
