package config

import (
	"fmt"
	"time"
)

const DialTimeout = 5 * time.Second
const LeaseTimeout = 15 * time.Second

const ETCDUrlFormat = "http://%s:%d"
const ETCDPort1 = 2379
const ETCDPort2 = 12379
const ETCDPort3 = 22379

func GetETCDEndpoint() []string {
	endpoints := []string{
		"http://192.168.86.148:2379",
		"http://192.168.86.148:12379",
		"http://192.168.86.148:22379",
	}

	return endpoints
}

func GetETCDEndpointsByLocalFile() []string {
	remoteIP := GetRemoteIP()
	endpoints := []string{
		fmt.Sprintf(ETCDUrlFormat, remoteIP, ETCDPort1),
		fmt.Sprintf(ETCDUrlFormat, remoteIP, ETCDPort2),
		fmt.Sprintf(ETCDUrlFormat, remoteIP, ETCDPort3),
	}

	return endpoints
}
