package config

import (
	"fmt"
	"os"
)

var ConfigFile = "D:\\coding\\TestDir\\remote"

const (
	DSN      = "tb_test_user:123456@tcp(%s:3306)/%s?charset=latin1&parseTime=True"
	TestBase = "tb_test"

	RoleDBDSN = "role_test:123456@tcp(%s:3306)/%s?charset=latin1&parseTime=True"

	RawDSN = "%s:%s@tcp(%s:3306)/%s?charset=latin1&parseTime=True"
)

func GetRemoteIP() string {
	content, err := os.ReadFile(ConfigFile)
	if err != nil {
		fmt.Println("read config file error: ", err)
		return ""
	}

	return string(content)
}

func GetRemoteMysqlDSN() string {
	fullDSN := fmt.Sprintf(DSN, GetRemoteIP(), TestBase)
	fmt.Println("GetRemoteMysqlDSN return: " + fullDSN)
	return fullDSN
}

func GetMysqlDSNByAll(database, username, password string) string {
	ip := GetRemoteIP()
	fullDSN := fmt.Sprintf(RawDSN, username, password, ip, database)
	fmt.Println("GetMysqlDSNByAll return: " + fullDSN)
	return fullDSN
}
