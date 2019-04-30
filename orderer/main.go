/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package main is the entrypoint for the orderer binary
// and calls only into the server.Main() function.  No other
// function should be included in this package.
package main

import "github.com/hyperledger/fabric/orderer/common/server"

func main() {
	//由orderer start启动主函数
	//初始化相关的配置

	server.Main()
}
