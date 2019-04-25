/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/hyperledger/fabric/peer/chaincode"
	"github.com/hyperledger/fabric/peer/channel"
	"github.com/hyperledger/fabric/peer/clilogging"
	"github.com/hyperledger/fabric/peer/common"
	"github.com/hyperledger/fabric/peer/node"
	"github.com/hyperledger/fabric/peer/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The main command describes the service and
// defaults to printing the help message.
//mainCmd的初始化，其中Use为命令名称
var mainCmd = &cobra.Command{
	Use: "peer"}

func main() {

	// For environment variables.
	//设置了环境变量前置
	viper.SetEnvPrefix(common.CmdRoot)
	//将环境变量加载进来
	viper.AutomaticEnv()
	//将环境变量中的_换成.，这样就和yaml文件的配置相匹配了
	replacer := strings.NewReplacer(".", "_")
	//因为viper读取yaml文件所形成的配置项就是按层级并以.分隔的格式，如peer.address
	viper.SetEnvKeyReplacer(replacer)

	// Define command-line flags that are valid for all peer commands and
	// subcommands.
	//为命令添加flag，也就是添加命令行选项,比如下面这个就是peer的主命令添加了一个version选项，各个用法类似于go的flag包
	mainFlags := mainCmd.PersistentFlags()

	//--logging-level和--test.coverprofile分别用于版本信息、日志级别和测试覆盖率分析
	mainFlags.String("logging-level", "", "Legacy logging level flag")
	viper.BindPFlag("logging_level", mainFlags.Lookup("logging-level"))
	mainFlags.MarkHidden("logging-level")

	//添加子命令
	mainCmd.AddCommand(version.Cmd())
	mainCmd.AddCommand(node.Cmd())
	mainCmd.AddCommand(chaincode.Cmd(nil))
	mainCmd.AddCommand(clilogging.Cmd(nil))
	mainCmd.AddCommand(channel.Cmd(nil))

	// On failure Cobra prints the usage message and error string, so we only
	// need to exit with a non-0 status
	//mainCmd.Execute()为命令启动
	if mainCmd.Execute() != nil {
		os.Exit(1)
	}
}
