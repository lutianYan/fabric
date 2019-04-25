/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package version

import (
	"fmt"
	"runtime"

	"github.com/hyperledger/fabric/common/metadata"
	"github.com/spf13/cobra"
)

// Program name
const ProgramName = "peer"

// Cmd returns the Cobra Command for Version
//构建 command的初始化命令
func Cmd() *cobra.Command {
	return cobraCommand
}

//PersistentPreRunE先于Run执行用于初始化日志系统，Run此处用于打印版本信息或帮助信息

//RunE，这是cobra的命令对象的入口函数
var cobraCommand = &cobra.Command{
	Use:   "version", //命令名称
	Short: "Print fabric peer version.",
	Long:  `Print current version of the fabric peer server.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("trailing args detected")
		}
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		fmt.Print(GetInfo())
		return nil
	},
}

// GetInfo returns version information for the peer
// 返回peer的版本信息
func GetInfo() string {
	if metadata.Version == "" {
		metadata.Version = "development build"
	}

	ccinfo := fmt.Sprintf(" Base Image Version: %s\n"+
		"  Base Docker Namespace: %s\n"+
		"  Base Docker Label: %s\n"+
		"  Docker Namespace: %s\n",
		metadata.BaseVersion, metadata.BaseDockerNamespace,
		metadata.BaseDockerLabel, metadata.DockerNamespace)

	return fmt.Sprintf("%s:\n Version: %s\n Commit SHA: %s\n Go version: %s\n"+
		" OS/Arch: %s\n"+
		" Chaincode:\n %s\n",
		ProgramName, metadata.Version, metadata.CommitSHA, runtime.Version(),
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH), ccinfo)
}
