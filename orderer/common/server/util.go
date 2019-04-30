/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	fileledger "github.com/hyperledger/fabric/common/ledger/blockledger/file"
	jsonledger "github.com/hyperledger/fabric/common/ledger/blockledger/json"
	ramledger "github.com/hyperledger/fabric/common/ledger/blockledger/ram"
	config "github.com/hyperledger/fabric/orderer/common/localconfig"
)

//读取Orderer配置对象conf，创建区块账本工厂对象，用于创建指定通道上基于文件的区块数据存储对象（fsBlockStore），负责执行存储区块、查询区块、查询交易数据等操作
func createLedgerFactory(conf *config.TopLevel) (blockledger.Factory, string) {
	var lf blockledger.Factory
	var ld string
	//解析获取conf配置对象中的账本配置类型（包括file文件类型、json文件类型和ram内存类型）
	switch conf.General.LedgerType {
	case "file":
		//获取Orderer节点上的区块账本存储目录ld，包括默认目录（var/hyperledger/production/orderer）或者临时目录中的子目录
		ld = conf.FileLedger.Location
		if ld == "" {
			ld = createTempDir(conf.FileLedger.Prefix)
		}
		logger.Debug("Ledger dir:", ld)
		//创建基于文件的区块账本工厂对象lf
		lf = fileledger.New(ld)
		// The file-based ledger stores the blocks for each channel
		// in a fsblkstorage.ChainsDir sub-directory that we have
		// to create separately. Otherwise the call to the ledger
		// Factory's ChainIDs below will fail (dir won't exist).
		//在区块账本目录下建立一chains命名的子目录（/var/hyperledger/production/orderer/chains），由每个通道账本的区块数据存储对象负责在chains子目录下创建维护以通道ID命名的通道账本子目录，
		// 用于保存该通道账本的所区块数据文件
		//其中，区块数据文件都是以blockfile——num命名，num是6位区块文件编号，不足6位用0补全
		createSubDir(ld, fsblkstorage.ChainsDir)
	case "json":
		ld = conf.FileLedger.Location
		if ld == "" {
			ld = createTempDir(conf.FileLedger.Prefix)
		}
		logger.Debug("Ledger dir:", ld)
		lf = jsonledger.New(ld)
	case "ram":
		fallthrough
	default:
		lf = ramledger.New(int(conf.RAMLedger.HistorySize))
	}
	return lf, ld
}

func createTempDir(dirPrefix string) string {
	dirPath, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		logger.Panic("Error creating temp dir:", err)
	}
	return dirPath
}

func createSubDir(parentDirPath string, subDir string) (string, bool) {
	var created bool
	subDirPath := filepath.Join(parentDirPath, subDir)
	if _, err := os.Stat(subDirPath); err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(subDirPath, 0755); err != nil {
				logger.Panic("Error creating sub dir:", err)
			}
			created = true
		}
	} else {
		logger.Debugf("Found %s sub-dir and using it", fsblkstorage.ChainsDir)
	}
	return subDirPath, created
}
