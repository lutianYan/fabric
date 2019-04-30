/*
Copyright IBM Corp. 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fileledger

import (
	"sync"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
)

type fileLedgerFactory struct {
	blkstorageProvider blkstorage.BlockStoreProvider //区块数据存储对象提供者，用于生成指定通道上基于文件的区块句存储对象，并指定其初始化配置信息（区块文件配置conf、区块索引配置indexConfig、区块索引数据库提供者leveldbProvider）
	ledgers            map[string]blockledger.ReadWriter //账本字典，记录通道Id与区块账本对象之间的映射关系，管理Orderer节点上所有通道的区块正本对象（FileLedger类型），封装了对应的区块数据存储对象，用于访问区块数据文件
	mutex              sync.Mutex //同步锁，用于控制账本字典ledgers上 读写同步操作
}

// GetOrCreate gets an existing ledger (if it exists) or creates it if it does not
//创建或者获取账本
func (flf *fileLedgerFactory) GetOrCreate(chainID string) (blockledger.ReadWriter, error) {
	flf.mutex.Lock()
	defer flf.mutex.Unlock()

	key := chainID
	// check cache
	//检查区块账本工厂对象上的账本字典中是否存在指定通道对应的区块账本对象
	ledger, ok := flf.ledgers[key]
	if ok {
		return ledger, nil
	}
	// open fresh
	//创建该通道上的区块数据存储对象blockStore（fsBlockStore对象），负责执行存储区块数据与区块链信息、获取区块与交易数据等
	blockStore, err := flf.blkstorageProvider.OpenBlockStore(key)
	if err != nil {
		return nil, err
	}
	//创建当前通道的区块账本对象ledger（FileLedger类型）
	ledger = NewFileLedger(blockStore)
	//将区块账本对象注册到账本工厂对象的账本字典中
	flf.ledgers[key] = ledger
	//将账本对象ledger返回
	return ledger, nil
}

// ChainIDs returns the chain IDs the factory is aware of
//读取Orderer节点上区块账本目录（var/hyperledger/production/orderer/chains）中的所有通道账本子目录名称，即当前节点上已经成功创建的现存通道ID列表existingChains
func (flf *fileLedgerFactory) ChainIDs() []string {
	chainIDs, err := flf.blkstorageProvider.List()
	if err != nil {
		logger.Panic(err)
	}
	return chainIDs
}

// Close releases all resources acquired by the factory
func (flf *fileLedgerFactory) Close() {
	flf.blkstorageProvider.Close()
}

// New creates a new ledger factory
func New(directory string) blockledger.Factory {
	return &fileLedgerFactory{
		blkstorageProvider: fsblkstorage.NewProvider(
			fsblkstorage.NewConf(directory, -1),
			&blkstorage.IndexConfig{
				AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum}},
		),
		ledgers: make(map[string]blockledger.ReadWriter),
	}
}
