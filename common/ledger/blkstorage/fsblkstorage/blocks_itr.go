/*
Copyright IBM Corp. 2016 All Rights Reserved.

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
//以迭代器的形式读取信息
package fsblkstorage

import (
	"sync"

	"github.com/hyperledger/fabric/common/ledger"
)

// blocksItr - an iterator for iterating over a sequence of blocks
//可以完全基于文件流上的区块信息读取操作
type blocksItr struct {
	mgr                  *blockfileMgr
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	stream               *blockStream
	closeMarker          bool
	closeMarkerLock      *sync.Mutex
}

func newBlockItr(mgr *blockfileMgr, startBlockNum uint64) *blocksItr {
	return &blocksItr{mgr, mgr.cpInfo.lastBlockNumber, startBlockNum, nil, false, &sync.Mutex{}}
}

//Orderer节点阻塞等待新区块，直到指定区块创建提交
func (itr *blocksItr) waitForBlock(blockNum uint64) uint64 {
	itr.mgr.cpInfoCond.L.Lock()
	defer itr.mgr.cpInfoCond.L.Unlock()
	//判断检查点信息中的最新区块号与要同步的区块的号，如果小，则说明指定 区块还没有被创建提交
	for itr.mgr.cpInfo.lastBlockNumber < blockNum && !itr.shouldClose() {
		//等待新区块，直到出现指定请求的区块数据
		logger.Debugf("Going to wait for newer blocks. maxAvailaBlockNumber=[%d], waitForBlockNum=[%d]",
			itr.mgr.cpInfo.lastBlockNumber, blockNum)
		//阻塞等待，直到有新的区块生成通知唤醒
		itr.mgr.cpInfoCond.Wait()
		logger.Debugf("Came out of wait. maxAvailaBlockNumber=[%d]", itr.mgr.cpInfo.lastBlockNumber)
	}
	//等待指定区块创建完毕，返回该区块号
	return itr.mgr.cpInfo.lastBlockNumber
}

func (itr *blocksItr) initStream() error {
	var lp *fileLocPointer
	var err error
	//获取区块迭代器对应的区块的文件位置指针
	if lp, err = itr.mgr.index.getBlockLocByBlockNum(itr.blockNumToRetrieve); err != nil {
		return err
	}
	//基于该文件指针以及账本区块文件目录，创建指定区块的区块文件流
	if itr.stream, err = newBlockStream(itr.mgr.rootDir, lp.fileSuffixNum, int64(lp.offset), -1); err != nil {
		return err
	}
	return nil
}

func (itr *blocksItr) shouldClose() bool {
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	return itr.closeMarker
}

// Next moves the cursor to next block and returns true iff the iterator is not exhausted
//获取下一个区块
func (itr *blocksItr) Next() (ledger.QueryResult, error) {
	//检查请求区块号若大于当前的最大区块号，则阻塞等待
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		//用于创建消息处理循环，以等待获取指定的区块数据
		itr.maxBlockNumAvailable = itr.waitForBlock(itr.blockNumToRetrieve)
	}
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	//已经关闭，则直接返回nil
	if itr.closeMarker {
		return nil, nil
	}
	//检查区块文件流
	if itr.stream == nil {
		logger.Debugf("Initializing block stream for iterator. itr.maxBlockNumAvailable=%d", itr.maxBlockNumAvailable)
		//初始化指定区块号的区块数据文件流
		if err := itr.initStream(); err != nil {
			return nil, err
		}
	}
	//获取下一个区块的字节数组
	nextBlockBytes, err := itr.stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	//指定获取的区块号加1
	itr.blockNumToRetrieve++
	//解析区块数据
	//解析出去块头、交易数据集合以及元数据、重新构造区块对象，再将请求的区块一次返回到deliverBlocks（）方法中，并回复给请求客户端
	return deserializeBlock(nextBlockBytes)
}

// Close releases any resources held by the iterator
func (itr *blocksItr) Close() {
	itr.mgr.cpInfoCond.L.Lock()
	defer itr.mgr.cpInfoCond.L.Unlock()
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	itr.closeMarker = true
	itr.mgr.cpInfoCond.Broadcast()
	if itr.stream != nil {
		itr.stream.close()
	}
}
