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
//管理如何存储区块
//
//管理区块文件系统中不同类型的信息
package fsblkstorage

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	putil "github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("fsblkstorage")

const (
	blockfilePrefix = "blockfile_"
)

var (
	blkMgrInfoKey = []byte("blkMgrInfo")
)

type blockfileMgr struct {
	rootDir           string
	conf              *Conf
	db                *leveldbhelper.DBHandle
	index             index
	cpInfo            *checkpointInfo
	//go语言中sync包提供了一个Cond类，这个类用于goroutine之间进行协作
	//这个类只有三个函数，Broadcast（），Signal()和Wait()，一个成员变量L Lock
	//其中Broadcast是唤醒cond上等待的所有的goroutine，而Signal只是选择一个进行唤醒，wait是让goroutine在这个cond上进行等待
	//注意：wait在调用的时候一定要确保已经获得了成员变量L锁，因为wait第一件事情就是解锁，但是wait结束等待返回之前会对L进行枷锁，也即是，当wait结束，调用它的goroutine仍然会获取Lock L
	//调用Broadcast会导致系统切换到之前在等待的那个goroutine进行执行
	cpInfoCond        *sync.Cond
	currentFileWriter *blockfileWriter
	bcInfo            atomic.Value
}

/*
Creates a new manager that will manage the files used for block persistence.
This manager manages the file system FS including
  -- the directory where the files are stored
  -- the individual files where the blocks are stored
  -- the checkpoint which tracks the latest file being persisted to
  -- the index which tracks what block and transaction is in what file
When a new blockfile manager is started (i.e. only on start-up), it checks
if this start-up is the first time the system is coming up or is this a restart
of the system.

The blockfile manager stores blocks of data into a file system.  That file
storage is done by creating sequentially numbered files of a configured size
i.e blockfile_000000, blockfile_000001, etc..

Each transaction in a block is stored with information about the number of
bytes in that transaction
 Adding txLoc [fileSuffixNum=0, offset=3, bytesLength=104] for tx [1:0] to index
 Adding txLoc [fileSuffixNum=0, offset=107, bytesLength=104] for tx [1:1] to index
Each block is stored with the total encoded length of that block as well as the
tx location offsets.

Remember that these steps are only done once at start-up of the system.
At start up a new manager:
  *) Checks if the directory for storing files exists, if not creates the dir
  *) Checks if the key value database exists, if not creates one
       (will create a db dir)
  *) Determines the checkpoint information (cpinfo) used for storage
		-- Loads from db if exist, if not instantiate a new cpinfo
		-- If cpinfo was loaded from db, compares to FS
		-- If cpinfo and file system are not in sync, syncs cpInfo from FS
  *) Starts a new file writer
		-- truncates file per cpinfo to remove any excess past last block
  *) Determines the index information used to find tx and blocks in
  the file blkstorage
		-- Instantiates a new blockIdxInfo
		-- Loads the index from the db if exists
		-- syncIndex comparing the last block indexed to what is in the FS
		-- If index and file system are not in sync, syncs index from the FS
  *)  Updates blockchain info used by the APIs
*/
//创建一个新的block存储管理者，接受了配置、索引配置和一个leveldb数据库三个参数
//配置中包含了block存储路径等信息
func newBlockfileMgr(id string, conf *Conf, indexConfig *blkstorage.IndexConfig, indexStore *leveldbhelper.DBHandle) *blockfileMgr {
	logger.Debugf("newBlockfileMgr() initializing file-based block storage for ledger: %s ", id)
	//Determine the root directory for the blockfile storage, if it does not exist create it
	rootDir := conf.getLedgerBlockDir(id)
	//检测用于存储信息的文件的文件夹是否存在，若不存在则创建文件夹
	_, err := util.CreateDirIfMissing(rootDir)
	if err != nil {
		panic(fmt.Sprintf("Error: %s", err))
	}
	//实例化一个blockfileMgr对象
	//Instantiate the manager, i.e. blockFileMgr structure
	mgr := &blockfileMgr{rootDir: rootDir, conf: conf, db: indexStore}

	// cp = checkpointInfo, retrieve from the database the file suffix or number of where blocks were stored.
	// It also retrieves the current size of that file and the last block number that was written to that file.
	// At init checkpointInfo:latestFileChunkSuffixNum=[0], latestFileChunksize=[0], lastBlockNumber=[0]
	//从leveldb中取出最新的检查点数据
	cpInfo, err := mgr.loadCurrentInfo()

	if err != nil {
		panic(fmt.Sprintf("Could not get block file info for current block file from db: %s", err))
	}
	//如果是空，说明当前新建的block存储管理者管理的是一个比较新的账本，要么什么数据都没有写入，要么写入了第一块block数据包的部分数据，此时可以将cpinfo更新为blockfile文件开始处的信息
	if cpInfo == nil {
		logger.Info(`Getting block information from block storage`)
		if cpInfo, err = constructCheckpointInfoFromBlockFiles(rootDir); err != nil {
			panic(fmt.Sprintf("Could not build checkpoint info from block files: %s", err))
		}
		logger.Debugf("Info constructed by scanning the blocks dir = %s", spew.Sdump(cpInfo))
	} else {
		logger.Debug(`Synching block information from block storage (if needed)`)
		//根据取出来的cpInfo同步更新的函数
		syncCPInfoFromFS(rootDir, cpInfo)
	}
	err = mgr.saveCurrentInfo(cpInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}

	//Open a writer to the file identified by the number and truncate it to only contain the latest block
	// that was completely saved (file system, index, cpinfo, etc)
	currentFileWriter, err := newBlockfileWriter(deriveBlockfilePath(rootDir, cpInfo.latestFileChunkSuffixNum))
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to current file: %s", err))
	}
	//Truncate the file to remove excess past last block
	//根据更新后的cpInfo所存储的最后一个block数据包的末尾位置，直接截断该blockfile文件
	err = currentFileWriter.truncateFile(cpInfo.latestFileChunksize)
	if err != nil {
		panic(fmt.Sprintf("Could not truncate current file to known size in db: %s", err))
	}

	// Create a new KeyValue store database handler for the blocks index in the keyvalue database
	//确定用于在文件区块中查找交易和区块信息的索引
	//实例化一个新的blockIdxInfo
	if mgr.index, err = newBlockIndex(indexConfig, indexStore); err != nil {
		panic(fmt.Sprintf("error in block index: %s", err))
	}

	// Update the manager with the checkpoint info and the file writer
	mgr.cpInfo = cpInfo
	mgr.currentFileWriter = currentFileWriter
	// Create a checkpoint condition (event) variable, for the  goroutine waiting for
	// or announcing the occurrence of an event.
	mgr.cpInfoCond = sync.NewCond(&sync.Mutex{})

	// init BlockchainInfo for external API's
	bcInfo := &common.BlockchainInfo{
		Height:            0,
		CurrentBlockHash:  nil,
		PreviousBlockHash: nil}

	if !cpInfo.isChainEmpty {
		//If start up is a restart of an existing storage, sync the index from block storage and update BlockchainInfo for external API's
		//根据更新后的cpInfo和自身的检查点信息，创建同步的索引
		mgr.syncIndex()
		lastBlockHeader, err := mgr.retrieveBlockHeaderByNumber(cpInfo.lastBlockNumber)
		if err != nil {
			panic(fmt.Sprintf("Could not retrieve header of the last block form file: %s", err))
		}
		lastBlockHash := lastBlockHeader.Hash()
		previousBlockHash := lastBlockHeader.PreviousHash
		bcInfo = &common.BlockchainInfo{
			Height:            cpInfo.lastBlockNumber + 1,
			CurrentBlockHash:  lastBlockHash,
			PreviousBlockHash: previousBlockHash}
	}
	mgr.bcInfo.Store(bcInfo)
	return mgr
}

//cp = checkpointInfo, from the database gets the file suffix and the size of
// the file of where the last block was written.  Also retrieves contains the
// last block number that was written.  At init
//checkpointInfo:latestFileChunkSuffixNum=[0], latestFileChunksize=[0], lastBlockNumber=[0]
//根据读取出来的cpInfo同步更新的函数
func syncCPInfoFromFS(rootDir string, cpInfo *checkpointInfo) {
	logger.Debugf("Starting checkpoint=%s", cpInfo)
	//Checks if the file suffix of where the last block was written exists
	filePath := deriveBlockfilePath(rootDir, cpInfo.latestFileChunkSuffixNum)
	exists, size, err := util.FileExists(filePath)
	if err != nil {
		panic(fmt.Sprintf("Error in checking whether file [%s] exists: %s", filePath, err))
	}
	logger.Debugf("status of file [%s]: exists=[%t], size=[%d]", filePath, exists, size)
	//Test is !exists because when file number is first used the file does not exist yet
	//checks that the file exists and that the size of the file is what is stored in cpinfo
	//status of file [/tmp/tests/ledger/blkstorage/fsblkstorage/blocks/blockfile_000000]: exists=[false], size=[0]
	if !exists || int(size) == cpInfo.latestFileChunksize {
		// check point info is in sync with the file on disk
		return
	}
	//Scan the file system to verify that the checkpoint info stored in db is correct
	//尝试读取blockfile最后一块的函数
	//三个参数：哪个目录、哪个文件（后缀）、从文件里面的哪个位置开始读取（也就是cpInfo所代表的block数据包末尾的下一个位置，下一个block数据包开始的位置，尝试读取一个完整的block数据包）
	_, endOffsetLastBlock, numBlocks, err := scanForLastCompleteBlock(
		rootDir, cpInfo.latestFileChunkSuffixNum, int64(cpInfo.latestFileChunksize))
	if err != nil {
		panic(fmt.Sprintf("Could not open current file for detecting last block in the file: %s", err))
	}
	cpInfo.latestFileChunksize = int(endOffsetLastBlock)
	if numBlocks == 0 {
		return
	}
	//Updates the checkpoint info for the actual last block number stored and it's end location
	if cpInfo.isChainEmpty {
		cpInfo.lastBlockNumber = uint64(numBlocks - 1)
	} else {
		cpInfo.lastBlockNumber += uint64(numBlocks)
	}
	cpInfo.isChainEmpty = false
	logger.Debugf("Checkpoint after updates by scanning the last file segment:%s", cpInfo)
}

func deriveBlockfilePath(rootDir string, suffixNum int) string {
	return rootDir + "/" + blockfilePrefix + fmt.Sprintf("%06d", suffixNum)
}

func (mgr *blockfileMgr) close() {
	mgr.currentFileWriter.close()
}

func (mgr *blockfileMgr) moveToNextFile() {
	//创建新的检查点
	cpInfo := &checkpointInfo{
		latestFileChunkSuffixNum: mgr.cpInfo.latestFileChunkSuffixNum + 1,
		latestFileChunksize:      0,
		lastBlockNumber:          mgr.cpInfo.lastBlockNumber}

	nextFileWriter, err := newBlockfileWriter(
		deriveBlockfilePath(mgr.rootDir, cpInfo.latestFileChunkSuffixNum))

	if err != nil {
		panic(fmt.Sprintf("Could not open writer to next file: %s", err))
	}
	//关闭文件
	mgr.currentFileWriter.close()
	//保存检查点，写入到磁盘中
	err = mgr.saveCurrentInfo(cpInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}
	mgr.currentFileWriter = nextFileWriter
	mgr.updateCheckpoint(cpInfo)
}

//对一个block数据块进行添加
//负责添加Block
func (mgr *blockfileMgr) addBlock(block *common.Block) error {
	//验证添加block数据的序列号是否为下一个block应该有的，也就是序列号是否连续
	//校验区块编号，必须与BlockChain的高度相同
	if block.Header.Number != mgr.getBlockchainInfo().Height {
		return fmt.Errorf("Block number should have been %d but was %d", mgr.getBlockchainInfo().Height, block.Header.Number)
	}
	//将block数据进行串行化，以便转化为可以直接写入blockfile文件的数据，这里的blockBytes也就是B
	//使用protobuf将Block序列化
	//在序列化的过程中，可以提取出Block的交易id列表，元数据等信息，保存在serializedBlockInfo中;还可以计算出区块占用的字节数
	blockBytes, info, err := serializeBlock(block)
	if err != nil {
		return fmt.Errorf("Error while serializing block: %s", err)
	}
	//计算Block的Hash值，逻辑为将Block的编号，上一个Block的Hash和当前区块的DataHash使用ASN.1 编码为byte[]，然后再使用SHA-256生成当前区块的Hash值
	blockHash := block.Header.Hash()
	//Get the location / offset where each transaction starts in the block and where the block ends
	//获取区块中每个交易的开始位置和偏移，区块的结束位置
	txOffsets := info.txOffsets
	//当前blockfile的大小，也就是准备开始写当前添加的block数据包的位置
	//latestFileChunksize记录当前的file的位置
	currentOffset := mgr.cpInfo.latestFileChunksize
	if err != nil {
		return fmt.Errorf("Error while serializing block: %s", err)
	}
	//记录序列化后区块的长度
	blockBytesLen := len(blockBytes)
	//为了节省空间和方便读取，胡idun这个整数进行编码变成一个更长的数据
	//压缩了block的大小值，这里的blockbytesEncodedLen也就是A
	blockBytesEncodedLen := proto.EncodeVarint(uint64(blockBytesLen))
	//block数据包的总共的大小
	//区块数据本身的长度和记录区块数据长度的数据的长度之和
	totalBytesToAppend := blockBytesLen + len(blockBytesEncodedLen)

	//Determine if we need to start a new file since the size of this block
	//exceeds the amount of space left in the current file
	//当前文件的大小与要添加的block数据包的大小之和大于配置中规定的blockfile的大小限制，则直接启用下一个blockfile文件来存储这个新的block数据包
	//当前的区块的大小大于区块文件的大小
	if currentOffset+totalBytesToAppend > mgr.conf.maxBlockfileSize {
		mgr.moveToNextFile()
		currentOffset = 0
	}
	//append blockBytesEncodedLen to the file
	//非同步的在上一步确定的blockfile文件和文件中的位置处写入A，第二个参数给的是false，因此此时A只是在缓存中
	err = mgr.currentFileWriter.append(blockBytesEncodedLen, false)
	if err == nil {
		//append the actual block bytes to the file
		//写入真实的block数据B
		//会调用file.Sync(),手工刷新缓存将缓存中的数据写入实际的磁盘文件中。。把当前内容持久化，一般就是马上写入到磁盘
		//A和B一起写入实际的磁盘文件，只是为了减少磁盘文件的操作
		err = mgr.currentFileWriter.append(blockBytes, true)
	}
	//如果写入A 和B的过程中出现错误，则直接把文件从最开始的写的地方截断，也就是将可能新写入的部分block数据包的数据删除，然后添加程序结束返回一个错误
	if err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(mgr.cpInfo.latestFileChunksize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Could not truncate current file to known size after an error during block append: %s", err))
		}
		return fmt.Errorf("Error while appending block to file: %s", err)
	}

	//Update the checkpoint info with the results of adding the new block
	//增加新的区块后，更新检查点
	currentCPInfo := mgr.cpInfo
	//创建新的检查点
	newCPInfo := &checkpointInfo{
		latestFileChunkSuffixNum: currentCPInfo.latestFileChunkSuffixNum,
		latestFileChunksize:      currentCPInfo.latestFileChunksize + totalBytesToAppend,
		isChainEmpty:             false,
		lastBlockNumber:          block.Header.Number}
	//save the checkpoint information in the database
	//非同步的将新的检查点写入leveldb中，如果有错误也是直接截断，将blockfile恢复
	if err = mgr.saveCurrentInfo(newCPInfo, false); err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(currentCPInfo.latestFileChunksize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Error in truncating current file to known size after an error in saving checkpoint info: %s", err))
		}
		return fmt.Errorf("Error while saving current file info to db: %s", err)
	}

	//Index block file location pointer updated with file suffex and offset for the new block
	//根据新的检查点newCPinfo和开始写入block数据包时的偏移信息，创建一个block数据包的位置信息blockFLP
	blockFLP := &fileLocPointer{fileSuffixNum: newCPInfo.latestFileChunkSuffixNum}
	blockFLP.offset = currentOffset
	// shift the txoffset because we prepend length of bytes before block bytes
	for _, txOffset := range txOffsets {
		//第二次计算block中包含的每笔交易的偏移，即加上了A所占的字节数，计算每个交易从A处计算起的偏移量，也可以说是每笔交易在block数据包中的偏移
		txOffset.loc.offset += len(blockBytesEncodedLen)
	}
	//save the index in the database
	//将收集到的索引用到的关于新的block数据包内的信息传入，创建一个新的block数据包的徐哦因，在这个函数中，会逐个写入1～6的索引项，其中在添加索引3和4这两个与交易有关的索引的时候，会调用txFlp：=newFileLocationPointer()
	//以第三次计算每个交易的偏移信息，确定每笔交易在blockfile文件内的偏移，同时也会在txFlp中记录每笔交易的大小
	//在所有的索引项都pi写入后，然后通过batch.Put()写入索引的检查点，然后非同步的写入leveldb数据库中
	//非同步的写入，执行后这些数据是写入leveldb的缓存区的而不是数据库中，这样可以提高写入的效率
	if err = mgr.index.indexBlock(&blockIdxInfo{
		blockNum: block.Header.Number, blockHash: blockHash,
		flp: blockFLP, txOffsets: txOffsets, metadata: block.Metadata}); err != nil {
		return err
	}

	//update the checkpoint info (for storage) and the blockchain info (for APIs) in the manager
	//更新了blockfileMgr对象保存的检查点cpInfo
	mgr.updateCheckpoint(newCPInfo)
	//原子性的存储链，也就是账本的信息 一个blockchainInfo对象
	//该对象的Height属性保存了链的高度，Current-BlockHash保存了当前链存储的最新的block的hash，previousBlockHash保存了上一个block的hash值
	mgr.updateBlockchainInfo(blockHash, block)
	return nil
}

func (mgr *blockfileMgr) syncIndex() error {
	var lastBlockIndexed uint64
	var indexEmpty bool
	var err error
	//from the database, get the last block that was indexed
	//获取索引的检查点，获取的是解码后的block序列号，也就是序列号为lastBlockIndexed的block的数据包已经完整存储
	if lastBlockIndexed, err = mgr.index.getLastBlockIndexed(); err != nil {
		if err != errIndexEmpty {
			return err
		}
		indexEmpty = true
	}

	//initialize index to file number:zero, offset:zero and blockNum:0
	startFileNum := 0
	startOffset := 0
	skipFirstBlock := false
	//get the last file that blocks were added to using the checkpoint info
	endFileNum := mgr.cpInfo.latestFileChunkSuffixNum
	startingBlockNum := uint64(0)

	//if the index stored in the db has value, update the index information with those values
	if !indexEmpty {
		if lastBlockIndexed == mgr.cpInfo.lastBlockNumber {
			logger.Debug("Both the block files and indices are in sync.")
			return nil
		}
		logger.Debugf("Last block indexed [%d], Last block present in block files [%d]", lastBlockIndexed, mgr.cpInfo.lastBlockNumber)
		var flp *fileLocPointer
		//是使用blockNumIdxKeyPrefix+blockID的索引方式获取序号为lastBlockIndexed的block数据包位置信息flp
		//又因为能成功获取索引的检查点，所以可知该block数据包一定是完整存储的，因此可以直接跳过这个block数据包而去尝试读取下一个block，这也是函数中skipFirstBlock变量所起到的作用。
		if flp, err = mgr.index.getBlockLocByBlockNum(lastBlockIndexed); err != nil {
			return err
		}
		startFileNum = flp.fileSuffixNum
		startOffset = flp.locPointer.offset
		skipFirstBlock = true
		startingBlockNum = lastBlockIndexed + 1
	} else {
		logger.Debugf("No block indexed, Last block present in block files=[%d]", mgr.cpInfo.lastBlockNumber)
	}

	logger.Infof("Start building index from block [%d] to last block [%d]", startingBlockNum, mgr.cpInfo.lastBlockNumber)

	//open a blockstream to the file location that was stored in the index
	var stream *blockStream
	if stream, err = newBlockStream(mgr.rootDir, startFileNum, int64(startOffset), endFileNum); err != nil {
		return err
	}
	var blockBytes []byte
	var blockPlacementInfo *blockPlacementInfo

	if skipFirstBlock {
		if blockBytes, _, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			return fmt.Errorf("block bytes for block num = [%d] should not be nil here. The indexes for the block are already present",
				lastBlockIndexed)
		}
	}

	//Should be at the last block already, but go ahead and loop looking for next blockBytes.
	//If there is another block, add it to the index.
	//This will ensure block indexes are correct, for example if peer had crashed before indexes got updated.
	blockIdxInfo := &blockIdxInfo{}
	for {
		if blockBytes, blockPlacementInfo, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			break
		}
		//根据读取出来的block数据包创建新的索引信息，然后调用mgr.index.indexBlock(blockIdxInfo)将新的block数据包的索引写入leveldb中，从而完成索引的更新同步
		info, err := extractSerializedBlockInfo(blockBytes)
		if err != nil {
			return err
		}

		//The blockStartOffset will get applied to the txOffsets prior to indexing within indexBlock(),
		//therefore just shift by the difference between blockBytesOffset and blockStartOffset
		numBytesToShift := int(blockPlacementInfo.blockBytesOffset - blockPlacementInfo.blockStartOffset)
		for _, offset := range info.txOffsets {
			offset.loc.offset += numBytesToShift
		}

		//Update the blockIndexInfo with what was actually stored in file system
		blockIdxInfo.blockHash = info.blockHeader.Hash()
		blockIdxInfo.blockNum = info.blockHeader.Number
		blockIdxInfo.flp = &fileLocPointer{fileSuffixNum: blockPlacementInfo.fileNum,
			locPointer: locPointer{offset: int(blockPlacementInfo.blockStartOffset)}}
		blockIdxInfo.txOffsets = info.txOffsets
		blockIdxInfo.metadata = info.metadata

		logger.Debugf("syncIndex() indexing block [%d]", blockIdxInfo.blockNum)
		//将新的block数据包的索引写入leveldb中，从而完成索引的更新同步
		if err = mgr.index.indexBlock(blockIdxInfo); err != nil {
			return err
		}
		if blockIdxInfo.blockNum%10000 == 0 {
			logger.Infof("Indexed block number [%d]", blockIdxInfo.blockNum)
		}
	}
	logger.Infof("Finished building index. Last block indexed [%d]", blockIdxInfo.blockNum)
	return nil
}

func (mgr *blockfileMgr) getBlockchainInfo() *common.BlockchainInfo {
	return mgr.bcInfo.Load().(*common.BlockchainInfo)
}

func (mgr *blockfileMgr) updateCheckpoint(cpInfo *checkpointInfo) {
	//枷锁
	mgr.cpInfoCond.L.Lock()
	defer mgr.cpInfoCond.L.Unlock()
	mgr.cpInfo = cpInfo
	logger.Debugf("Broadcasting about update checkpointInfo: %s", cpInfo)
	//唤醒所有等待的goroutine
	//唤醒所有等待该同步条件变量cpInfoCond的程序，通知已经有新区块提交到账本中
	//例如：Orderer节点阻塞等待的Deliver消息处理句柄会被重新唤醒，将指定的区块数据发送给通道组织的Leader主节点
	mgr.cpInfoCond.Broadcast()
}

func (mgr *blockfileMgr) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo := mgr.getBlockchainInfo()
	newBCInfo := &common.BlockchainInfo{
		Height:            currentBCInfo.Height + 1,
		CurrentBlockHash:  latestBlockHash,
		PreviousBlockHash: latestBlock.Header.PreviousHash}

	mgr.bcInfo.Store(newBCInfo)
}
//关于查询block数据很简单，不同索引所提供了不同的查询方式，均集中在blockfile_mgr.go，是一系列retrieve__By__
//这些函数主要通过leveldb中保存的block数据包的索引来获取block数据
func (mgr *blockfileMgr) retrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	logger.Debugf("retrieveBlockByHash() - blockHash = [%#v]", blockHash)
	loc, err := mgr.index.getBlockLocByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	logger.Debugf("retrieveBlockByNumber() - blockNum = [%d]", blockNum)

	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		blockNum = mgr.getBlockchainInfo().Height - 1
	}

	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockByTxID(txID string) (*common.Block, error) {
	logger.Debugf("retrieveBlockByTxID() - txID = [%s]", txID)

	loc, err := mgr.index.getBlockLocByTxID(txID)

	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	logger.Debugf("retrieveTxValidationCodeByTxID() - txID = [%s]", txID)
	return mgr.index.getTxValidationCodeByTxID(txID)
}

func (mgr *blockfileMgr) retrieveBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error) {
	logger.Debugf("retrieveBlockHeaderByNumber() - blockNum = [%d]", blockNum)
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	blockBytes, err := mgr.fetchBlockBytes(loc)
	if err != nil {
		return nil, err
	}
	info, err := extractSerializedBlockInfo(blockBytes)
	if err != nil {
		return nil, err
	}
	return info.blockHeader, nil
}

func (mgr *blockfileMgr) retrieveBlocks(startNum uint64) (*blocksItr, error) {
	return newBlockItr(mgr, startNum), nil
}

func (mgr *blockfileMgr) retrieveTransactionByID(txID string) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByID() - txId = [%s]", txID)
	loc, err := mgr.index.getTxLoc(txID)
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

func (mgr *blockfileMgr) retrieveTransactionByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByBlockNumTranNum() - blockNum = [%d], tranNum = [%d]", blockNum, tranNum)
	loc, err := mgr.index.getTXLocByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

func (mgr *blockfileMgr) fetchBlock(lp *fileLocPointer) (*common.Block, error) {
	blockBytes, err := mgr.fetchBlockBytes(lp)
	if err != nil {
		return nil, err
	}
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (mgr *blockfileMgr) fetchTransactionEnvelope(lp *fileLocPointer) (*common.Envelope, error) {
	logger.Debugf("Entering fetchTransactionEnvelope() %v\n", lp)
	var err error
	var txEnvelopeBytes []byte
	if txEnvelopeBytes, err = mgr.fetchRawBytes(lp); err != nil {
		return nil, err
	}
	_, n := proto.DecodeVarint(txEnvelopeBytes)
	return putil.GetEnvelopeFromBlock(txEnvelopeBytes[n:])
}

func (mgr *blockfileMgr) fetchBlockBytes(lp *fileLocPointer) ([]byte, error) {
	stream, err := newBlockfileStream(mgr.rootDir, lp.fileSuffixNum, int64(lp.offset))
	if err != nil {
		return nil, err
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (mgr *blockfileMgr) fetchRawBytes(lp *fileLocPointer) ([]byte, error) {
	filePath := deriveBlockfilePath(mgr.rootDir, lp.fileSuffixNum)
	reader, err := newBlockfileReader(filePath)
	if err != nil {
		return nil, err
	}
	defer reader.close()
	b, err := reader.read(lp.offset, lp.bytesLength)
	if err != nil {
		return nil, err
	}
	return b, nil
}

//Get the current checkpoint information that is stored in the database
func (mgr *blockfileMgr) loadCurrentInfo() (*checkpointInfo, error) {
	var b []byte
	var err error
	if b, err = mgr.db.Get(blkMgrInfoKey); b == nil || err != nil {
		return nil, err
	}
	i := &checkpointInfo{}
	if err = i.unmarshal(b); err != nil {
		return nil, err
	}
	logger.Debugf("loaded checkpointInfo:%s", i)
	return i, nil
}

func (mgr *blockfileMgr) saveCurrentInfo(i *checkpointInfo, sync bool) error {
	b, err := i.marshal()
	if err != nil {
		return err
	}
	if err = mgr.db.Put(blkMgrInfoKey, b, sync); err != nil {
		return err
	}
	return nil
}

// scanForLastCompleteBlock scan a given block file and detects the last offset in the file
// after which there may lie a block partially written (towards the end of the file in a crash scenario).
func scanForLastCompleteBlock(rootDir string, fileNum int, startingOffset int64) ([]byte, int64, int, error) {
	//scan the passed file number suffix starting from the passed offset to find the last completed block
	numBlocks := 0
	var lastBlockBytes []byte
	blockStream, errOpen := newBlockfileStream(rootDir, fileNum, startingOffset)
	if errOpen != nil {
		return nil, 0, 0, errOpen
	}
	defer blockStream.close()
	var errRead error
	var blockBytes []byte
	for {
		blockBytes, errRead = blockStream.nextBlockBytes()
		if blockBytes == nil || errRead != nil {
			break
		}
		lastBlockBytes = blockBytes
		numBlocks++
	}
	if errRead == ErrUnexpectedEndOfBlockfile {
		logger.Debugf(`Error:%s
		The error may happen if a crash has happened during block appending.
		Resetting error to nil and returning current offset as a last complete block's end offset`, errRead)
		errRead = nil
	}
	logger.Debugf("scanForLastCompleteBlock(): last complete block ends at offset=[%d]", blockStream.currentOffset)
	return lastBlockBytes, blockStream.currentOffset, numBlocks, errRead
}

// checkpointInfo
//检查点的作用是记录当前账本的保存状态，也就是说记录账本当前保存到哪里，保存到哪个block
type checkpointInfo struct {
	latestFileChunkSuffixNum int    //记录block所在的blockfile的文件名后缀
	latestFileChunksize      int    //记录当前blockfile最新的大小
	isChainEmpty             bool   //标识当前账本是否为空
	lastBlockNumber          uint64 //记录当前账本最新的block的序列号
}

func (i *checkpointInfo) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	var err error
	if err = buffer.EncodeVarint(uint64(i.latestFileChunkSuffixNum)); err != nil {
		return nil, err
	}
	if err = buffer.EncodeVarint(uint64(i.latestFileChunksize)); err != nil {
		return nil, err
	}
	if err = buffer.EncodeVarint(i.lastBlockNumber); err != nil {
		return nil, err
	}
	var chainEmptyMarker uint64
	if i.isChainEmpty {
		chainEmptyMarker = 1
	}
	if err = buffer.EncodeVarint(chainEmptyMarker); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (i *checkpointInfo) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	var val uint64
	var chainEmptyMarker uint64
	var err error

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileChunkSuffixNum = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileChunksize = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.lastBlockNumber = val
	if chainEmptyMarker, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.isChainEmpty = chainEmptyMarker == 1
	return nil
}

func (i *checkpointInfo) String() string {
	return fmt.Sprintf("latestFileChunkSuffixNum=[%d], latestFileChunksize=[%d], isChainEmpty=[%t], lastBlockNumber=[%d]",
		i.latestFileChunkSuffixNum, i.latestFileChunksize, i.isChainEmpty, i.lastBlockNumber)
}
