/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"sync"

	newchannelconfig "github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/util"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/golang/protobuf/proto"
)

type blockWriterSupport interface {
	crypto.LocalSigner
	blockledger.ReadWriter
	configtx.Validator
	Update(*newchannelconfig.Bundle)
	CreateBundle(channelID string, config *cb.Config) (*newchannelconfig.Bundle, error)
}

// BlockWriter efficiently writes the blockchain to disk.
// To safely use BlockWriter, only one thread should interact with it.
// BlockWriter will spawn additional committing go routines and handle locking
// so that these other go routines safely interact with the calling one.
type BlockWriter struct {
	support            blockWriterSupport
	registrar          *Registrar
	lastConfigBlockNum uint64
	lastConfigSeq      uint64
	lastBlock          *cb.Block
	committingBlock    sync.Mutex
}

func newBlockWriter(lastBlock *cb.Block, r *Registrar, support blockWriterSupport) *BlockWriter {
	bw := &BlockWriter{
		support:       support,
		lastConfigSeq: support.Sequence(),
		lastBlock:     lastBlock,
		registrar:     r,
	}

	// If this is the genesis block, the lastconfig field may be empty, and, the last config block is necessarily block 0
	// so no need to initialize lastConfig
	if lastBlock.Header.Number != 0 {
		var err error
		bw.lastConfigBlockNum, err = utils.GetLastConfigIndexFromBlock(lastBlock)
		if err != nil {
			logger.Panicf("[channel: %s] Error extracting last config block from block metadata: %s", support.ChainID(), err)
		}
	}

	logger.Debugf("[channel: %s] Creating block writer for tip of chain (blockNumber=%d, lastConfigBlockNum=%d, lastConfigSeq=%d)", support.ChainID(), lastBlock.Header.Number, bw.lastConfigBlockNum, bw.lastConfigSeq)
	return bw
}

// CreateNextBlock creates a new block with the next block number, and the given contents.
//创建一个新的区块
func (bw *BlockWriter) CreateNextBlock(messages []*cb.Envelope) *cb.Block {
	//获取当前最新区块头部的hash值
	previousBlockHash := bw.lastBlock.Header.Hash()

	data := &cb.BlockData{
		Data: make([][]byte, len(messages)),
	}

	var err error
	//遍历批量交易集合并序列化封装到BlockData结构中
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		if err != nil {
			logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	//创建新区块（最新区块号增1）并设置区块hash值和交易集合数据
	block := cb.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	//设置区块hash值
	block.Header.DataHash = data.Hash()
	//设置交易集合数据
	block.Data = data

	return block
}

// WriteConfigBlock should be invoked for blocks which contain a config transaction.
// This call will block until the new config has taken effect, then will return
// while the block is written asynchronously to disk.
func (bw *BlockWriter) WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte) {
	//从配置区块中提取第一个交易消息
	ctx, err := utils.ExtractEnvelope(block, 0)
	if err != nil {
		logger.Panicf("Told to write a config block, but could not get configtx: %s", err)
	}

	payload, err := utils.UnmarshalPayload(ctx.Payload)
	if err != nil {
		logger.Panicf("Told to write a config block, but configtx payload is invalid: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Told to write a config block, but configtx payload header is missing")
	}

	//解析消息通道头部
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Told to write a config block with an invalid channel header: %s", err)
	}

	switch chdr.Type {
	//用于创建新的应用通道
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		//解析通道配置交易消息
		newChannelConfig, err := utils.UnmarshalEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config update embedded: %s", err)
		}
		//创建新的应用通道
		bw.registrar.newChain(newChannelConfig)
	//用于更新通道配置
	case int32(cb.HeaderType_CONFIG):
		//从消息负载数据payload.data中解析出通道配置更新消息
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config envelope encoded: %s", err)
		}

		//验证通道配置交易消息的合法性
		err = bw.support.Validate(configEnvelope)
		if err != nil {
			logger.Panicf("Told to write a config block with new config, but could not apply it: %s", err)
		}

		//基于新的通道配置交易消息创建通道配置实体对象
		bundle, err := bw.support.CreateBundle(chdr.ChannelId, configEnvelope.Config)
		if err != nil {
			logger.Panicf("Told to write a config block with a new config, but could not convert it to a bundle: %s", err)
		}

		//更新通道上链支持对象的通道配置实体，不需要直接修改多通道注册管理器上的链支持对象字典
		bw.support.Update(bundle)
	default:
		logger.Panicf("Told to write a config block with unknown header type: %v", chdr.Type)
	}

	//将区块写入账本
	bw.WriteBlock(block, encodedMetadataValue)
}

// WriteBlock should be invoked for blocks which contain normal transactions.
// It sets the target block as the pending next block, and returns before it is committed.
// Before returning, it acquires the committing lock, and spawns a go routine which will
// annotate the block with metadata and signatures, and write the block to the ledger
// then release the lock.  This allows the calling thread to begin assembling the next block
// before the commit phase is complete.
//在返回之前，它获取提交锁定，并生成一个go例程，该例程将使用元数据和签名对块进行注释，然后将块写入分类帐，然后释放锁定。
//这允许调用线程在提交阶段完成之前开始组装下一个块。
//有一个同步机制
func (bw *BlockWriter) WriteBlock(block *cb.Block, encodedMetadataValue []byte) {
	bw.committingBlock.Lock()
	//更新最新的区块
	bw.lastBlock = block

	go func() {
		defer bw.committingBlock.Unlock()
		//提交区块到区块账本中
		bw.commitBlock(encodedMetadataValue)
	}()
}

// commitBlock should only ever be invoked with the bw.committingBlock held
// this ensures that the encoded config sequence numbers stay in sync
//提交区块到区块账本中（可以用于更新区块元数据）
func (bw *BlockWriter) commitBlock(encodedMetadataValue []byte) {
	// Set the orderer-related metadata field
	//更新Orderer相关的元数据
	if encodedMetadataValue != nil {
		bw.lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER] = utils.MarshalOrPanic(&cb.Metadata{Value: encodedMetadataValue})
	}
	//添加区块元数据中的签名
	bw.addBlockSignature(bw.lastBlock)
	//添加区块元数据中的最新配置区块和签名
	bw.addLastConfigSignature(bw.lastBlock)

	//添加新的区块
	//将新区块添加到当前的通道账本的区块数据文件中，建立并更新区块索引信息
	err := bw.support.Append(bw.lastBlock)
	if err != nil {
		logger.Panicf("[channel: %s] Could not append block: %s", bw.support.ChainID(), err)
	}
	logger.Debugf("[channel: %s] Wrote block %d", bw.support.ChainID(), bw.lastBlock.GetHeader().Number)
}

//封装了对签名头部（含有签名者身份信息与消息随机数Nonce）与区块头部对的组合信息签名
func (bw *BlockWriter) addBlockSignature(block *cb.Block) {
	blockSignature := &cb.MetadataSignature{
		SignatureHeader: utils.MarshalOrPanic(utils.NewSignatureHeaderOrPanic(bw.support)),
	}

	// Note, this value is intentionally nil, as this metadata is only about the signature, there is no additional metadata
	// information required beyond the fact that the metadata item is signed.
	blockSignatureValue := []byte(nil)

	blockSignature.Signature = utils.SignOrPanic(bw.support, util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, block.Header.Bytes()))

	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = utils.MarshalOrPanic(&cb.Metadata{
		Value: blockSignatureValue,
		Signatures: []*cb.MetadataSignature{
			blockSignature,
		},
	})
}

//封装了当前最新配置值lastConfigValue（记录最新配置区块的区块号）和组合信息（包含lastConfigValue、签名头部与区块头部）的签名
//同时，该方法还获取了当前通道的最新配置序号configSeq
func (bw *BlockWriter) addLastConfigSignature(block *cb.Block) {
	configSeq := bw.support.Sequence()
	//如果区块写组件上最新配置区块的区块号小鱼configSeq，则说明当前通道配置发生了更新
	//因此，将刽写组件上最新的配置区块的区块号更新为最新区块号，将最新的通道配置序号更新为configSeq
	if configSeq > bw.lastConfigSeq {
		logger.Debugf("[channel: %s] Detected lastConfigSeq transitioning from %d to %d, setting lastConfigBlockNum from %d to %d", bw.support.ChainID(), bw.lastConfigSeq, configSeq, bw.lastConfigBlockNum, block.Header.Number)
		bw.lastConfigBlockNum = block.Header.Number
		bw.lastConfigSeq = configSeq
	}

	lastConfigSignature := &cb.MetadataSignature{
		SignatureHeader: utils.MarshalOrPanic(utils.NewSignatureHeaderOrPanic(bw.support)),
	}

	lastConfigValue := utils.MarshalOrPanic(&cb.LastConfig{Index: bw.lastConfigBlockNum})
	logger.Debugf("[channel: %s] About to write block, setting its LAST_CONFIG to %d", bw.support.ChainID(), bw.lastConfigBlockNum)

	lastConfigSignature.Signature = utils.SignOrPanic(bw.support, util.ConcatenateBytes(lastConfigValue, lastConfigSignature.SignatureHeader, block.Header.Bytes()))

	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = utils.MarshalOrPanic(&cb.Metadata{
		Value: lastConfigValue,
		Signatures: []*cb.MetadataSignature{
			lastConfigSignature,
		},
	})
}
