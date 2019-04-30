/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/orderer/common/blockcutter"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/pkg/errors"
)

// ChainSupport holds the resources for a particular channel.
type ChainSupport struct {
	*ledgerResources //账本资源对象
	msgprocessor.Processor //消息处理器
	*BlockWriter //区块写组件
	consensus.Chain //共识组件链对象
	cutter blockcutter.Receiver //消息切割组件
	crypto.LocalSigner //本地签名者
}

func newChainSupport(
	registrar *Registrar,
	ledgerResources *ledgerResources,
	consenters map[string]consensus.Consenter,
	signer crypto.LocalSigner,
) *ChainSupport {
	// Read in the last block and metadata for the channel
	//获取通道上账本的最新区块
	lastBlock := blockledger.GetBlock(ledgerResources, ledgerResources.Height()-1)

	//从最新区块中获取Orderer元数据索引项
	metadata, err := utils.GetMetadataFromBlock(lastBlock, cb.BlockMetadataIndex_ORDERER)
	// Assuming a block created with cb.NewBlock(), this should not
	// error even if the orderer metadata is an empty byte slice
	if err != nil {
		logger.Fatalf("[channel: %s] Error extracting orderer metadata: %s", ledgerResources.ConfigtxValidator().ChainID(), err)
	}

	// Construct limited support needed as a parameter for additional support
	//构造指定通道的链支持对象
	cs := &ChainSupport{
		ledgerResources: ledgerResources, //区块账本资源对象
		LocalSigner:     signer, //本地签名者
		cutter:          blockcutter.NewReceiverImpl(ledgerResources), //消息切割组件
	}

	// Set up the msgprocessor
	//设置标准的通道消息处理器
	cs.Processor = msgprocessor.NewStandardChannel(cs, msgprocessor.CreateStandardChannelFilters(cs))

	// Set up the block writer
	//将区块写入组件
	cs.BlockWriter = newBlockWriter(lastBlock, registrar, cs)

	// Set up the consenter
	//获取共识组件类型
	consenterType := ledgerResources.SharedConfig().ConsensusType()
	//获取共识组件对象
	consenter, ok := consenters[consenterType]
	if !ok {
		logger.Panicf("Error retrieving consenter of type: %s", consenterType)
	}

	//注意链支持对象cs实现了ConsenterSupport接口，以支持Solo与Kafka共识组件链对象
	//Solo共识组件只使用了cs参数，kafka共识组件则使用了两个参数
	//创建新的共识组件链对象
	cs.Chain, err = consenter.HandleChain(cs, metadata)
	if err != nil {
		logger.Panicf("[channel: %s] Error creating consenter: %s", cs.ChainID(), err)
	}

	logger.Debugf("[channel: %s] Done creating channel support resources", cs.ChainID())

	return cs
}

func (cs *ChainSupport) Reader() blockledger.Reader {
	return cs
}

// Signer returns the crypto.Localsigner for this channel.
func (cs *ChainSupport) Signer() crypto.LocalSigner {
	return cs
}

func (cs *ChainSupport) start() {
	cs.Chain.Start()
}

// BlockCutter returns the blockcutter.Receiver instance for this channel.
func (cs *ChainSupport) BlockCutter() blockcutter.Receiver {
	return cs.cutter
}

// Validate passes through to the underlying configtx.Validator
func (cs *ChainSupport) Validate(configEnv *cb.ConfigEnvelope) error {
	return cs.ConfigtxValidator().Validate(configEnv)
}

// ProposeConfigUpdate passes through to the underlying configtx.Validator
func (cs *ChainSupport) ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error) {
	//创建新的通道配置更新消息
	env, err := cs.ConfigtxValidator().ProposeConfigUpdate(configtx)
	if err != nil {
		return nil, err
	}

	//创建新的通道配置实体对象
	bundle, err := cs.CreateBundle(cs.ChainID(), env.Config)
	if err != nil {
		return nil, err
	}

	//检查该对象的兼容性与合法性，包括Orderer、Application与COnsortiums配置项中 共识类型、匹配MSPID以及组织名称等
	if err = checkResources(bundle); err != nil {
		return nil, errors.Wrap(err, "config update is not compatible")
	}

	return env, cs.ValidateNew(bundle)
}

// ChainID passes through to the underlying configtx.Validator
func (cs *ChainSupport) ChainID() string {
	return cs.ConfigtxValidator().ChainID()
}

// ConfigProto passes through to the underlying configtx.Validator
func (cs *ChainSupport) ConfigProto() *cb.Config {
	return cs.ConfigtxValidator().ConfigProto()
}

// Sequence passes through to the underlying configtx.Validator
func (cs *ChainSupport) Sequence() uint64 {
	return cs.ConfigtxValidator().Sequence()
}
