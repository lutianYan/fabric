/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package multichannel tracks the channel resources for the orderer.  It initially
// loads the set of existing channels, and provides an interface for users of these
// channels to retrieve them, or create new ones.
package multichannel

import (
	"fmt"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

const (
	pkgLogID = "orderer/commmon/multichannel"

	msgVersion = int32(0)
	epoch      = 0
)

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

// checkResources makes sure that the channel config is compatible with this binary and logs sanity checks
func checkResources(res channelconfig.Resources) error {
	channelconfig.LogSanityChecks(res)
	oc, ok := res.OrdererConfig()
	if !ok {
		return errors.New("config does not contain orderer config")
	}
	if err := oc.Capabilities().Supported(); err != nil {
		return errors.Wrapf(err, "config requires unsupported orderer capabilities: %s", err)
	}
	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		return errors.Wrapf(err, "config requires unsupported channel capabilities: %s", err)
	}
	return nil
}

// checkResourcesOrPanic invokes checkResources and panics if an error is returned
func checkResourcesOrPanic(res channelconfig.Resources) {
	if err := checkResources(res); err != nil {
		logger.Panicf("[channel %s] %s", res.ConfigtxValidator().ChainID(), err)
	}
}

type mutableResources interface {
	channelconfig.Resources
	Update(*channelconfig.Bundle)
}

type configResources struct {
	mutableResources
}

func (cr *configResources) CreateBundle(channelID string, config *cb.Config) (*channelconfig.Bundle, error) {
	return channelconfig.NewBundle(channelID, config)
}

func (cr *configResources) Update(bndl *channelconfig.Bundle) {
	checkResourcesOrPanic(bndl)
	cr.mutableResources.Update(bndl)
}
//获取共识组件类型、交易处快周期时间、区块最大字节数、通道限制参数（如通道数量等）
func (cr *configResources) SharedConfig() channelconfig.Orderer {
	oc, ok := cr.OrdererConfig()
	if !ok {
		logger.Panicf("[channel %s] has no orderer configuration", cr.ConfigtxValidator().ChainID())
	}
	return oc
}

type ledgerResources struct {
	*configResources
	blockledger.ReadWriter
}

// Registrar serves as a point of access and control for the individual channel resources.
type Registrar struct {
	chains          map[string]*ChainSupport //链支持对象字典
	consenters      map[string]consensus.Consenter //共识组件字典
	ledgerFactory   blockledger.Factory //账本工厂对象组件
	signer          crypto.LocalSigner //本地签名者ITIS
	systemChannelID string //系统通道ID
	systemChannel   *ChainSupport //系统通道链支持对象
	templator       msgprocessor.ChannelConfigTemplator //通道配置模板，用于生成消息处理器
	callbacks       []func(bundle *channelconfig.Bundle) //TLS认证链接回调函数列表
}

func getConfigTx(reader blockledger.Reader) *cb.Envelope {
	//获取当前账本上的最新区块（区块号为区块账本的高度-1）
	lastBlock := blockledger.GetBlock(reader, reader.Height()-1)
	//获取最新区块上元数据中的BlockMetadataIndex_LAST_CONFIG索引项，解析获得最新配置区块的索引区块号index
	index, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		logger.Panicf("Chain did not have appropriately encoded last config in its latest block: %s", err)
	}
	//从区块文件中获取指定区块号index对应的最新配置区块对象
	configBlock := blockledger.GetBlock(reader, index)
	if configBlock == nil {
		logger.Panicf("Config block does not exist")
	}
	//从该配置区块的交易集合中解析获取第一个交易对象（实际上配置区块只有一个配置交易），作为该通道的最新配置交易对象返回到configTX变量
	return utils.ExtractEnvelopeOrPanic(configBlock, 0)
}

// NewRegistrar produces an instance of a *Registrar.
//创建Orderer节点上的多通道注册管理器对象。同时，创建系统通道与现存应用通道的链支持对象（chainSupport），将这些通道都注册到多通道注册管理器上，
// 并调用chain.start()方法，一次启动每一个应用通道关联的链支持对象，最后启动系统通道的
//链支持对象，实际上都是启动的共识组件链对象
//实现多通道管理机制，支持多个通道及其链上的数据相互隔离，确保只有同意个通道内的Peer才能接受该通道上的账本数据，切不允许其他通扫上的节点或外部非法节点接受与访问本通道数据，从而报数通道上的数据隐私
//可用于创建多通道注册管理器对象
func NewRegistrar(ledgerFactory blockledger.Factory, consenters map[string]consensus.Consenter,
	signer crypto.LocalSigner, callbacks ...func(bundle *channelconfig.Bundle)) *Registrar {
	r := &Registrar{
		chains:        make(map[string]*ChainSupport), //链支持对象字典
		ledgerFactory: ledgerFactory, //账本工厂对象
		consenters:    consenters, //共识组件字典
		signer:        signer, //本地签名者
		callbacks:     callbacks, //回调函数（比如TLS认证链接毁掉函数）
	}

	//获取该账本工厂对象关联的现存通道ID列表
	existingChains := ledgerFactory.ChainIDs()
	//遍历现存通道Id列表(如果是新建的orderer排序节点刚刚启动，则existingChains列表只会包含系统通道ID，testchainid，，如果是Orderer节点崩溃后重启，则existingChains列表还可能包含其他的已经成功创建的应用通道ID列表)
	for _, chainID := range existingChains {
		//根据通道ID获取或者新建指定通道上的区块账本对象
		rl, err := ledgerFactory.GetOrCreate(chainID)
		if err != nil {
			logger.Panicf("Ledger factory reported chainID %s but could not retrieve it: %s", chainID, err)
		}
		//获取该通道账本上最新的配置交易对象
		configTx := getConfigTx(rl)
		if configTx == nil {
			logger.Panic("Programming error, configTx should never be nil here")
		}
		//创建新的账本资源对象
		ledgerResources := r.newLedgerResources(configTx)
		//重新获取链ID
		chainID := ledgerResources.ConfigtxValidator().ChainID()

		//如果存在Consortiums配置，则说明是系统通道
		if _, ok := ledgerResources.ConsortiumsConfig(); ok {
			//如果已经设置系统通道名称，则说明已经创建了系统通道和他的名称
			if r.systemChannelID != "" {
				logger.Panicf("There appear to be two system chains %s and %s", r.systemChannelID, chainID)
			}
			//构造该通道的链支持对象
			chain := newChainSupport(
				r, //多通道管理器
				ledgerResources, //账本资源对象
				consenters, //共识组件字典
				signer) //签名者实体
			//创建默认通道配置模板
			r.templator = msgprocessor.NewDefaultTemplator(chain)
			//创建系统通道消息处理器
			chain.Processor = msgprocessor.NewSystemChannel(chain, r.templator, msgprocessor.CreateSystemChannelFilters(r, chain))

			// Retrieve genesis block to log its hash. See FAB-5450 for the purpose
			//将账本的区块迭代器指针设置为最旧的区块位置
			iter, pos := rl.Iterator(&ab.SeekPosition{Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}}})
			defer iter.Close()
			//检查区块号，如果不是0，则报错
			if pos != uint64(0) {
				logger.Panicf("Error iterating over system channel: '%s', expected position 0, got %d", chainID, pos)
			}
			//获取创世区块并在日志中记录其hash值
			genesisBlock, status := iter.Next()
			if status != cb.Status_SUCCESS {
				logger.Panicf("Error reading genesis block of system channel '%s'", chainID)
			}
			logger.Infof("Starting system channel '%s' with genesis block hash %x and orderer type %s", chainID, genesisBlock.Header.Hash(), chain.SharedConfig().ConsensusType())

			//将系统通道注册到多通道管理器中，这里的代码i执行一次
			r.chains[chainID] = chain //注册到链支持对象字典中
			r.systemChannelID = chainID //设置系统通道ID(默认是testchainid)
			r.systemChannel = chain //设置系统通道的链支持对象
			// We delay starting this chain, as it might try to copy and replace the chains map via newChain before the map is fully built
			//为了保证不出错，在函数退出的时候最后启动系统通道的链支持对象
			//以确保在其他应用通道都初始化完毕后启动共识组件链对象之后，在启动系统通道的共识组件链对象
			defer chain.start()
			//orderer节点上的系统通道创建完毕
		} else {
			//如果不存在联盟配置，则属于应用通道，直接创建链支持对象，注册到多通道注册管理器中
			logger.Debugf("Starting chain: %s", chainID)
			//构造应用通道的链支持对象
			chain := newChainSupport(
				r,
				ledgerResources,
				consenters,
				signer)
			//设置应用通道的链支持对象
			r.chains[chainID] = chain
			// 启动链支持对象
			chain.start()
		}

	}

	// 如果检测系统通道ID为空，则说明还没有设置系统通道
	if r.systemChannelID == "" {
		logger.Panicf("No system chain found.  If bootstrapping, does your system channel contain a consortiums group definition?")
	}

	return r
}

// SystemChannelID returns the ChannelID for the system channel.
func (r *Registrar) SystemChannelID() string {
	return r.systemChannelID
}

// BroadcastChannelSupport returns the message channel header, whether the message is a config update
// and the channel resources for a message or an error if the message is not a message which can
// be processed directly (like CONFIG and ORDERER_TRANSACTION messages)
func (r *Registrar) BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, *ChainSupport, error) {
	//从交易消息中解析出消息的通道头部chdr（channelHeader）
	chdr, err := utils.ChannelHeader(msg)
	if err != nil {
		return nil, false, nil, fmt.Errorf("could not determine channel ID: %s", err)
	}

	//从多通道的注册管理器的chains字典中获取关联通道上的链支持对象cs
	cs, ok := r.chains[chdr.ChannelId]
	//如果chains中已经存在指定通道上的链支持对象cs，则说明该消息是普通交易消息或更新通道配置的配置交易消息，此时返回对应通道的链支持对象
	//否则，多通道注册管理器上还没有注册该通道的链支持对象，说明还没有创建该通道，此时该消息是用于创建新应用通道的配置交易消息，因此返回系统通道的链支持对象，用于创建新的应用通道
	if !ok {
		cs = r.systemChannel
	}

	isConfig := false
	//检查消息的通道头部类型
	//如果是通道配置交易消息类型，则设置配置交易消息标志位为true，否则设置为false，以表示区分普通交易消息和配置交易消息
	switch cs.ClassifyMsg(chdr) {
	case msgprocessor.ConfigUpdateMsg:
		isConfig = true
	case msgprocessor.ConfigMsg:
		return chdr, false, nil, errors.New("message is of type that cannot be processed directly")
	default:
	}

	return chdr, isConfig, cs, nil
}

// GetChain retrieves the chain support for a chain (and whether it exists)
func (r *Registrar) GetChain(chainID string) (*ChainSupport, bool) {
	cs, ok := r.chains[chainID]
	return cs, ok
}
//基于指定的通道的交易配置UI想configTx创建账本资源对象，封装了通道配置资源对象和区块账本对象，分别用于管理通道的配置信息与区块账本
func (r *Registrar) newLedgerResources(configTx *cb.Envelope) *ledgerResources {
	payload, err := utils.UnmarshalPayload(configTx.Payload)
	if err != nil {
		logger.Panicf("Error umarshaling envelope to payload: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Missing channel header: %s", err)
	}

	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Error unmarshaling channel header: %s", err)
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		logger.Panicf("Error umarshaling config envelope from payload data: %s", err)
	}

	bundle, err := channelconfig.NewBundle(chdr.ChannelId, configEnvelope.Config)
	if err != nil {
		logger.Panicf("Error creating channelconfig bundle: %s", err)
	}

	checkResourcesOrPanic(bundle)

	ledger, err := r.ledgerFactory.GetOrCreate(chdr.ChannelId)
	if err != nil {
		logger.Panicf("Error getting ledger for %s", chdr.ChannelId)
	}

	return &ledgerResources{
		configResources: &configResources{
			mutableResources: channelconfig.NewBundleSource(bundle, r.callbacks...),
		},
		ReadWriter: ledger,
	}
}

//创建应用通道
func (r *Registrar) newChain(configtx *cb.Envelope) {
	//基于给定的配置交易消息创建新的账本资源对象
	//封装了通道配置实体对象（Bundle类型）及通道的区块账本对象（FileLedger类型），分别管理通道配置与区块账本
	ledgerResources := r.newLedgerResources(configtx)
	//添加创世区块（将配置交易消息构造成配置区块，也就是当前新应用通道的创世区块）
	ledgerResources.Append(blockledger.CreateNextBlock(ledgerResources, []*cb.Envelope{configtx}))

	// Copy the map to allow concurrent reads from broadcast/deliver while the new chainSupport is
	//复制当前Orderer上多通道注册管理器的chains链支持对象字典，允许同时提供Broadcast/Deliver服务
	newChains := make(map[string]*ChainSupport)
	for key, value := range r.chains {
		//复制链支持对象字典到临时字典中
		newChains[key] = value
	}

	//创建新的链支持对象
	cs := newChainSupport(r, ledgerResources, r.consenters, r.signer)
	//重新获取通道ID
	chainID := ledgerResources.ConfigtxValidator().ChainID()

	logger.Infof("Created and starting new chain %s", chainID)

	//设置指定通道及其链支持对象
	newChains[string(chainID)] = cs
	//启动链支持对象，实际启动共识组件链对象
	cs.start()

	//更新多通道注册管理器上的链支持对象字典chains
	r.chains = newChains
}

// ChannelsCount returns the count of the current total number of channels.
func (r *Registrar) ChannelsCount() int {
	return len(r.chains)
}

// NewChannelConfig produces a new template channel configuration based on the system channel's current config.
func (r *Registrar) NewChannelConfig(envConfigUpdate *cb.Envelope) (channelconfig.Resources, error) {
	return r.templator.NewChannelConfig(envConfigUpdate)
}

// CreateBundle calls channelconfig.NewBundle
func (r *Registrar) CreateBundle(channelID string, config *cb.Config) (channelconfig.Resources, error) {
	return channelconfig.NewBundle(channelID, config)
}
