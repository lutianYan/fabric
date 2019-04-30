/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/policies"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

// StandardChannelSupport includes the resources needed for the StandardChannel processor.
type StandardChannelSupport interface {
	// Sequence should return the current configSeq
	Sequence() uint64

	// ChainID returns the ChannelID
	ChainID() string

	// Signer returns the signer for this orderer
	Signer() crypto.LocalSigner

	// ProposeConfigUpdate takes in an Envelope of type CONFIG_UPDATE and produces a
	// ConfigEnvelope to be used as the Envelope Payload Data of a CONFIG message
	ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error)
}

// StandardChannel implements the Processor interface for standard extant channels
type StandardChannel struct {
	support StandardChannelSupport
	filters *RuleSet
}

// NewStandardChannel creates a new standard message processor
func NewStandardChannel(support StandardChannelSupport, filters *RuleSet) *StandardChannel {
	return &StandardChannel{
		filters: filters,
		support: support,
	}
}

// CreateStandardChannelFilters creates the set of filters for a normal (non-system) chain
func CreateStandardChannelFilters(filterSupport channelconfig.Resources) *RuleSet {
	ordererConfig, ok := filterSupport.OrdererConfig()
	if !ok {
		logger.Panicf("Missing orderer config")
	}
	return NewRuleSet([]Rule{
		EmptyRejectRule,
		NewExpirationRejectRule(filterSupport),
		NewSizeFilter(ordererConfig),
		NewSigFilter(policies.ChannelWriters, filterSupport),
	})
}

// ClassifyMsg inspects the message to determine which type of processing is necessary
func (s *StandardChannel) ClassifyMsg(chdr *cb.ChannelHeader) Classification {
	switch chdr.Type {
	case int32(cb.HeaderType_CONFIG_UPDATE):
		return ConfigUpdateMsg
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		// In order to maintain backwards compatibility, we must classify these messages
		return ConfigMsg
	case int32(cb.HeaderType_CONFIG):
		// In order to maintain backwards compatibility, we must classify these messages
		return ConfigMsg
	default:
		return NormalMsg
	}
}

// ProcessNormalMsg will check the validity of a message based on the current configuration.  It returns the current
// configuration sequence number and nil on success, or an error if the message is not valid
//利用标准通道消息处理器（实际上是该链支持对象的消息处理器）处理普通交易消息
func (s *StandardChannel) ProcessNormalMsg(env *cb.Envelope) (configSeq uint64, err error) {
	//获取该通道的最新配置序号，默认初始值为0，创建新的应用通道后该配置序号自增为1
	//该配置序号可用于标志通道配置信息的版本，通过比较该序号就能判断当前通道配置版本是否未发生了更新，从而确定当前交易信息是否需要重新过滤与重新排序
	configSeq = s.support.Sequence()
	//利用自带的4个默认通道消息过滤器过滤该消息，来检查是否满足应用通道上的消息处理要求，（不能为空过滤器，拒绝过期的签名者身份证书的过滤器，最大字节数过滤器，信息签名一直过滤器）
	err = s.filters.Apply(env)
	return
}

// ProcessConfigUpdateMsg will attempt to apply the config impetus msg to the current configuration, and if successful
// return the resulting config message and the configSeq the config was computed from.  If the config impetus message
// is invalid, an error is returned.
//更新通道配置
func (s *StandardChannel) ProcessConfigUpdateMsg(env *cb.Envelope) (config *cb.Envelope, configSeq uint64, err error) {
	logger.Debugf("Processing config update message for channel %s", s.support.ChainID())

	// Call Sequence first.  If seq advances between proposal and acceptance, this is okay, and will cause reprocessing
	// however, if Sequence is called last, then a success could be falsely attributed to a newer configSeq
	//获取当前通道的配置序号
	seq := s.support.Sequence()
	//过滤消息
	err = s.filters.Apply(env)
	if err != nil {
		return nil, 0, err
	}

	//利用通道的配置交易管理器基于当前的配置交易消息创建新的通道配置更新交易消息configEnvelope，注意配置序号增1，验证该配置交易消息的签名集合是否满足更新配置项的修改策略
	configEnvelope, err := s.support.ProposeConfigUpdate(env)
	if err != nil {
		return nil, 0, err
	}

	//创建新的通道配置消息
	//将configEnvelope消息封装为新的通道配置交易消息
	config, err = utils.CreateSignedEnvelope(cb.HeaderType_CONFIG, s.support.ChainID(), s.support.Signer(), configEnvelope, msgVersion, epoch)
	if err != nil {
		return nil, 0, err
	}

	// We re-apply the filters here, especially for the size filter, to ensure that the transaction we
	// just constructed is not too large for our consenter.  It additionally reapplies the signature
	// check, which although not strictly necessary, is a good sanity check, in case the orderer
	// has not been configured with the right cert material.  The additional overhead of the signature
	// check is negligable, as this is the reconfig path and not the normal path.
	//过滤消息，以确保交易消息处理的合法性
	err = s.filters.Apply(config)
	if err != nil {
		return nil, 0, err
	}

	//通道配置交易消息，当前通道的配置序号
	return config, seq, nil
}

// ProcessConfigMsg takes an envelope of type `HeaderType_CONFIG`, unpacks the `ConfigEnvelope` from it
// extracts the `ConfigUpdate` from `LastUpdate` field, and calls `ProcessConfigUpdateMsg` on it.
func (s *StandardChannel) ProcessConfigMsg(env *cb.Envelope) (config *cb.Envelope, configSeq uint64, err error) {
	logger.Debugf("Processing config message for channel %s", s.support.ChainID())

	configEnvelope := &cb.ConfigEnvelope{}
	_, err = utils.UnmarshalEnvelopeOfType(env, cb.HeaderType_CONFIG, configEnvelope)
	if err != nil {
		return
	}

	return s.ProcessConfigUpdateMsg(configEnvelope.LastUpdate)
}
