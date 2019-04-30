/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"github.com/hyperledger/fabric/common/channelconfig"
	cb "github.com/hyperledger/fabric/protos/common"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/op/go-logging"
)

const pkgLogID = "orderer/common/blockcutter"

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver. It
	// is useful for Kafka orderer to determine the `LastOffsetPersisted` of block.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope
}

//块分割工具
type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope//已有的消息//缓存交易列表
	pendingBatchSizeBytes uint32
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(sharedConfigFetcher OrdererConfigFetcher) Receiver {
	return &receiver{
		sharedConfigFetcher: sharedConfigFetcher,
	}
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
//这里的messageBatche相当于区块，messageBatches相当于区块集合
//按照交易处快规则切割成批量交易集合，同时提供Cut（）接口，不添加交易信息，直接切割当前的缓存交易信息列表构造成批量交易集合
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	//获取orderer的配置参数
	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}
	batchSize := ordererConfig.BatchSize()
	//当前消息的大小
	messageSizeBytes := messageSizeBytes(msg)
	//如果消息的大小大于PreferredMaxBytes （( 512 KB )指定了 一 条消息 最优的最大字节数），立即cut
	//检查当前的消息的字节数是否超过推荐的消息的最大字节数
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		//如果存在缓存交易消息列表，则切割出批量交易集合
		if len(r.pendingBatch) > 0 {
			//立即cut
			//切割批量交易集合
			messageBatch := r.Cut()
			//将消息添加到消息集合中
			//添加到messageBatches列表
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		//创建一个新的batch，用单独的msg
		//将msg构造为单独的批量交易集合，并添加到messageBatch列表中
		//这种情况会形成两个批量交易集合
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		//返回消息处理循环
		return
	}

	//若 一 个 Envelope 的大小加上 blockcutter
	//已有的消息的大小之和大于 PreferredMaxBytes ,要立即 Cut
	//如果添加msg消息后的消息长度超过推荐的消息最大字节数，则先清空当前缓存交易消息列表
	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	//如果消息上溢
	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		//切割批量交易集合
		messageBatch := r.Cut()
		//添加到messageBatches列表
		//这种情况胡形成第一个批量交易集合
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	//将消息msg添加到缓存交易消息列表
	r.pendingBatch = append(r.pendingBatch, msg)
	//调整缓存交易消息列表的消息字节数
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	//检查调整后的缓存交易消息列表的消息个数是否超过了预设的最大消息数
	//判断缓存交易消息列表中的消息个数是否大于Orderer配置的最大的消息数，若是，则立即cut
	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		//切割批量交易集合
		messageBatch := r.Cut()
		//添加到批量交易集合列表
		//形成第二个批量交易集合，否则只会形成第一个批量交易集合
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
//改变当前的revceiver的值
func (r *receiver) Cut() []*cb.Envelope {
	//将缓存的交易消息列表复制到批量交易集合batch中
	batch := r.pendingBatch
	//将缓存交易信息列表pendingBatch设置为nil
	r.pendingBatch = nil
	//将缓存交易消息列表字节数清零
	r.pendingBatchSizeBytes = 0
	return batch
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
