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

package solo

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
)

const pkgLogID = "orderer/consensus/solo"

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

type consenter struct{}

type chain struct {
	support  consensus.ConsenterSupport //共识组件支持对象（链支持对象cs）
	sendChan chan *message  //用于传递和排序交易，只存在一个单独的交易消息通道（chan*message类型，阻塞接受一个消息），并按照FIFO原则接收和排序
	exitChan chan struct{} //用于接受退出消息，结束循环退出消息处理循环
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	return &consenter{}
}

func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support), nil
}

//创建solo共识组件链对象（chain类型）
func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support:  support, //共识组件支持对象（链支持对象cs）
		sendChan: make(chan *message), //用于传递和排序交易
		exitChan: make(chan struct{}), //用于接受退出消息
	}
}

//新通道创建和恢复现存的通道时会继续调用，启动通道上的共识组件链对象，创建消息处理循环，等待从共识排序后端接收已经排序的交易消息
func (ch *chain) Start() {
	//利用goroutine启动指定通道上的共识组件链对象，建立消息处理循环，等待接受排序后的交易消息并处理
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
//构造新的普通交易消息与，封装了当前的通道配置序号与过滤后的合法原始消息，并提交给共识排序后端请求排序
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {
	select {
	//重新构造新的普通交易消息，并发送到sendChain通道
	case ch.sendChan <- &message{
		configSeq: configSeq, //通道的最新配置序号
		normalMsg: env, //普通交易消息
	}:
		return nil
	case <-ch.exitChan: //检查通道，退出消息
		return fmt.Errorf("Exiting")
	}
}

// Configure accepts configuration update messages for ordering
//配置交易消息
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	//重新构造消息，并发送到sendChain通道上
	case ch.sendChan <- &message{
		configSeq: configSeq, //通道的最新配置序号
		configMsg: config, //通道配置交易消息
	}:
		return nil
	case <-ch.exitChan:    //检查退出消息
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

//可创建消息处理循环，阻塞并等待sendChan通道中的消息，检查是否存在Broadcast服务处理句柄过滤转发的消息
//所以Solo共识组件链对象只存在一个单独的交易消息通道按照FIFO原则接受和处理次奥西
//sendChan通道负责对交易进行排序，实际上只能处理一个消息，因此，只能用于测试环境而不适用于生产环境
func (ch *chain) main() {
	//声明一个只能接收的通道，通道元素类型time.Time
	//按照出块时间配置（2s）周期性的触发定时器，调用ch.support.BlockCutter().Cut(),将当前的缓存交易消息列表切割成一个批量交易集合batch，再调用createNextBlock和WriteBlock方法，对batch都早新区块，并写入通道账本上的区块数据文件中。
	//这样做是为了防止消息较少二可能无法达到其他出块规则要求，造成通道账本上长时间不能形成区块数据，使得无法正常查询区块数据
	//因此设置了定时打包出块的消息机制
	var timer <-chan time.Time
	var err error

	for {
		//获取当前channel的最新配置序号，阻塞等待消息
		seq := ch.support.Sequence()
		err = nil
		select {
		//检查sendChain通道 消息
		case msg := <-ch.sendChan:
			if msg.configMsg == nil {
				// NormalMsg
				//普通交易消息

				//判断配置交易消息经过排序后，当前通道的通道配置是否发生了更新，如果消息的configSeq较小，则说明当前通道配置已经更新，那么需要重新过滤与处理通道配置交易消息，以确保符合通道消息的要求
				if msg.configSeq < seq {
					//重新过滤验证普通交易消息
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					//若发现错误，则丢弃该消息，跳转继续循环
					if err != nil {
						logger.Warningf("Discarding bad normal message: %s", err)
						continue
					}
				}
				//用于将消息添加到缓存交易消息列表，按照交易处快规则切割成批量交易集合，同时提供Cut（）接口，不添加交易消息，直接切割当前的缓存交易消息列表构造成批量交易集合
				batches, _ := ch.support.BlockCutter().Ordered(msg.normalMsg)
				if len(batches) == 0 && timer == nil {
					//如果结果中不存在处快消息并且没有设定时器，则设置定时器
					timer = time.After(ch.support.SharedConfig().BatchTimeout())
					continue
				}
				//检查等待打包出块的批量交易集合列表
				//遍历批量交易集合列表
				for _, batch := range batches {
					//利用区块账本写组件将批量交易集合构造成新区块
					block := ch.support.CreateNextBlock(batch)
					//利用区块账本写组件将区块提交到账本
					ch.support.WriteBlock(block, nil)
				}
				//若存在批量交易集合列表，则取消定时器
				if len(batches) > 0 {
					timer = nil
				}
			} else {
				// ConfigMsg
				//通道配置交易消息：创建新的应用通道或更新通道配置
				if msg.configSeq < seq {//检查消息中的配置序号与当前通道的配置序号
					//重新过滤与处理配置交易消息
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					//发现错误，丢弃该消息，跳转至循环开始处继续检查
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				//将当前缓存交易消息列表切割成交易消息
				//如果当前接受的配置交易消息是用于创建新的应用通道，说明白不存在通道对象，则当前缓存交易列表中不存在任何交易消息。所以返回的batch是nil
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					//创建新区块
					block := ch.support.CreateNextBlock(batch)
					//将区块写入账本
					ch.support.WriteBlock(block, nil)
				}

				//将配置交易消息构造成新区块
				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				//创建新的应用通道或更新通道配置，在利用区块账本写组件将新区块提交到账本
				//将配置区块写入账本，同事执行通道管理
				ch.support.WriteConfigBlock(block, nil)
				timer = nil//取消定时器
			}
		//检查出块超时的定时器
		case <-timer:
			//clear the timer
			//取消定时器
			timer = nil

			//将当前缓存交易消息的列表切割成批量交易集合
			batch := ch.support.BlockCutter().Cut()
			//如果不存在任何消息，则跳转至循环开始处继续执行
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			//创建新的区块
			block := ch.support.CreateNextBlock(batch)
			//将区块写入账本
			ch.support.WriteBlock(block, nil)
		//若接受到退出消息，则退出消息处理循环
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
