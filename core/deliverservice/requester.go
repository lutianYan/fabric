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

package deliverclient

import (
	"math"

	"github.com/hyperledger/fabric/common/localmsp"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/deliverservice/blocksprovider"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/utils"
)

type blocksRequester struct {
	tls     bool
	chainID string
	client  blocksprovider.BlocksDeliverer
}
//peer向orderer索要数据
//LedgerInfo包含了高度信息
//若该高度值为向orderer索要的开始的block的序列号，如果高度值>0，则调用seekLatestFromCommitter，否则调用seekOldest()
func (b *blocksRequester) RequestBlocks(ledgerInfoProvider blocksprovider.LedgerInfo) error {
	//获取本地账本高度
	//获取peer节点（Leader）节点的区块账本高度height
	height, err := ledgerInfoProvider.LedgerHeight()
	if err != nil {
		logger.Errorf("Can't get ledger height for channel %s from committer [%s]", b.chainID, err)
		return err
	}

	if height > 0 {
		//检查账本高度
		logger.Debugf("Starting deliver with block [%d] for channel %s", height, b.chainID)
		//请求从最新账本高度开始获取区块数据集合
		if err := b.seekLatestFromCommitter(height); err != nil {
			return err
		}
	} else {
		logger.Debugf("Starting deliver with oldest block for channel %s", b.chainID)
		//请求从最旧的区块开始获取区块数据集合
		if err := b.seekOldest(); err != nil {
			return err
		}
	}

	return nil
}

func (b *blocksRequester) getTLSCertHash() []byte {
	if b.tls {
		return util.ComputeSHA256(comm.GetCredentialSupport().GetClientCertificate().Certificate[0])
	}
	return nil
}

//搜索从最旧的区块开始的区块数据集合
func (b *blocksRequester) seekOldest() error {
	//构造区块搜索信息
	seekInfo := &orderer.SeekInfo{
		Start:    &orderer.SeekPosition{Type: &orderer.SeekPosition_Oldest{Oldest: &orderer.SeekOldest{}}}, //开始位置
		Stop:     &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}}, //结束位置
		Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY, //搜索区块行为类型
	}

	//TODO- epoch and msgVersion may need to be obtained for nowfollowing usage in orderer/configupdate/configupdate.go
	msgVersion := int32(0)
	epoch := uint64(0)
	tlsCertHash := b.getTLSCertHash()
	//创建请求消息（添加通道Id、消息版本、TLS证书hash等信息），创建新的签名请求消息
	env, err := utils.CreateSignedEnvelopeWithTLSBinding(common.HeaderType_DELIVER_SEEK_INFO, b.chainID, localmsp.NewSigner(), seekInfo, msgVersion, epoch, tlsCertHash)
	if err != nil {
		return err
	}
	//发送请求消息给Orderer
	return b.client.Send(env)
}

//搜索从指定区块高度开始的区块数据集合
func (b *blocksRequester) seekLatestFromCommitter(height uint64) error {
	//创建一个包装了SeekInfo信息（即索要的block的起止范围信息）的HeaderType_CONFIG_UPDATE类型的Envelope消息
	seekInfo := &orderer.SeekInfo{
		Start:    &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: height}}}, //开始位置
		Stop:     &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}}, //结束位置
		Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY, //搜索区块行为类型
	}

	//TODO- epoch and msgVersion may need to be obtained for nowfollowing usage in orderer/configupdate/configupdate.go
	msgVersion := int32(0)
	epoch := uint64(0)
	tlsCertHash := b.getTLSCertHash()
	//创建请求消息
	env, err := utils.CreateSignedEnvelopeWithTLSBinding(common.HeaderType_DELIVER_SEEK_INFO, b.chainID, localmsp.NewSigner(), seekInfo, msgVersion, epoch, tlsCertHash)
	if err != nil {
		return err
	}
	//向orderer端的Deliver服务端索要block
	//发送请求消息
	return b.client.Send(env)
}
