/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/orderer/common/broadcast"
	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/pkg/errors"
)

type broadcastSupport struct {
	*multichannel.Registrar
}

func (bs broadcastSupport) BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, broadcast.ChannelSupport, error) {
	return bs.Registrar.BroadcastChannelSupport(msg)
}

type deliverSupport struct {
	*multichannel.Registrar
}

func (ds deliverSupport) GetChain(chainID string) (deliver.Chain, bool) {
	return ds.Registrar.GetChain(chainID)
}

type server struct {
	bh    broadcast.Handler
	dh    *deliver.Handler
	debug *localconfig.Debug
	*multichannel.Registrar
}

type responseSender struct {
	ab.AtomicBroadcast_DeliverServer
}

func (rs *responseSender) SendStatusResponse(status cb.Status) error {
	reply := &ab.DeliverResponse{
		Type: &ab.DeliverResponse_Status{Status: status},
	}
	return rs.Send(reply)
}

func (rs *responseSender) SendBlockResponse(block *cb.Block) error {
	response := &ab.DeliverResponse{
		Type: &ab.DeliverResponse_Block{Block: block},
	}
	return rs.Send(response)
}

// NewServer creates an ab.AtomicBroadcastServer based on the broadcast target and ledger Reader
//创建orderer排序服务器，并注册到本地默认的grpc服务器（默认为7050端口），提供Broadcast()与Deliver()借口
//通过自身的Broadcast服务器处理句柄与Deliver服务器处理句柄，分别接受与处理对应的消息请求
//同时，基于自身的多通道注册管理器对象管理Orderer节点上所有的通道配置和账本 共识组件等，并创建共识组件链对象负责通道管理 交易拍戏工作
func NewServer(r *multichannel.Registrar, _ crypto.LocalSigner, debug *localconfig.Debug, timeWindow time.Duration, mutualTLS bool) ab.AtomicBroadcastServer {
	s := &server{
		dh:        deliver.NewHandler(deliverSupport{Registrar: r}, timeWindow, mutualTLS), //Deliver服务处理句柄
		bh:        broadcast.NewHandlerImpl(broadcastSupport{Registrar: r}), //Broadcast服务处理句柄
		debug:     debug, //调试信息
		Registrar: r, //多通道注册管理器
	}
	return s
}

type msgTracer struct {
	function string
	debug    *localconfig.Debug
}

func (mt *msgTracer) trace(traceDir string, msg *cb.Envelope, err error) {
	if err != nil {
		return
	}

	now := time.Now().UnixNano()
	path := fmt.Sprintf("%s%c%d_%p.%s", traceDir, os.PathSeparator, now, msg, mt.function)
	logger.Debugf("Writing %s request trace to %s", mt.function, path)
	go func() {
		pb, err := proto.Marshal(msg)
		if err != nil {
			logger.Debugf("Error marshaling trace msg for %s: %s", path, err)
			return
		}
		err = ioutil.WriteFile(path, pb, 0660)
		if err != nil {
			logger.Debugf("Error writing trace msg for %s: %s", path, err)
		}
	}()
}

type broadcastMsgTracer struct {
	ab.AtomicBroadcast_BroadcastServer
	msgTracer
}

func (bmt *broadcastMsgTracer) Recv() (*cb.Envelope, error) {
	msg, err := bmt.AtomicBroadcast_BroadcastServer.Recv()
	if traceDir := bmt.debug.BroadcastTraceDir; traceDir != "" {
		bmt.trace(bmt.debug.BroadcastTraceDir, msg, err)
	}
	return msg, err
}

type deliverMsgTracer struct {
	deliver.Receiver
	msgTracer
}

func (dmt *deliverMsgTracer) Recv() (*cb.Envelope, error) {
	msg, err := dmt.Receiver.Recv()
	if traceDir := dmt.debug.DeliverTraceDir; traceDir != "" {
		dmt.trace(traceDir, msg, err)
	}
	return msg, err
}

// Broadcast receives a stream of messages from a client for ordering
func (s *server) Broadcast(srv ab.AtomicBroadcast_BroadcastServer) error {
	logger.Debugf("Starting new Broadcast handler")
	defer func() {
		if r := recover(); r != nil {
			logger.Criticalf("Broadcast client triggered panic: %s\n%s", r, debug.Stack())
		}
		logger.Debugf("Closing Broadcast stream")
	}()
	return s.bh.Handle(&broadcastMsgTracer{
		AtomicBroadcast_BroadcastServer: srv,
		msgTracer: msgTracer{
			debug:    s.debug,
			function: "Broadcast",
		},
	})
}

// Deliver sends a stream of blocks to a client after ordering
//Deliver区块请求服务方法
func (s *server) Deliver(srv ab.AtomicBroadcast_DeliverServer) error {
	logger.Debugf("Starting new Deliver handler")
	defer func() {
		if r := recover(); r != nil {
			logger.Criticalf("Deliver client triggered panic: %s\n%s", r, debug.Stack())
		}
		logger.Debugf("Closing Deliver stream")
	}()

	//定义策略检查器
	//用于检查接受的区块请求消息必须满足指定通道上的访问控制权限策略的要求
	policyChecker := func(env *cb.Envelope, channelID string) error {
		//获取指定通道的链支持对象
		chain, ok := s.GetChain(channelID)
		if !ok {
			return errors.Errorf("channel %s not found", channelID)
		}
		//创建消息过滤器
		sf := msgprocessor.NewSigFilter(policies.ChannelReaders, chain)
		//过滤消息
		return sf.Apply(env)
	}
	deliverServer := &deliver.Server{
		PolicyChecker: deliver.PolicyCheckerFunc(policyChecker),
		Receiver: &deliverMsgTracer{
			Receiver: srv,
			msgTracer: msgTracer{
				debug:    s.debug,
				function: "Deliver",
			},
		},
		ResponseSender: &responseSender{
			AtomicBroadcast_DeliverServer: srv,
		},
	}

	//Deliver服务消息处理
	return s.dh.Handle(srv.Context(), deliverServer)
}

//发送指定执行结果状态类型的Deliver服务相应消息
func (s *server) sendProducer(srv ab.AtomicBroadcast_DeliverServer) func(msg proto.Message) error {
	return func(msg proto.Message) error {
		response, ok := msg.(*ab.DeliverResponse)
		if !ok {
			logger.Errorf("received wrong response type, expected response type ab.DeliverResponse")
			return errors.New("expected response type ab.DeliverResponse")
		}
		return srv.Send(response)
	}
}
