// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof" // This is essentially the main package for the orderer

	"os"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/tools/configtxgen/encoder"
	genesisconfig "github.com/hyperledger/fabric/common/tools/configtxgen/localconfig"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/orderer/common/bootstrap/file"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/metadata"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/kafka"
	"github.com/hyperledger/fabric/orderer/consensus/solo"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/utils"

	"github.com/hyperledger/fabric/common/localmsp"
	"github.com/hyperledger/fabric/common/util"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/orderer/common/performance"
	"github.com/op/go-logging"
	"gopkg.in/alecthomas/kingpin.v2"
)

const pkgLogID = "orderer/common/server"

var logger *logging.Logger

func init() {
	logger = flogging.MustGetLogger(pkgLogID)
}

//command line flags
var (
	app = kingpin.New("orderer", "Hyperledger Fabric orderer node")

	start     = app.Command("start", "Start the orderer node").Default()
	version   = app.Command("version", "Show version information")
	benchmark = app.Command("benchmark", "Run orderer in benchmark mode")
)

// Main is the entry point of orderer process
func Main() {
	//解析用户命令行
	fullCmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// "version" command
	if fullCmd == version.FullCommand() {
		fmt.Println(metadata.GetVersionInfo())
		return
	}

	//加载orderer.yaml配置文件，解析获取Orderer配置信息，保存在orderer对象conf中
	conf, err := localconfig.Load()
	if err != nil {
		logger.Error("failed to parse config: ", err)
		os.Exit(1)
	}
	//初始化日志级别
	//负责设置orderer节点上的日志后端输出流 输出格式与默认日志级别
	initializeLoggingLevel(conf)
	//初始化MSP组件
	initializeLocalMsp(conf)

	//打印配置信息
	prettyPrintStruct(conf)
	//启动 Orderer排序服务器
	Start(fullCmd, conf)
}

// Start provides a layer of abstraction for benchmark test
//负责启动和测试Orderer排序节点
func Start(cmd string, conf *localconfig.TopLevel) {
	//创建本地MSP签名者实体
	signer := localmsp.NewSigner()
	//初始化grpc服务器配置
	//首先利用Orderer配置对象conf初始化TLS安全认证配置选项secureOpts
	serverConfig := initializeServerConfig(conf)
	//初始化grpc服务
	grpcServer := initializeGrpcServer(conf, serverConfig)
	//构造CA证书支持组件对象
	caSupport := &comm.CASupport{
		//Application根CA证书字典
		AppRootCAsByChain:     make(map[string][][]byte),
		//Orderer根CA证书字典
		OrdererRootCAsByChain: make(map[string][][]byte),
		//设置TLS认证的客户端根CA证书列表
		ClientRootCAs:         serverConfig.SecOpts.ClientRootCAs,
	}
	//设置TlS链接认证的回调函数
	tlsCallback := func(bundle *channelconfig.Bundle) {
		// only need to do this if mutual TLS is required
		if grpcServer.MutualTLSRequired() {
			logger.Debug("Executing callback to update root CAs")
			updateTrustedRoots(grpcServer, caSupport, bundle)
		}
	}

	//初始化多通道管理器对象
	//创建多通道注册管理器对象，用于注册Orderer节点上的所有通道（包括系统通道和应用通道），负责维护通道、账本等重要资源
	//可以创建solo和kafka两种类型的共识组件
	manager := initializeMultichannelRegistrar(conf, signer, tlsCallback)
	//设置TLS双向任之鞥标志位
	mutualTLS := serverConfig.SecOpts.UseTLS && serverConfig.SecOpts.RequireClientCert
	//创建Orderer排序服务器
	server := NewServer(manager, signer, &conf.Debug, conf.General.Authentication.TimeWindow, mutualTLS)

	//分析命令类型
	switch cmd {
	//start启动子命令
	case start.FullCommand(): // "start" command
		logger.Infof("Starting %s", metadata.GetVersionInfo())
		//goroutine启动go profile服务
		initializeProfilingService(conf)
		//将Orderer排序服务器注册到grpc服务器上
		ab.RegisterAtomicBroadcastServer(grpcServer.Server(), server)
		logger.Info("Beginning to serve requests")
		//启动grpc服务器提供Orderer服务
		grpcServer.Start()
	//benchmark测试用例子命令
	case benchmark.FullCommand(): // "benchmark" command
		logger.Info("Starting orderer in benchmark mode")
		//创建benchmaek服务器
		benchmarkServer := performance.GetBenchmarkServer()
		//注册到benchmark服务器上
		benchmarkServer.RegisterService(server)
		//启动benchmark服务器
		benchmarkServer.Start()
	}
}

// Set the logging level
func initializeLoggingLevel(conf *localconfig.TopLevel) {
	flogging.InitBackend(flogging.SetFormat(conf.General.LogFormat), os.Stderr)
	flogging.InitFromSpec(conf.General.LogLevel)
}

// Start the profiling service if enabled.
func initializeProfilingService(conf *localconfig.TopLevel) {
	if conf.General.Profile.Enabled {
		go func() {
			logger.Info("Starting Go pprof profiling service on:", conf.General.Profile.Address)
			// The ListenAndServe() call does not return unless an error occurs.
			logger.Panic("Go pprof service failed:", http.ListenAndServe(conf.General.Profile.Address, nil))
		}()
	}
}

func initializeServerConfig(conf *localconfig.TopLevel) comm.ServerConfig {
	// secure server config
	//首先利用Orderer配置对象conf初始化TLS安全认证配置选项secureOpts
	secureOpts := &comm.SecureOptions{
		UseTLS:            conf.General.TLS.Enabled,
		RequireClientCert: conf.General.TLS.ClientAuthRequired,
	}
	// check to see if TLS is enabled
	//启用TLS安全认证的是能标志位（默认为false），如果是true，则需要从指定的配置文件路径中读取服务器端的签名私钥、服务器端的身份证书，服务器端的根CA证书列表
	if secureOpts.UseTLS {
		msg := "TLS"
		// load crypto material from files
		serverCertificate, err := ioutil.ReadFile(conf.General.TLS.Certificate)
		if err != nil {
			logger.Fatalf("Failed to load server Certificate file '%s' (%s)",
				conf.General.TLS.Certificate, err)
		}
		serverKey, err := ioutil.ReadFile(conf.General.TLS.PrivateKey)
		if err != nil {
			logger.Fatalf("Failed to load PrivateKey file '%s' (%s)",
				conf.General.TLS.PrivateKey, err)
		}
		var serverRootCAs, clientRootCAs [][]byte
		for _, serverRoot := range conf.General.TLS.RootCAs {
			root, err := ioutil.ReadFile(serverRoot)
			if err != nil {
				logger.Fatalf("Failed to load ServerRootCAs file '%s' (%s)",
					err, serverRoot)
			}
			serverRootCAs = append(serverRootCAs, root)
		}
		//启用对客户端证书认证的使能标志位（默认是false），如果是true，则需要对服务器端与客户端进行双向TLS安全认证，从指定的客户端根CA生疏文件列表中读取证书
		if secureOpts.RequireClientCert {
			for _, clientRoot := range conf.General.TLS.ClientRootCAs {
				root, err := ioutil.ReadFile(clientRoot)
				if err != nil {
					logger.Fatalf("Failed to load ClientRootCAs file '%s' (%s)",
						err, clientRoot)
				}
				clientRootCAs = append(clientRootCAs, root)
			}
			msg = "mutual TLS"
		}
		secureOpts.Key = serverKey
		secureOpts.Certificate = serverCertificate
		secureOpts.ServerRootCAs = serverRootCAs
		secureOpts.ClientRootCAs = clientRootCAs
		logger.Infof("Starting orderer with %s enabled", msg)
	}
	//创建心跳消息配置项kaOpts，用于指定grpc服务器端与客户端之间的心跳消息周期，超时时间、最小心跳信息周期时间等
	kaOpts := comm.DefaultKeepaliveOptions
	// keepalive settings
	// ServerMinInterval must be greater than 0
	if conf.General.Keepalive.ServerMinInterval > time.Duration(0) {
		kaOpts.ServerMinInterval = conf.General.Keepalive.ServerMinInterval
	}
	kaOpts.ServerInterval = conf.General.Keepalive.ServerInterval
	kaOpts.ServerTimeout = conf.General.Keepalive.ServerTimeout

	//创建grpc服务器配置对象
	return comm.ServerConfig{SecOpts: secureOpts, KaOpts: kaOpts}
}

//首先创建系统通道的创世区块，初始化系统通道的区块账本对象及其区块数据存储对象，然后将创世区块添加到本地的区块数据文件中
//其中创世区块包含了系统通道的出事配置信息
func initializeBootstrapChannel(conf *localconfig.TopLevel, lf blockledger.Factory) {
	var genesisBlock *cb.Block

	// Select the bootstrapping mechanism

	//分析创世区块的生成方式
	switch conf.General.GenesisMethod {
	case "provisional":
		//根据配置文件生成创世区块
		genesisBlock = encoder.New(genesisconfig.Load(conf.General.GenesisProfile)).GenesisBlockForChannel(conf.General.SystemChannel)
	case "file":
		//根据创世区块文件生成创世区块
		genesisBlock = file.New(conf.General.GenesisFile).GenesisBlock()
	default:
		logger.Panic("Unknown genesis method:", conf.General.GenesisMethod)
	}

	//从创世区块中解析获取通道ID
	chainID, err := utils.GetChainIDFromBlock(genesisBlock)
	if err != nil {
		logger.Fatal("Failed to parse chain ID from genesis block:", err)
	}
	//创建刺痛通道的区块账本对象
	gl, err := lf.GetOrCreate(chainID)
	if err != nil {
		logger.Fatal("Failed to create the system chain:", err)
	}

	//将创世区块genesisBlock添加到系统通道账本的区块文件中
	//利用区块句存储对象底层的区块文件管理器，将创世区块添加到系统通道账本的区块数据文件中
	//同时，保存区块检查点信息与索引检查点信息，建立区块索引信息，更新区块文件管理器上的区块检查点信息
	err = gl.Append(genesisBlock)
	if err != nil {
		logger.Fatal("Could not write genesis block to ledger:", err)
	}
}

//根据Orderer配置信息对象conf与服务器配置项serverConfig创建Orderer节点上的grpc服务器
func initializeGrpcServer(conf *localconfig.TopLevel, serverConfig comm.ServerConfig) *comm.GRPCServer {
	//构造指定地址与端口7050上的监听器
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.General.ListenAddress, conf.General.ListenPort))
	if err != nil {
		logger.Fatal("Failed to listen:", err)
	}

	// Create GRPC server - return if an error occurs
	//基于以上监听器和服务配置项创建grpc服务器实力
	grpcServer, err := comm.NewGRPCServerFromListener(lis, serverConfig)
	if err != nil {
		logger.Fatal("Failed to return new GRPC server:", err)
	}

	return grpcServer
}

func initializeLocalMsp(conf *localconfig.TopLevel) {
	// Load local MSP
	//根据MSP配置文件，BCCSp密码服务组件配置，MSP名称初始化本地MSP组件
	//本地的MSP对象默认使用bccmsp类型对象，该类型的MSP组件是给予BCCSP组件提供密码套件服务，封装了MSP组件信任的相关证书列表等
	err := mspmgmt.LoadLocalMsp(conf.General.LocalMSPDir, conf.General.BCCSP, conf.General.LocalMSPID)
	if err != nil { // Handle errors reading the config file
		logger.Fatal("Failed to initialize local MSP:", err)
	}
}

//创建并初始化Orderer节点上的多通道注册管理器对象，用于注册管理Orderer节点上的所有通道（包括系统通道和应用通道）、区块账本、共识组件等资源
//多通道注册管理器相当于Orderer节点上的“资源管理器”，位每一个通道创建关联的共识组件链对象，负责交易排序、打包处快、提交账本以及通道管理等工作
func initializeMultichannelRegistrar(conf *localconfig.TopLevel, signer crypto.LocalSigner,
	callbacks ...func(bundle *channelconfig.Bundle)) *multichannel.Registrar {
	//创建通道的账本工厂对象lf，根据Orderer的配置信息对象conf参数
	lf, _ := createLedgerFactory(conf)
	// Are we bootstrapping?
	//不存在任何通道，orderer没有创建任何通道
	if len(lf.ChainIDs()) == 0 {
		//初始化系统通道，根据Orderer的配置参数conf和账本工厂对象lf。
		initializeBootstrapChannel(conf, lf)
	} else {
		logger.Info("Not bootstrapping because of existing chains")
	}

	//创建并设置共识组件字典
	consenters := make(map[string]consensus.Consenter)
	//solo类型共识组件
	//直接返回solo共识组件对象
	consenters["solo"] = solo.New()
	//kafka类型共识组件
	consenters["kafka"] = kafka.New(conf.Kafka)

	//创建多通道注册管理器对象
	return multichannel.NewRegistrar(lf, consenters, signer, callbacks...)
}

func updateTrustedRoots(srv *comm.GRPCServer, rootCASupport *comm.CASupport,
	cm channelconfig.Resources) {
	rootCASupport.Lock()
	defer rootCASupport.Unlock()

	appRootCAs := [][]byte{}
	ordererRootCAs := [][]byte{}
	appOrgMSPs := make(map[string]struct{})
	ordOrgMSPs := make(map[string]struct{})

	if ac, ok := cm.ApplicationConfig(); ok {
		//loop through app orgs and build map of MSPIDs
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	if ac, ok := cm.OrdererConfig(); ok {
		//loop through orderer orgs and build map of MSPIDs
		for _, ordOrg := range ac.Organizations() {
			ordOrgMSPs[ordOrg.MSPID()] = struct{}{}
		}
	}

	if cc, ok := cm.ConsortiumsConfig(); ok {
		for _, consortium := range cc.Consortiums() {
			//loop through consortium orgs and build map of MSPIDs
			for _, consortiumOrg := range consortium.Organizations() {
				appOrgMSPs[consortiumOrg.MSPID()] = struct{}{}
			}
		}
	}

	cid := cm.ConfigtxValidator().ChainID()
	logger.Debugf("updating root CAs for channel [%s]", cid)
	msps, err := cm.MSPManager().GetMSPs()
	if err != nil {
		logger.Errorf("Error getting root CAs for channel %s (%s)", cid, err)
	}
	if err == nil {
		for k, v := range msps {
			// check to see if this is a FABRIC MSP
			if v.GetType() == msp.FABRIC {
				for _, root := range v.GetTLSRootCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						logger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, root)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						logger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, root)
					}
				}
				for _, intermediate := range v.GetTLSIntermediateCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						logger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, intermediate)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						logger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, intermediate)
					}
				}
			}
		}
		rootCASupport.AppRootCAsByChain[cid] = appRootCAs
		rootCASupport.OrdererRootCAsByChain[cid] = ordererRootCAs

		// now iterate over all roots for all app and orderer chains
		trustedRoots := [][]byte{}
		for _, roots := range rootCASupport.AppRootCAsByChain {
			trustedRoots = append(trustedRoots, roots...)
		}
		for _, roots := range rootCASupport.OrdererRootCAsByChain {
			trustedRoots = append(trustedRoots, roots...)
		}
		// also need to append statically configured root certs
		if len(rootCASupport.ClientRootCAs) > 0 {
			trustedRoots = append(trustedRoots, rootCASupport.ClientRootCAs...)
		}

		// now update the client roots for the gRPC server
		err := srv.SetClientRootCAs(trustedRoots)
		if err != nil {
			msg := "Failed to update trusted roots for orderer from latest config " +
				"block.  This orderer may not be able to communicate " +
				"with members of channel %s (%s)"
			logger.Warningf(msg, cm.ConfigtxValidator().ChainID(), err)
		}
	}
}

func prettyPrintStruct(i interface{}) {
	params := util.Flatten(i)
	var buffer bytes.Buffer
	for i := range params {
		buffer.WriteString("\n\t")
		buffer.WriteString(params[i])
	}
	logger.Infof("Orderer config values:%s\n", buffer.String())
}
