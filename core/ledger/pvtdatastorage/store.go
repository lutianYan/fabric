/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
//在联盟链中，不是所有的数据都是共享的，一些私有的信息我们并不想让盟友掌握，所以需要一些私有数据的存储机制
//该文件函数就是为了提供私有的信息存储机制而存在的
package pvtdatastorage

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
)

// Provider provides handle to specific 'Store' that in turn manages
// private write sets for a ledger
type Provider interface {
	OpenStore(id string) (Store, error)
	Close()
}

// Store manages the permanent storage of private write sets for a ledger
// Beacsue the pvt data is supposed to be in sync with the blocks in the
// ledger, both should logically happen in an atomic operation. In order
// to accomplish this, an implementation of this store should provide
// support for a two-phase like commit/rollback capability.
// The expected use is such that - first the private data will be given to
// this store (via `Prepare` funtion) and then the block is appended to the block storage.
// Finally, one of the functions `Commit` or `Rollback` is invoked on this store based
// on whether the block was written successfully or not. The store implementation
// is expected to survive a server crash between the call to `Prepare` and `Commit`/`Rollback`
//永久存储私有信息的机制，因为私有信息应该与账本中的区块信息同步，所以私有信息需要以原子操作的形式实现。为了实现这种功能，接口store的实现需要提供如commit/roolback的两阶段的提交功能
//具体的实现：私有数据通过prepare函数提交给Store，然后区块会被附加到区块存储中，最后根据区块是否写入成功，分别调用commit和rollback
//Store接口的实现应该能够调用Prepare和Commot/Roolback函数之间容忍服务器崩溃
type Store interface {
	// Init initializes the store. This function is expected to be invoked before using the store
	Init(btlPolicy pvtdatapolicy.BTLPolicy)
	// InitLastCommittedBlockHeight sets the last commited block height into the pvt data store
	// This function is used in a special case where the peer is started up with the blockchain
	// from an earlier version of a peer when the pvt data feature (and hence this store) was not
	// available. This function is expected to be called only this situation and hence is
	// expected to throw an error if the store is not empty. On a successful return from this
	// fucntion the state of the store is expected to be same as of calling the prepare/commit
	// function for block `0` through `blockNum` with no pvt data
	InitLastCommittedBlock(blockNum uint64) error
	// GetPvtDataByBlockNum returns only the pvt data  corresponding to the given block number
	// The pvt data is filtered by the list of 'ns/collections' supplied in the filter
	// A nil filter does not filter any results
	GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error)
	// Prepare prepares the Store for commiting the pvt data. This call does not commit the pvt data.
	// Subsequently, the caller is expected to call either `Commit` or `Rollback` function.
	// Return from this should ensure that enough preparation is done such that `Commit` function invoked afterwards
	// can commit the data and the store is capable of surviving a crash between this function call and the next
	// invoke to the `Commit`
	//这个调用不提交pvt数据。随后，调用者应该调用`Commit`或`Rollback`函数。从这里返回应该确保做足够的准备工作，以便之后调用的`Commit`函数可以提交数据和store能够在这个函数调用和下一次调用`Commit`之间幸存
	Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error
	// Commit commits the pvt data passed in the previous invoke to the `Prepare` function
	Commit() error
	// Rollback rolls back the pvt data passed in the previous invoke to the `Prepare` function
	Rollback() error
	// IsEmpty returns true if the store does not have any block committed yet
	IsEmpty() (bool, error)
	// LastCommittedBlockHeight returns the height of the last committed block
	LastCommittedBlockHeight() (uint64, error)
	// HasPendingBatch returns if the store has a pending batch
	HasPendingBatch() (bool, error)
	// Shutdown stops the store
	Shutdown()
}

// ErrIllegalCall is to be thrown by a store impl if the store does not expect a call to Prepare/Commit/Rollback/InitLastCommittedBlock
type ErrIllegalCall struct {
	msg string
}

func (err *ErrIllegalCall) Error() string {
	return err.msg
}

// ErrIllegalArgs is to be thrown by a store impl if the args passed are not allowed
type ErrIllegalArgs struct {
	msg string
}

func (err *ErrIllegalArgs) Error() string {
	return err.msg
}

// ErrOutOfRange is to be thrown for the request for the data that is not yet committed
type ErrOutOfRange struct {
	msg string
}

func (err *ErrOutOfRange) Error() string {
	return err.msg
}
