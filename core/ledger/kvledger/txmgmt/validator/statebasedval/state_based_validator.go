/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statebasedval

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valinternal"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
)

var logger = flogging.MustGetLogger("statebasedval")

// Validator validates a tx against the latest committed state
// and preceding valid transactions with in the same block
//验证器
type Validator struct {
	db privacyenabledstate.DB
}

// NewValidator constructs StateValidator
func NewValidator(db privacyenabledstate.DB) *Validator {
	return &Validator{db}
}

// preLoadCommittedVersionOfRSet loads committed version of all keys in each
// transaction's read set into a cache.
//preLoadCommittedVersionOfRSet将每个事务的读取集中的所有键的已提交版本加载到缓存中。
func (v *Validator) preLoadCommittedVersionOfRSet(block *valinternal.Block) error {

	// Collect both public and hashed keys in read sets of all transactions in a given block
	var pubKeys []*statedb.CompositeKey
	var hashedKeys []*privacyenabledstate.HashedCompositeKey

	// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
	// pubKeys and hashedKeys. Though map alone can be used to collect keys in
	// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
	// array is used for better code readability. On the negative side, this approach
	// might use some extra memory.
	pubKeysMap := make(map[statedb.CompositeKey]interface{})
	hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})

	for _, tx := range block.Txs {
		for _, nsRWSet := range tx.RWSet.NsRwSets {
			for _, kvRead := range nsRWSet.KvRwSet.Reads {
				compositeKey := statedb.CompositeKey{
					Namespace: nsRWSet.NameSpace,
					Key:       kvRead.Key,
				}
				if _, ok := pubKeysMap[compositeKey]; !ok {
					pubKeysMap[compositeKey] = nil
					pubKeys = append(pubKeys, &compositeKey)
				}

			}
			for _, colHashedRwSet := range nsRWSet.CollHashedRwSets {
				for _, kvHashedRead := range colHashedRwSet.HashedRwSet.HashedReads {
					hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
						Namespace:      nsRWSet.NameSpace,
						CollectionName: colHashedRwSet.CollectionName,
						KeyHash:        string(kvHashedRead.KeyHash),
					}
					if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
						hashedKeysMap[hashedCompositeKey] = nil
						hashedKeys = append(hashedKeys, &hashedCompositeKey)
					}
				}
			}
		}
	}

	// Load committed version of all keys into a cache
	if len(pubKeys) > 0 || len(hashedKeys) > 0 {
		err := v.db.LoadCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

// ValidateAndPrepareBatch implements method in Validator interface
//负责实现验证功能
//其中第二个参数决定是否使用mvcc验证，默认为true（多版本并发控制）
//验证的内容是：key对应的版本号
//双方是：block中携带的读集和范围读集
func (v *Validator) ValidateAndPrepareBatch(block *valinternal.Block, doMVCCValidation bool) (*valinternal.PubAndHashUpdates, error) {
	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		//preLoadCommittedVersionOfRSet将每个事务的读取集中的所有键的已提交版本加载到缓存中。
		err := v.preLoadCommittedVersionOfRSet(block)
		if err != nil {
			return nil, err
		}
	}
	//准备updates用于存放批量升级包
	updates := valinternal.NewPubAndHashUpdates()
	for _, tx := range block.Txs {
		var validationCode peer.TxValidationCode
		var err error
		//验证交易，返回交易的读写集和验证结果
		//就是一个分发任务依次验证的过程
		//返回为peer.TxValidationCode_VALID，则说明经过上述验证读集、验证范围读集的过程，证明当前交易有效且可以写入state数据库
		if validationCode, err = v.validateEndorserTX(tx.RWSet, doMVCCValidation, updates); err != nil {
			return nil, err
		}

		tx.ValidationCode = validationCode
		if validationCode == peer.TxValidationCode_VALID {
			logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator", block.Num, tx.IndexInBlock, tx.ID)
			//生成提交版本号(写值自身是不带版本号的，而写值的向state数据提交的版本号就是在这生成的）
			committingTxHeight := version.NewHeight(block.Num, uint64(tx.IndexInBlock))
			//将有效交易的写集和对应的提交版本号放入批量升级包updates
			updates.ApplyWriteSet(tx.RWSet, committingTxHeight)
		} else {
			logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
				block.Num, tx.IndexInBlock, tx.ID, validationCode.String())
		}
	}
	return updates, nil
}

// validateEndorserTX validates endorser transaction
//分发任务依次验证的过程，交易读写集合将需要验证的读写集合抽取出来，把任务交给v.validatetx()
//然后经过分拆又把验证读集的任务交给validateReadSet
//将验证范围读集合的任务交给validateRangeQueries
func (v *Validator) validateEndorserTX(
	txRWSet *rwsetutil.TxRwSet,
	doMVCCValidation bool,
	updates *valinternal.PubAndHashUpdates) (peer.TxValidationCode, error) {

	var validationCode = peer.TxValidationCode_VALID
	var err error
	//mvccvalidation, may invalidate transaction
	if doMVCCValidation {
		//将任务进行分发
		validationCode, err = v.validateTx(txRWSet, updates)
	}
	return validationCode, err
}

func (v *Validator) validateTx(txRWSet *rwsetutil.TxRwSet, updates *valinternal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))
	for _, nsRWSet := range txRWSet.NsRwSets {
		ns := nsRWSet.NameSpace
		// Validate public reads
		//验证读集合
		if valid, err := v.validateReadSet(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		//验证范围读集合
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	return peer.TxValidationCode_VALID, nil
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of public read-set
////////////////////////////////////////////////////////////////////////////////
//验证读集合
func (v *Validator) validateReadSet(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	//循环调用validateKVRead，一一验证每个读值
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVRead(ns, kvRead, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVRead performs mvcc check for a key read during transaction simulation.
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVRead(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	//查看升级包中是否有与读值相同的key，若存在，则判断交易失效
	if updates.Exists(ns, kvRead.Key) {
		return false, nil
	}
	//状态数据库中key的版本
	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		return false, err
	}

	logger.Debugf("Comparing versions for key [%s]: committed version=%#v and read version=%#v",
		kvRead.Key, committedVersion, rwsetutil.NewVersion(kvRead.Version))
	//比较两个版本号是否一致，若不一致，则判定当前交易无效，直接返回false 否则返回true
	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvRead.Version)) {
		logger.Debugf("Version mismatch for key [%s:%s]. Committed version = [%#v], Version in readSet [%#v]",
			ns, kvRead.Key, committedVersion, kvRead.Version)
		return false, nil
	}
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of range queries
////////////////////////////////////////////////////////////////////////////////
//验证范围读集
//一一验证每个范围读值
//范围读值的验证由于是一系列的值，这一系列的值既要与state中现存的版本号比较，也要与updates中已存在的key比较
//还牵扯到可能范围读值中的一个key在updates和state中同时存在（这是这个key就存在两个版本号了）而选哪一个版本号来和范围读值中这个key的版本比较的问题，所以就稍显麻烦

func (v *Validator) validateRangeQueries(ns string, rangeQueriesInfo []*kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	//验证范围读集的方法就是循环调用rangeQueriesInfo，一一验证每个范围读值
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateRangeQuery(ns, rqi, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateRangeQuery performs a phantom read check i.e., it
// checks whether the results of the range query are still the same when executed on the
// statedb (latest state as of last committed block) + updates (prepared by the writes of preceding valid transactions
// in the current block and yet to be committed as part of group commit at the end of the validation of the block)
//范围读值的验证由于是一系列的值，这一系列的值既要与state中现在存在的版本号进行比较，也要与updates中已经存在的key比较，还涉及可能范围读值中的一个key在updates和state
//中同时存在（这时这个key就存在两个版本号了）而选择哪一个版本号来和范围读值中这个key的版本比较的问题，所以就稍显麻烦
func (v *Validator) validateRangeQuery(ns string, rangeQueryInfo *kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	logger.Debugf("validateRangeQuery: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	// If during simulation, the caller had not exhausted the iterator so
	// rangeQueryInfo.EndKey is not actual endKey given by the caller in the range query
	// but rather it is the last key seen by the caller and hence the combinedItr should include the endKey in the results.
	//记录下此范围读值中是否包含与endKey对应的读值
	//因为范围读值默认是不包含endkey对应的读值的，但是当这个范围读值没有被模拟器读尽时，
	// 比如key2-key10只读了前3个就返回了，此时rangeQueryInfo.ItrExhausted值为false时，
	// 则说明此时实际的endkey是key4而非key10，且此时范围读值应该包含key4的读值。
	// 相反，若ItrExhausted值为true时，则说明模拟器读尽了key2-key10间的8个key，由于范围读值默认的不包含endkey，所以此时范围读值中不包含key10对应的读值

	includeEndKey := !rangeQueryInfo.ItrExhausted

	//根据updates，startkey，endkey，includeEndKey，创建一个联合迭代器。
	combinedItr, err := newCombinedIterator(v.db, updates.UpdateBatch,
		ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey, includeEndKey)
	if err != nil {
		return false, err
	}
	defer combinedItr.Close()
	var validator rangeQueryValidator
	//默认返回的范围读值中是归并的hash值
	if rangeQueryInfo.GetReadsMerkleHashes() != nil {
		logger.Debug(`Hashing results are present in the range query info hence, initiating hashing based validation`)
		validator = &rangeQueryHashValidator{}
	} else {
		logger.Debug(`Hashing results are not present in the range query info hence, initiating raw KVReads based validation`)
		validator = &rangeQueryResultsValidator{}
	}
	//根据范围读值rangeQueryInfo和生成的联合迭代器combinedItr，同时内建了一个上文提及的RangeQueryResultsHelper工具，创建一个用于验证范围读值的归并哈希值的验证器。
	validator.init(rangeQueryInfo, combinedItr)
	//使用验证器验证范围读值。
	// 粗略的验证过程就是使用RangeQueryResultsHelper工具重新一步步构建出一个与rangeQueryInfo范围相同的默克尔树并不断进行归并哈希的操作，将得到的归并哈希值与rangeQueryInfo携带的哈希值进行比较。
	return validator.validate()
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of hashed read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateNsHashedReadSets(ns string, collHashedRWSets []*rwsetutil.CollHashedRwSet,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateCollHashedReadSet(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwSet.HashedReads, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateCollHashedReadSet(ns, coll string, kvReadHashes []*kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateKVReadHash(ns, coll, kvReadHash, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVReadHash performs mvcc check for a hash of a key that is present in the private data space
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVReadHash(ns, coll string, kvReadHash *kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	if updates.Contains(ns, coll, kvReadHash.KeyHash) {
		return false, nil
	}
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.KeyHash)
	if err != nil {
		return false, err
	}

	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvReadHash.Version)) {
		logger.Debugf("Version mismatch for key hash [%s:%s:%#v]. Committed version = [%s], Version in hashedReadSet [%s]",
			ns, coll, kvReadHash.KeyHash, committedVersion, kvReadHash.Version)
		return false, nil
	}
	return true, nil
}
