/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lockbasedtxmgr

import (
	"errors"
	"fmt"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
)

// LockBasedTxSimulator is a transaction simulator used in `LockBasedTxMgr`
//交易模拟器
type lockBasedTxSimulator struct {
	lockBasedQueryExecutor    //包含QueryExecutor，QueryExecutor算是TxSimulator在差存功能上的曾强和扩展
	rwsetBuilder              *rwsetutil.RWSetBuilder
	writePerformed            bool
	pvtdataQueriesPerformed   bool
	simulationResultsComputed bool
}

func newLockBasedTxSimulator(txmgr *LockBasedTxMgr, txid string) (*lockBasedTxSimulator, error) {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	//实现自身的查询方面的功能
	helper := newQueryHelper(txmgr, rwsetBuilder)
	logger.Debugf("constructing new tx simulator txid = [%s]", txid)
	return &lockBasedTxSimulator{lockBasedQueryExecutor{helper, txid}, rwsetBuilder, false, false, false}, nil
}

// SetState implements method in interface `ledger.TxSimulator`
//将一个值写入交易的写集合
func (s *lockBasedTxSimulator) SetState(ns string, key string, value []byte) error {
	if err := s.helper.checkDone(); err != nil {
		return err
	}
	if err := s.checkBeforeWrite(); err != nil {
		return err
	}
	//验证key和value的编码等
	if err := s.helper.txmgr.db.ValidateKeyValue(key, value); err != nil {
		return err
	}
	//将一个值写入写集
	s.rwsetBuilder.AddToWriteSet(ns, key, value)
	return nil
}

// DeleteState implements method in interface `ledger.TxSimulator`
//删除一个值，当给的key的value为nil时，即表示要将此key的值赋值为nil，也就是要删除这个这个值
func (s *lockBasedTxSimulator) DeleteState(ns string, key string) error {
	return s.SetState(ns, key, nil)
}

// SetStateMultipleKeys implements method in interface `ledger.TxSimulator`
//一次性写入设置多个键值对，写入交易的写集合中
func (s *lockBasedTxSimulator) SetStateMultipleKeys(namespace string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetState(namespace, k, v); err != nil {
			return err
		}
	}
	return nil
}

// SetStateMetadata implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) SetStateMetadata(namespace, key string, metadata map[string][]byte) error {
	return errors.New("not implemented")
}

// DeleteStateMetadata implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) DeleteStateMetadata(namespace, key string) error {
	return errors.New("not implemented")
}

// SetPrivateData implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) SetPrivateData(ns, coll, key string, value []byte) error {
	if err := s.helper.validateCollName(ns, coll); err != nil {
		return err
	}
	if err := s.helper.checkDone(); err != nil {
		return err
	}
	if err := s.checkBeforeWrite(); err != nil {
		return err
	}
	if err := s.helper.txmgr.db.ValidateKeyValue(key, value); err != nil {
		return err
	}
	s.writePerformed = true
	s.rwsetBuilder.AddToPvtAndHashedWriteSet(ns, coll, key, value)
	return nil
}

// DeletePrivateData implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) DeletePrivateData(ns, coll, key string) error {
	return s.SetPrivateData(ns, coll, key, nil)
}

// SetPrivateDataMultipleKeys implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) SetPrivateDataMultipleKeys(ns, coll string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetPrivateData(ns, coll, k, v); err != nil {
			return err
		}
	}
	return nil
}

// GetPrivateDataRangeScanIterator implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.lockBasedQueryExecutor.GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey)
}

// SetPrivateDataMetadata implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) SetPrivateDataMetadata(namespace, collection, key string, metadata map[string][]byte) error {
	return errors.New("not implemented")
}

// DeletePrivateMetadata implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) DeletePrivateDataMetadata(namespace, collection, key string) error {
	return errors.New("not implemented")
}

// ExecuteQueryOnPrivateData implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.lockBasedQueryExecutor.ExecuteQueryOnPrivateData(namespace, collection, query)
}

// GetTxSimulationResults implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) GetTxSimulationResults() (*ledger.TxSimulationResults, error) {
	if s.simulationResultsComputed {
		return nil, errors.New("the function GetTxSimulationResults() should only be called once on a transaction simulator instance")
	}
	defer func() { s.simulationResultsComputed = true }()
	logger.Debugf("Simulation completed, getting simulation results")
	if s.helper.err != nil {
		return nil, s.helper.err
	}
	s.helper.addRangeQueryInfo()
	return s.rwsetBuilder.GetTxSimulationResults()
}

// ExecuteUpdate implements method in interface `ledger.TxSimulator`
func (s *lockBasedTxSimulator) ExecuteUpdate(query string) error {
	return errors.New("Not supported")
}

func (s *lockBasedTxSimulator) checkBeforeWrite() error {
	if s.pvtdataQueriesPerformed {
		return &txmgr.ErrUnsupportedTransaction{
			Msg: fmt.Sprintf("Tx [%s]: Transaction has already performed queries on pvt data. Writes are not allowed", s.txid),
		}
	}
	s.writePerformed = true
	return nil
}

func (s *lockBasedTxSimulator) checkBeforePvtdataQueries() error {
	if s.writePerformed {
		return &txmgr.ErrUnsupportedTransaction{
			Msg: fmt.Sprintf("Tx [%s]: Queries on pvt data is supported only in a read-only transaction", s.txid),
		}
	}
	s.pvtdataQueriesPerformed = true
	return nil
}
