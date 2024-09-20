package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/metrics"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const (
	parallelPrimarySlot = 0
	parallelShadowSlot  = 1
	stage2CheckNumber   = 30 // ConfirmStage2 will check this number of transaction, to avoid too busy stage2 check
	stage2AheadNum      = 3  // enter ConfirmStage2 in advance to avoid waiting for Fat Tx
)

var (
	FallbackToSerialProcessorErr = errors.New("fallback to serial processor")
)

type ResultHandleEnv struct {
	statedb     *state.StateDB
	gp          *GasPool
	txCount     int
	isByzantium bool
}

type ParallelStateProcessor struct {
	StateProcessor
	parallelNum           int          // leave a CPU to dispatcher
	slotState             []*SlotState // idle, or pending messages
	allTxReqs             []*ParallelTxRequest
	txResultChan          chan *ParallelTxResult // to notify dispatcher that a tx is done
	mergedTxIndex         atomic.Int32           // the latest finalized tx index
	mergedTxCount         atomic.Int32           // the number of merged tx
	pendingConfirmResults *sync.Map              // tx could be executed several times, with several result to check
	unconfirmedResults    *sync.Map              // for stage2 confirm, since pendingConfirmResults can not be accessed in stage2 loop
	unconfirmedDBs        *sync.Map              // intermediate store of slotDB that is not verified
	slotDBsToRelease      *sync.Map
	stopSlotChan          chan struct{}
	stopConfirmChan       chan struct{}
	debugConflictRedoNum  int

	confirmStage2Chan     chan int
	stopConfirmStage2Chan chan struct{}
	txReqExecuteRecord    map[int]int
	txReqExecuteCount     int
	inConfirmStage2       bool
	targetStage2Count     int
	nextStage2TxIndex     int
	delayGasFee           bool

	commonTxs         []*types.Transaction
	receipts          types.Receipts
	error             error
	resultMutex       sync.RWMutex
	resultProcessChan chan *ResultHandleEnv
	resultAppendChan  chan int
	parallelDBManager *state.ParallelDBManager
	trustDAG          bool
}

func newParallelStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine, parallelNum int, trustDAG bool) *ParallelStateProcessor {
	processor := &ParallelStateProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		parallelNum:    parallelNum,
		trustDAG:       trustDAG,
	}
	processor.init()
	return processor
}

type MergedTxInfo struct {
	slotDB              *state.StateDB
	StateObjectSuicided map[common.Address]struct{}
	StateChangeSet      map[common.Address]state.StateKeys
	BalanceChangeSet    map[common.Address]struct{}
	CodeChangeSet       map[common.Address]struct{}
	AddrStateChangeSet  map[common.Address]struct{}
	txIndex             int
}

type SlotState struct {
	pendingTxReqList  []*ParallelTxRequest
	primaryWakeUpChan chan struct{}
	shadowWakeUpChan  chan struct{}
	primaryStopChan   chan struct{}
	shadowStopChan    chan struct{}
	activatedType     int32 // 0: primary slot, 1: shadow slot
}

type ParallelTxResult struct {
	executedIndex int32 // record the current execute number of the tx
	slotIndex     int
	txReq         *ParallelTxRequest
	receipt       *types.Receipt
	slotDB        *state.ParallelStateDB
	gpSlot        *GasPool
	evm           *vm.EVM
	result        *ExecutionResult
	originalNonce *uint64
	err           error
}

type ParallelTxRequest struct {
	txIndex         int
	baseStateDB     *state.StateDB
	staticSlotIndex int
	tx              *types.Transaction
	gasLimit        uint64
	msg             *Message
	block           *types.Block
	vmConfig        vm.Config
	curTxChan       chan int
	runnable        int32 // 0: not runnable 1: runnable - can be scheduled
	executedNum     atomic.Int32
	conflictIndex   atomic.Int32 // the conflicted mainDB index, the txs will not be executed before this number
	useDAG          bool
}

// init to initialize and start the execution goroutines
func (p *ParallelStateProcessor) init() {
	log.Info("Parallel execution mode is enabled", "Parallel Num", p.parallelNum,
		"CPUNum", runtime.NumCPU(), "trustDAG", p.trustDAG)
	p.txResultChan = make(chan *ParallelTxResult, 10000)
	p.stopSlotChan = make(chan struct{}, 1)
	p.stopConfirmChan = make(chan struct{}, 1)
	p.stopConfirmStage2Chan = make(chan struct{}, 1)

	p.resultProcessChan = make(chan *ResultHandleEnv, 1)
	p.resultAppendChan = make(chan int, 10000)

	p.slotState = make([]*SlotState, p.parallelNum)

	p.parallelDBManager = state.NewParallelDBManager(20000, state.NewEmptySlotDB)

	quickMergeNum := 0 // p.parallelNum / 2
	for i := 0; i < p.parallelNum-quickMergeNum; i++ {
		p.slotState[i] = &SlotState{
			primaryWakeUpChan: make(chan struct{}, 1),
			shadowWakeUpChan:  make(chan struct{}, 1),
			primaryStopChan:   make(chan struct{}, 1),
			shadowStopChan:    make(chan struct{}, 1),
		}
		// start the primary slot's goroutine
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, parallelPrimarySlot) // this loop will be permanent live
		}(i)

		// start the shadow slot.
		// It is back up of the primary slot to make sure transaction can be redone ASAP,
		// since the primary slot could be busy at executing another transaction
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, parallelShadowSlot)
		}(i)
	}

	for i := p.parallelNum - quickMergeNum; i < p.parallelNum; i++ {
		// init a quick merge slot
		p.slotState[i] = &SlotState{
			primaryWakeUpChan: make(chan struct{}, 1),
			shadowWakeUpChan:  make(chan struct{}, 1),
			primaryStopChan:   make(chan struct{}, 1),
			shadowStopChan:    make(chan struct{}, 1),
		}
		go func(slotIndex int) {
			p.runQuickMergeSlotLoop(slotIndex, parallelPrimarySlot)
		}(i)
		go func(slotIndex int) {
			p.runQuickMergeSlotLoop(slotIndex, parallelShadowSlot)
		}(i)
	}

	p.confirmStage2Chan = make(chan int, 10)
	go func() {
		p.runConfirmStage2Loop()
	}()

	go func() {
		p.handlePendingResultLoop()
	}()

}

// resetState clear slot state for each block.
func (p *ParallelStateProcessor) resetState(txNum int, statedb *state.StateDB) {
	if txNum == 0 {
		return
	}
	p.mergedTxIndex.Store(-1)
	p.mergedTxCount.Store(0)
	p.debugConflictRedoNum = 0
	p.inConfirmStage2 = false

	statedb.PrepareForParallel()
	p.allTxReqs = make([]*ParallelTxRequest, txNum)

	for _, slot := range p.slotState {
		slot.pendingTxReqList = make([]*ParallelTxRequest, 0)
		slot.activatedType = parallelPrimarySlot
	}
	p.unconfirmedResults = new(sync.Map)
	p.unconfirmedDBs = new(sync.Map)
	p.slotDBsToRelease = new(sync.Map)
	p.pendingConfirmResults = new(sync.Map)
	p.txReqExecuteRecord = make(map[int]int, txNum)
	p.txReqExecuteCount = 0
	p.nextStage2TxIndex = 0
}

// Benefits of StaticDispatch:
//
//	** try best to make Txs with same From() in same slot
//	** reduce IPC cost by dispatch in Unit
//	** make sure same From in same slot
//	** try to make it balanced, queue to the most hungry slot for new Address
func (p *ParallelStateProcessor) doStaticDispatch(txReqs []*ParallelTxRequest) {
	fromSlotMap := make(map[common.Address]int, 100)
	toSlotMap := make(map[common.Address]int, 100)
	for _, txReq := range txReqs {
		var slotIndex = -1
		if i, ok := fromSlotMap[txReq.msg.From]; ok {
			// first: same From goes to same slot
			slotIndex = i
		} else if txReq.msg.To != nil {
			// To Address, with txIndex sorted, could be in different slot.
			if i, ok := toSlotMap[*txReq.msg.To]; ok {
				slotIndex = i
			}
		}

		// not found, dispatch to most hungry slot
		if slotIndex == -1 {
			slotIndex = p.mostHungrySlot()
		}
		// update
		fromSlotMap[txReq.msg.From] = slotIndex
		if txReq.msg.To != nil {
			toSlotMap[*txReq.msg.To] = slotIndex
		}

		slot := p.slotState[slotIndex]
		txReq.staticSlotIndex = slotIndex // txReq is better to be executed in this slot
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
	}
}

func (p *ParallelStateProcessor) mostHungrySlot() int {
	var (
		workload  = len(p.slotState[0].pendingTxReqList)
		slotIndex = 0
	)
	for i, slot := range p.slotState { // can start from index 1
		if len(slot.pendingTxReqList) < workload {
			slotIndex = i
			workload = len(slot.pendingTxReqList)
		}
		// just return the first slot with 0 workload
		if workload == 0 {
			return slotIndex
		}
	}
	return slotIndex
}

// hasConflict conducts conflict check
func (p *ParallelStateProcessor) hasConflict(txResult *ParallelTxResult, isStage2 bool) bool {
	slotDB := txResult.slotDB

	if p.trustDAG && slotDB.UseDAG() {
		// skip conflict check
		return false
	}

	if txResult.err != nil {
		log.Info("HasConflict due to err", "err", txResult.err)
		return true
	} else if slotDB.NeedsRedo() {
		log.Info("HasConflict needsRedo")
		// if there is any reason that indicates this transaction needs to redo, skip the conflict check
		return true
	} else {
		// check whether the slot db reads during execution are correct.
		if !slotDB.IsParallelReadsValid(isStage2) {
			return true
		}
	}
	return false
}

func (p *ParallelStateProcessor) switchSlot(slotIndex int) {
	slot := p.slotState[slotIndex]
	if atomic.CompareAndSwapInt32(&slot.activatedType, parallelPrimarySlot, parallelShadowSlot) {
		// switch from normal to shadow slot
		if len(slot.shadowWakeUpChan) == 0 {
			slot.shadowWakeUpChan <- struct{}{}
		}
	} else if atomic.CompareAndSwapInt32(&slot.activatedType, parallelShadowSlot, parallelPrimarySlot) {
		// switch from shadow to normal slot
		if len(slot.primaryWakeUpChan) == 0 {
			slot.primaryWakeUpChan <- struct{}{}
		}
	}
}

// executeInSlot do tx execution with thread local slot.
func (p *ParallelStateProcessor) executeInSlot(slotIndex int, txReq *ParallelTxRequest) *ParallelTxResult {
	mIndex := p.mergedTxIndex.Load()
	conflictIndex := txReq.conflictIndex.Load()
	log.Debug("executeInSlot", "txIndex", txReq.txIndex, "conflictIndex", conflictIndex, "mIndex", mIndex)
	if mIndex < conflictIndex {
		// The conflicted TX has not been finished executing, skip.
		// the transaction failed at check(nonce or balance), actually it has not been executed yet.
		atomic.CompareAndSwapInt32(&txReq.runnable, 0, 1)
		return nil
	}
	execNum := txReq.executedNum.Add(1)
	slotDB := state.NewSlotDB(txReq.baseStateDB, txReq.txIndex, int(mIndex), p.parallelDBManager, p.unconfirmedDBs, txReq.useDAG)
	blockContext := NewEVMBlockContext(txReq.block.Header(), p.bc, nil, p.config, slotDB) // can share blockContext within a block for efficiency
	txContext := NewEVMTxContext(txReq.msg)
	vmenv := vm.NewEVM(blockContext, txContext, slotDB, p.config, txReq.vmConfig)

	rules := p.config.Rules(txReq.block.Number(), blockContext.Random != nil, blockContext.Time)
	slotDB.Prepare(rules, txReq.msg.From, vmenv.Context.Coinbase, txReq.msg.To, vm.ActivePrecompiles(rules), txReq.msg.AccessList)

	// gasLimit not accurate, but it is ok for block import.
	// each slot would use its own gas pool, and will do gas limit check later
	gpSlot := new(GasPool).AddGas(txReq.gasLimit) // block.GasLimit()

	on := txReq.tx.Nonce()
	if txReq.msg.IsDepositTx && p.config.IsOptimismRegolith(vmenv.Context.Time) {
		on = slotDB.GetNonce(txReq.msg.From)
	}

	slotDB.SetTxContext(txReq.tx.Hash(), txReq.txIndex)
	evm, result, err := applyTransactionStageExecution(txReq.msg, gpSlot, slotDB, vmenv, p.delayGasFee)
	txResult := ParallelTxResult{
		executedIndex: execNum,
		slotIndex:     slotIndex,
		txReq:         txReq,
		receipt:       nil,
		slotDB:        slotDB,
		err:           err,
		gpSlot:        gpSlot,
		evm:           evm,
		result:        result,
		originalNonce: &on,
	}

	if err == nil {
		p.unconfirmedDBs.Store(txReq.txIndex, slotDB)
	} else {
		// the transaction failed at check(nonce or balance), actually it has not been executed yet.
		// the error here can be either expected or unexpected.
		// expected - the execution is correct and the error is normal result
		// unexpected -  the execution is incorrectly accessed the state because of parallelization.
		// In both case, rerun with next version of stateDB, it is a waste and buggy to rerun with same
		// version of stateDB that has been marked conflict.
		// Therefore, treat it as conflict and rerun, leave the result to conflict check.
		// Load conflict as it maybe updated by conflict checker or other execution slots.
		// use old mIndex so that we can try the new one that is updated by other thread of merging
		// during execution.
		conflictIndex = txReq.conflictIndex.Load()
		if conflictIndex < mIndex {
			if txReq.conflictIndex.CompareAndSwap(conflictIndex, mIndex) {
				log.Debug(fmt.Sprintf("Update conflictIndex in execution because of error: %s, mIndex: %d, new conflictIndex: %d, txIndex: %d",
					err.Error(), mIndex, txReq.conflictIndex.Load(), txReq.txIndex))
				atomic.CompareAndSwapInt32(&txReq.runnable, 0, 1)
				// the error could be caused by unconfirmed balance reference,
				// the balance could insufficient to pay its gas limit, which cause it preCheck.buyGas() failed
				// redo could solve it.
				log.Debug("In slot execution error", "error", err,
					"slotIndex", slotIndex, "txIndex", txReq.txIndex,
					"conflictIndex", conflictIndex, "baseIndex", txResult.slotDB.BaseTxIndex(),
					"mIndex", mIndex)
			}
		}
	}
	log.Debug("executeInSlot - store unconfirmedResults", "slotIndex", slotIndex, "txIndex", txReq.txIndex, "conflictIndex", conflictIndex,
		"baseIndex", txResult.slotDB.BaseTxIndex())
	p.unconfirmedResults.Store(txReq.txIndex, &txResult)
	return &txResult
}

// toConfirmTxIndex confirm a serial TxResults with same txIndex
func (p *ParallelStateProcessor) toConfirmTxIndex(targetTxIndex int, resultToMerge *ParallelTxResult, isStage2 bool) *ParallelTxResult {
	if isStage2 {
		if p.trustDAG {
			return nil
		}
		if targetTxIndex <= int(p.mergedTxIndex.Load())+1 {
			// `p.mergedTxIndex+1` is the one to be merged,
			// in stage2, we do likely conflict check, for these not their turn.
			return nil
		}
	}

	for {
		// handle a targetTxIndex in a loop
		targetResult := resultToMerge
		if isStage2 {
			result, ok := p.unconfirmedResults.Load(targetTxIndex)
			if !ok {
				return nil
			}
			targetResult = result.(*ParallelTxResult)

			// in stage 2, don't schedule a new redo if the TxReq is:
			//  a.runnable: it will be redone
			//  b.running: the new result will be more reliable, we skip check right now
			if atomic.LoadInt32(&targetResult.txReq.runnable) == 1 {
				return nil
			}
			if targetResult.executedIndex < targetResult.txReq.executedNum.Load() {
				// skip the intermediate result that is not the latest.
				return nil
			}
		}

		valid := p.toConfirmTxIndexResult(targetResult, isStage2)
		if !valid {
			staticSlotIndex := targetResult.txReq.staticSlotIndex
			conflictBase := targetResult.slotDB.BaseTxIndex()
			conflictIndex := targetResult.txReq.conflictIndex.Load()
			log.Debug("toConfirmTxIndexResult is not valid", "conflictBase", conflictBase, "conflictIndex", conflictIndex)
			if conflictIndex < int32(conflictBase) {
				if targetResult.txReq.conflictIndex.CompareAndSwap(conflictIndex, int32(conflictBase)) {
					log.Debug("Update conflict index", "conflictIndex", conflictIndex, "conflictBase", conflictBase, "stage2?", isStage2)
				}
			}
			if isStage2 {
				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt the slot's current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
				return nil
			}
			r, ok := p.pendingConfirmResults.LoadAndDelete(targetTxIndex)
			if !ok {
				// this is the last result to check, and it is not valid
				// This means that the tx has been executed with error
				if p.trustDAG && targetResult.slotDB.UseDAG() {
					// with trustDAG, treat execution error as normal error.
					if targetResult.err != nil {
						log.Debug(fmt.Sprintf("Parallel execution exited with error!, txIndex:%d, err: %v\n", targetResult.txReq.txIndex, targetResult.err))
						return targetResult
					}
				}
				if targetResult.txReq.txIndex == int(p.mergedTxIndex.Load())+1 &&
					targetResult.slotDB.BaseTxIndex() >= int(p.mergedTxIndex.Load()) {
					if targetResult.err != nil {
						log.Debug(fmt.Sprintf("Parallel execution exited with error!, txIndex:%d, err: %v\n", targetResult.txReq.txIndex, targetResult.err))
						return targetResult
					} else {
						// abnormal exit with conflict error, need check the parallel algorithm
						targetResult.err = ErrParallelUnexpectedConflict
						log.Debug(fmt.Sprintf("Parallel execution exited unexpected conflict, txIndex:%d\n", targetResult.txReq.txIndex))
						return targetResult
					}
				}

				atomic.CompareAndSwapInt32(&targetResult.txReq.runnable, 0, 1) // needs redo
				p.debugConflictRedoNum++
				// interrupt its current routine, and switch to the other routine
				p.switchSlot(staticSlotIndex)
				// reclaim the result.
				p.slotDBsToRelease.Store(targetResult.slotDB, targetResult.slotDB)
				return nil
			}
			resultToMerge = r.(*ParallelTxResult)
			continue
		}
		if isStage2 {
			// likely valid, but not sure, can not deliver
			return nil
		}
		return targetResult
	}
}

// to confirm one txResult, return true if the result is valid
// if it is in Stage 2 it is a likely result, not 100% sure
func (p *ParallelStateProcessor) toConfirmTxIndexResult(txResult *ParallelTxResult, isStage2 bool) bool {
	txReq := txResult.txReq
	if p.hasConflict(txResult, isStage2) {
		log.Info(fmt.Sprintf("HasConflict!! block: %d, txIndex: %d, isStage2: %v",
			txResult.txReq.block.NumberU64(), txResult.txReq.txIndex, isStage2))
		return false
	}
	if isStage2 { // not its turn
		return true // likely valid, not sure, not finalized right now.
	}

	// goroutine unsafe operation will be handled from here for safety
	gasConsumed := txReq.gasLimit - txResult.gpSlot.Gas()
	if gasConsumed != txResult.result.UsedGas {
		log.Error("gasConsumed != result.UsedGas mismatch",
			"gasConsumed", gasConsumed, "result.UsedGas", txResult.result.UsedGas)
	}

	// ok, time to do finalize, stage2 should not be parallel
	txResult.receipt, txResult.err = applyTransactionStageFinalization(txResult.evm, txResult.result,
		*txReq.msg, p.config, txResult.slotDB, txReq.block,
		txReq.tx, txResult.originalNonce)
	return true
}

func (p *ParallelStateProcessor) runSlotLoop(slotIndex int, slotType int32) {
	curSlot := p.slotState[slotIndex]
	var wakeupChan chan struct{}
	var stopChan chan struct{}

	if slotType == parallelPrimarySlot {
		wakeupChan = curSlot.primaryWakeUpChan
		stopChan = curSlot.primaryStopChan
	} else {
		wakeupChan = curSlot.shadowWakeUpChan
		stopChan = curSlot.shadowStopChan
	}

	lastStartPos := 0
	for {
		select {
		case <-stopChan:
			p.stopSlotChan <- struct{}{}
			continue
		case <-wakeupChan:
		}

		interrupted := false

		for i := lastStartPos; i < len(curSlot.pendingTxReqList); i++ {
			// for i, txReq := range curSlot.pendingTxReqList {
			txReq := curSlot.pendingTxReqList[i]
			if txReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}
			lastStartPos = i

			if txReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}

			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				break
			}

			// first try next to be merged req.
			nextIdx := p.mergedTxIndex.Load() + 1
			if nextIdx < int32(len(p.allTxReqs)) {
				nextMergeReq := p.allTxReqs[nextIdx]
				if nextMergeReq.runnable == 1 {
					if atomic.CompareAndSwapInt32(&nextMergeReq.runnable, 1, 0) {
						// execute.
						res := p.executeInSlot(slotIndex, nextMergeReq)
						if res != nil {
							p.txResultChan <- res
						}
					}
				}
			}

			if txReq.runnable == 1 {
				// try the next req in loop sequence.
				if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, txReq)
				if res == nil {
					continue
				}
				p.txResultChan <- res
			}
		}
		// switched to the other slot.
		if interrupted {
			continue
		}

		// txReq in this Slot have all been executed, try steal one from other slot.
		// as long as the TxReq is runnable, we steal it, mark it as stolen

		for j := int(p.mergedTxIndex.Load()) + 1; j < len(p.allTxReqs); j++ {
			stealTxReq := p.allTxReqs[j]
			if stealTxReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}

			if stealTxReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}

			if atomic.LoadInt32(&curSlot.activatedType) != slotType {
				interrupted = true
				break
			}

			// first try next to be merged req.
			nextIdx := p.mergedTxIndex.Load() + 1
			if nextIdx < int32(len(p.allTxReqs)) {
				nextMergeReq := p.allTxReqs[nextIdx]
				if nextMergeReq.runnable == 1 {
					if atomic.CompareAndSwapInt32(&nextMergeReq.runnable, 1, 0) {
						// execute.
						res := p.executeInSlot(slotIndex, nextMergeReq)
						if res != nil {
							p.txResultChan <- res
						}
					}
				}
			}

			if stealTxReq.runnable == 1 {
				if !atomic.CompareAndSwapInt32(&stealTxReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, stealTxReq)
				if res == nil {
					continue
				}
				p.txResultChan <- res
			}
		}
	}
}

func (p *ParallelStateProcessor) runQuickMergeSlotLoop(slotIndex int, slotType int32) {
	curSlot := p.slotState[slotIndex]
	var wakeupChan chan struct{}
	var stopChan chan struct{}

	if slotType == parallelPrimarySlot {
		wakeupChan = curSlot.primaryWakeUpChan
		stopChan = curSlot.primaryStopChan
	} else {
		wakeupChan = curSlot.shadowWakeUpChan
		stopChan = curSlot.shadowStopChan
	}
	for {
		select {
		case <-stopChan:
			p.stopSlotChan <- struct{}{}
			continue
		case <-wakeupChan:
		}

		next := int(p.mergedTxIndex.Load()) + 1

		executed := 5
		for i := next; i < len(p.allTxReqs); i++ {
			txReq := p.allTxReqs[next]
			if executed == 0 {
				break
			}
			if txReq.txIndex <= int(p.mergedTxIndex.Load()) {
				continue
			}
			if txReq.conflictIndex.Load() > p.mergedTxIndex.Load() {
				break
			}
			if txReq.runnable == 1 {
				if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
					continue
				}
				res := p.executeInSlot(slotIndex, txReq)
				if res != nil {
					executed--
					p.txResultChan <- res
				}
			}
		}
	}
}

func (p *ParallelStateProcessor) runConfirmStage2Loop() {
	for {
		select {
		case <-p.stopConfirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				<-p.confirmStage2Chan
			}
			p.stopSlotChan <- struct{}{}
			continue
		case <-p.confirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				<-p.confirmStage2Chan // drain the chan to get the latest merged txIndex
			}
		}
		// stage 2,if all tx have been executed at least once, and its result has been received.
		// in Stage 2, we will run check when merge is advanced.
		// more aggressive tx result confirm, even for these Txs not in turn
		startTxIndex := int(p.mergedTxIndex.Load()) + 2 // stage 2's will start from the next target merge index
		endTxIndex := startTxIndex + stage2CheckNumber
		txSize := len(p.allTxReqs)
		if endTxIndex > (txSize - 1) {
			endTxIndex = txSize - 1
		}
		log.Debug("runConfirmStage2Loop", "startTxIndex", startTxIndex, "endTxIndex", endTxIndex)
		for txIndex := startTxIndex; txIndex < endTxIndex; txIndex++ {
			p.toConfirmTxIndex(txIndex, nil, true)
		}
		// make sure all slots are wake up
		for i := 0; i < p.parallelNum; i++ {
			p.switchSlot(i)
		}
	}
}

func (p *ParallelStateProcessor) handleTxResults(index int, resultToMerge *ParallelTxResult) *ParallelTxResult {
	confirmedResult := p.toConfirmTxIndex(index, resultToMerge, false)
	if confirmedResult == nil {
		return nil
	}
	// schedule stage 2 when new Tx has been merged, schedule once and ASAP
	// stage 2,if all tx have been executed at least once, and its result has been received.
	// in Stage 2, we will run check when main DB is advanced, i.e., new Tx result has been merged.
	if !p.trustDAG {
		if p.inConfirmStage2 && int(p.mergedTxIndex.Load()) >= p.nextStage2TxIndex {
			p.nextStage2TxIndex = int(p.mergedTxIndex.Load()) + stage2CheckNumber
			p.confirmStage2Chan <- int(p.mergedTxIndex.Load())
		}
	}
	return confirmedResult
}

// wait until the next Tx is executed and its result is merged to the main stateDB
func (p *ParallelStateProcessor) confirmTxResults(statedb *state.StateDB, gp *GasPool, index int,
	resultToMerge *ParallelTxResult, CumulativeGasUsed *uint64) *ParallelTxResult {
	result := p.handleTxResults(index, resultToMerge)
	if result == nil {
		return nil
	}
	// ok, the tx result is valid and can be merged
	if result.err != nil {
		return result
	}

	if err := gp.SubGas(result.receipt.GasUsed); err != nil {
		log.Error("gas limit reached", "block", result.txReq.block.Number(),
			"txIndex", result.txReq.txIndex, "GasUsed", result.receipt.GasUsed, "gp.Gas", gp.Gas())
	}

	resultTxIndex := result.txReq.txIndex

	var root []byte
	header := result.txReq.block.Header()

	isByzantium := p.config.IsByzantium(header.Number)
	isEIP158 := p.config.IsEIP158(header.Number)
	result.slotDB.FinaliseForParallel(isByzantium || isEIP158, statedb)

	// merge slotDB into mainDB
	statedb.MergeSlotDB(result.slotDB, result.receipt, resultTxIndex, result.result.delayFees)

	delayGasFee := result.result.delayFees
	// add delayed gas fee
	if delayGasFee != nil {
		if delayGasFee.TipFee != nil {
			statedb.AddBalance(delayGasFee.Coinbase, delayGasFee.TipFee)
		}
		if delayGasFee.BaseFee != nil {
			statedb.AddBalance(params.OptimismBaseFeeRecipient, delayGasFee.BaseFee)
		}
		if delayGasFee.L1Fee != nil {
			statedb.AddBalance(params.OptimismL1FeeRecipient, delayGasFee.L1Fee)
		}
	}

	if !p.trustDAG {
		slotReceipt := result.receipt
		thash := result.txReq.tx.Hash()
		statedb.SetTxContext(thash, result.txReq.txIndex)
		// receipt.Logs use unified log index within a block
		// align slotDB's log index to the block stateDB's logSize
		for _, l := range slotReceipt.Logs {
			statedb.AddLog(l)
		}

		*CumulativeGasUsed += slotReceipt.GasUsed
		result.receipt.CumulativeGasUsed = *CumulativeGasUsed
		// Do IntermediateRoot after mergeSlotDB.
		if !isByzantium {
			root = statedb.IntermediateRoot(isEIP158).Bytes()
		}
		result.receipt.PostState = root

		if resultTxIndex != int(p.mergedTxIndex.Load())+1 {
			log.Error("ProcessParallel tx result out of order", "resultTxIndex", resultTxIndex,
				"p.mergedTxIndex", p.mergedTxIndex.Load())
		}
		p.mergedTxIndex.Store(int32(resultTxIndex))
	} else {
		if !isByzantium {
			log.Error("ProcessParallel tx result out of order is not supported for non-Byzantium")
			os.Exit(-1)
		}

		if resultTxIndex == int(p.mergedTxIndex.Load())+1 {
			// the receipt logs will be updated in the loop of pendingResultHandleLoop method. so skip here.
			receipt := result.receipt
			statedb.SetTxContext(receipt.TxHash, resultTxIndex)
			for _, l := range receipt.Logs {
				statedb.AddLog(l)
			}
			*CumulativeGasUsed += receipt.GasUsed
			log.Debug("ProcessParallel tx result out of order", "resultTxIndex", resultTxIndex, "CumulativeGasUsed", *CumulativeGasUsed)
			result.receipt.CumulativeGasUsed = *CumulativeGasUsed
			p.mergedTxIndex.Store(int32(resultTxIndex))
		}
		p.mergedTxCount.Add(1)
	}

	// trigger all slot to run left conflicted txs
	for _, slot := range p.slotState {
		var wakeupChan chan struct{}
		if slot.activatedType == parallelPrimarySlot {
			wakeupChan = slot.primaryWakeUpChan
		} else {
			wakeupChan = slot.shadowWakeUpChan
		}
		select {
		case wakeupChan <- struct{}{}:
		default:
		}
	}
	// schedule prefetch once only when unconfirmedResult is valid
	if result.err == nil {
		if _, ok := p.txReqExecuteRecord[resultTxIndex]; !ok {
			p.txReqExecuteRecord[resultTxIndex] = 0
			p.txReqExecuteCount++
			statedb.AddrPrefetch(result.slotDB)
			if !p.inConfirmStage2 && p.txReqExecuteCount == p.targetStage2Count {
				p.inConfirmStage2 = true
			}
		}
		p.txReqExecuteRecord[resultTxIndex]++
	}
	// after merge, the slotDB will not accessible, reclaim the resource
	p.slotDBsToRelease.Store(result.slotDB, result.slotDB)
	return result
}

func (p *ParallelStateProcessor) doCleanUp() {
	// 1.clean up all slot: primary and shadow, to make sure they are stopped
	for _, slot := range p.slotState {
		slot.primaryStopChan <- struct{}{}
		slot.shadowStopChan <- struct{}{}
		<-p.stopSlotChan
		<-p.stopSlotChan
	}
	// 2.discard delayed txResults if any
	for {
		if len(p.txResultChan) > 0 {
			<-p.txResultChan
			continue
		}
		break
	}
	// 3.make sure the confirmation routine is stopped
	p.stopConfirmStage2Chan <- struct{}{}
	<-p.stopSlotChan

	p.unconfirmedResults = nil
	p.unconfirmedDBs = nil
	p.pendingConfirmResults = nil

	go func() {
		p.slotDBsToRelease.Range(func(key, value any) bool {
			sdb := value.(*state.ParallelStateDB)
			sdb.PutSyncPool(p.parallelDBManager)
			return true
		})
	}()
}

// Process implements BEP-130 Parallel Transaction Execution
func (p *ParallelStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		usedGas = new(uint64)
		header  = block.Header()
		gp      = new(GasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	if p.config.PreContractForkBlock != nil && p.config.PreContractForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyPreContractHardFork(statedb)
	}

	misc.EnsureCreate2Deployer(p.config, block.Time(), statedb)

	allTxs := block.Transactions()
	p.resetState(len(allTxs), statedb)

	var (
		// with parallel mode, vmenv will be created inside of slot
		blockContext = NewEVMBlockContext(block.Header(), p.bc, nil, p.config, statedb)
		vmenv        = vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
		signer       = types.MakeSigner(p.bc.chainConfig, block.Number(), block.Time())
	)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}
	statedb.MarkFullProcessed()
	txDAG := cfg.TxDAG

	txNum := len(allTxs)
	latestExcludedTx := -1
	// Iterate over and process the individual transactions
	p.commonTxs = make([]*types.Transaction, txNum)
	p.receipts = make([]*types.Receipt, txNum)

	parallelNum := p.parallelNum

	if txNum > parallelNum*2 && txNum >= 4 {
		var wg sync.WaitGroup
		errChan := make(chan error)

		begin := 0
		// first try to find latestExcludeTx, as for opBNB, they are the first consecutive txs.
		for idx := 0; idx < len(allTxs); idx++ {
			if txDAG != nil && txDAG.TxDep(idx).CheckFlag(types.ExcludedTxFlag) {
				if err := p.transferTxs(allTxs, idx, signer, block, statedb, cfg, latestExcludedTx); err != nil {
					return nil, nil, 0, err
				}
				latestExcludedTx = idx
			} else {
				begin = idx
				break
			}
		}

		// Create a cancelable context
		ctx, cancel := context.WithCancel(context.Background())

		// Create a pool of workers
		transactionsPerWorker := (len(allTxs) - begin) / parallelNum

		// Create a pool of workers
		for i := 0; i < parallelNum; i++ {
			wg.Add(1)
			go func(start, end int, signer types.Signer, blk *types.Block, sdb *state.StateDB, cfg vm.Config, usedGas *uint64) {
				defer wg.Done()
				for j := start; j < end; j++ {
					select {
					case <-ctx.Done():
						return // Exit the goroutine if the context is canceled
					default:
						if err := p.transferTxs(allTxs, j, signer, block, statedb, cfg, latestExcludedTx); err != nil {
							errChan <- err
							cancel() // Cancel the context to stop other goroutines
							return
						}
					}
				}
			}(begin+i*transactionsPerWorker, begin+(i+1)*transactionsPerWorker, signer, block, statedb, cfg, usedGas)
		}

		// Distribute any remaining transactions
		for i := begin + parallelNum*transactionsPerWorker; i < len(allTxs); i++ {
			if err := p.transferTxs(allTxs, i, signer, block, statedb, cfg, latestExcludedTx); err != nil {
				errChan <- err
				cancel() // Cancel the context to stop other goroutines
			}
		}

		// Wait for all workers to finish and handle errors
		go func() {
			wg.Wait()
			close(errChan)
		}()

		for err := range errChan {
			return nil, nil, 0, err
		}
		//
	} else {
		for i, tx := range allTxs {
			msg, err := TransactionToMessage(tx, signer, header.BaseFee)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
			}

			// find the latestDepTx from TxDAG or latestExcludedTx
			latestDepTx := -1
			if dep := types.TxDependency(txDAG, i); len(dep) > 0 {
				latestDepTx = int(dep[len(dep)-1])
			}
			if latestDepTx < latestExcludedTx {
				latestDepTx = latestExcludedTx
			}

			// parallel start, wrap an exec message, which will be dispatched to a slot
			txReq := &ParallelTxRequest{
				txIndex:         i,
				baseStateDB:     statedb,
				staticSlotIndex: -1,
				tx:              tx,
				gasLimit:        block.GasLimit(), // gp.Gas().
				msg:             msg,
				block:           block,
				vmConfig:        cfg,
				curTxChan:       make(chan int, 1),
				runnable:        1, // 0: not runnable, 1: runnable
				useDAG:          txDAG != nil,
			}
			txReq.executedNum.Store(0)
			txReq.conflictIndex.Store(-2)
			if latestDepTx >= 0 {
				txReq.conflictIndex.Store(int32(latestDepTx))
			}
			p.allTxReqs[i] = txReq
			if txDAG != nil && txDAG.TxDep(i).CheckFlag(types.ExcludedTxFlag) {
				latestExcludedTx = i
			}
		}
	}
	allTxCount := len(p.allTxReqs)
	// set up stage2 enter criteria
	p.targetStage2Count = allTxCount
	if p.targetStage2Count > 50 {
		// usually, the last Tx could be the bottleneck it could be very slow,
		// so it is better for us to enter stage 2 a bit earlier
		p.targetStage2Count = p.targetStage2Count - stage2AheadNum
	}

	p.delayGasFee = true
	p.doStaticDispatch(p.allTxReqs)
	if txDAG != nil && txDAG.DelayGasFeeDistribution() {
		p.delayGasFee = true
	}

	// after static dispatch, we notify the slot to work.
	for _, slot := range p.slotState {
		slot.primaryWakeUpChan <- struct{}{}
	}

	// ======================= Result handling ================= //
	// kick off the result handler.
	isByzantium := p.config.IsByzantium(header.Number)
	p.resultProcessChan <- &ResultHandleEnv{
		statedb:     statedb,
		gp:          gp,
		txCount:     allTxCount,
		isByzantium: isByzantium,
	}
	for {
		log.Debug("Process ResultSendingLoop", "mergedIndex", p.mergedTxIndex.Load())
		if allTxCount == 0 {
			// put it ahead of chan receive to avoid waiting for empty block
			break
		}
		// wait for execute result or the merged all signal.
		unconfirmedResult := <-p.txResultChan
		log.Debug("Process get unconfirmedResult", "unconfirmedResult.txReq", unconfirmedResult.txReq, "pmergedIndex", p.mergedTxIndex.Load())
		if unconfirmedResult.txReq == nil {
			// all tx results are merged.
			if int(p.mergedTxIndex.Load())+1 == allTxCount {
				*usedGas = unconfirmedResult.result.UsedGas
				log.Debug("Process get nil Result", "All transactions merged", p.mergedTxIndex.Load())
			} else {
				log.Error("Process get nil Result", "result handling abort because of err", unconfirmedResult.err)
			}
			break
		}

		// if it is not byz, it requires root calculation.
		// if it doesn't trustDAG, it can not do OOO merge and have to do conflictCheck
		OutOfOrderMerge := isByzantium && p.trustDAG
		unconfirmedTxIndex := unconfirmedResult.txReq.txIndex

		if !OutOfOrderMerge {
			if unconfirmedTxIndex <= int(p.mergedTxIndex.Load()) {
				log.Debug("drop merged txReq", "unconfirmedTxIndex", unconfirmedTxIndex, "p.mergedTxIndex", p.mergedTxIndex.Load())
				continue
			}
		} else {
			if p.commonTxs[unconfirmedTxIndex] != nil {
				// unconfirmedTx already merged.
				continue
			}
		}
		// update pendingConfirmResult with the newest result.
		prevResult, ok := p.pendingConfirmResults.Load(unconfirmedTxIndex)
		if !ok || prevResult.(*ParallelTxResult).slotDB.BaseTxIndex() < unconfirmedResult.slotDB.BaseTxIndex() {
			p.pendingConfirmResults.Store(unconfirmedTxIndex, unconfirmedResult)
			// send to handler
			log.Debug("Send unconfirmTx to handler", "unconfirmedTxIndex", unconfirmedTxIndex)
			p.resultAppendChan <- unconfirmedTxIndex
		}
	}

	// ======================== All result merged ====================== //

	// clean up when the block is processed
	p.doCleanUp()

	if p.error != nil {
		return nil, nil, 0, p.error
	}

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	// all txs have been merged at this point, no need to acquire the lock of commonTxs
	log.Debug("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
		"txNum", txNum)

	if p.mergedTxIndex.Load() >= 0 && p.debugConflictRedoNum > 0 {
		log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
			"txNum", txNum,
			"conflictNum", p.debugConflictRedoNum,
			"redoRate(%)", 100*(p.debugConflictRedoNum)/txNum,
			"txDAG", txDAG != nil)
	}
	if metrics.EnabledExpensive {
		parallelTxNumMeter.Mark(int64(txNum))
		parallelConflictTxNumMeter.Mark(int64(p.debugConflictRedoNum))
	}

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Number(), block.Time()) {
		return nil, nil, 0, errors.New("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, allTxs, block.Uncles(), withdrawals)

	var allLogs []*types.Log
	var receipts []*types.Receipt
	for _, receipt := range p.receipts {
		if receipt == nil {
			// skip the one with err.
			continue
		}
		allLogs = append(allLogs, receipt.Logs...)
		receipts = append(receipts, receipt)
	}
	return receipts, allLogs, *usedGas, nil
}

func (p *ParallelStateProcessor) handlePendingResultLoop() {
	var info *ResultHandleEnv
	var stateDB *state.StateDB
	var gp *GasPool
	var txCount int
	var receivedTxIdx int
	var isByzantium bool
	var OutOfOrderMerge bool
	var CumulativeGasUsed uint64
	for {
		select {
		case info = <-p.resultProcessChan:
			stateDB = info.statedb
			gp = info.gp
			txCount = info.txCount
			isByzantium = info.isByzantium
			OutOfOrderMerge = p.trustDAG && isByzantium
			CumulativeGasUsed = 0
			log.Debug("handlePendingResult get Env", "stateDBTx", stateDB.TxIndex(), "gp", gp.String(), "txCount", txCount)
		case receivedTxIdx = <-p.resultAppendChan:
		}

		// if all merged, notify the main routine. continue to wait for next block.
		if p.mergedTxIndex.Load()+1 == int32(txCount) {
			log.Debug("handlePendingResult merged all")

			p.txResultChan <- &ParallelTxResult{txReq: nil, result: &ExecutionResult{UsedGas: CumulativeGasUsed}}
			// clear the pending chan.
			for len(p.resultAppendChan) > 0 {
				<-p.resultAppendChan
			}
			continue
		}

		// busy waiting.
		for {
			log.Debug("busy waiting for pending result", "mergedIndex", p.mergedTxIndex.Load(), "allTxCount", txCount)
			nextTxIndex := int(p.mergedTxIndex.Load()) + 1
			if OutOfOrderMerge {
				// skip those already merged.
				for {
					log.Debug("OOOMerge check loop", "nextTxIndex", nextTxIndex, "txCount", txCount)
					if nextTxIndex == txCount {
						// reach the last, update the mergedTxIndex if needed
						if p.mergedTxIndex.Load() != int32(nextTxIndex-1) {
							if p.receipts[nextTxIndex-1] == nil {
								// no receipts, there is an error at applyMessage.
								break
							}
							if p.receipts[nextTxIndex-1].CumulativeGasUsed == 0 {
								// update receipt
								receipt := p.receipts[nextTxIndex-1]
								stateDB.SetTxContext(receipt.TxHash, nextTxIndex-1)
								for _, l := range receipt.Logs {
									stateDB.AddLog(l)
								}
								CumulativeGasUsed += receipt.GasUsed
								log.Debug("update CumulativeGasUse txCount", "TxIndex", nextTxIndex-1, "CumulativeGasUsed", CumulativeGasUsed)

								p.receipts[nextTxIndex-1].CumulativeGasUsed = CumulativeGasUsed
							}
							p.mergedTxIndex.Store(int32(nextTxIndex - 1))
						}
						break
					}
					if nextTxIndex < txCount {
						if p.commonTxs[nextTxIndex] == nil {
							// not merged, do merge
							// update the mergedTxIndex to those already merged txs.
							p.mergedTxIndex.Store(int32(nextTxIndex - 1))
							// trigger all slot to run left conflicted txs
							for _, slot := range p.slotState {
								var wakeupChan chan struct{}
								if slot.activatedType == parallelPrimarySlot {
									wakeupChan = slot.primaryWakeUpChan
								} else {
									wakeupChan = slot.shadowWakeUpChan
								}
								select {
								case wakeupChan <- struct{}{}:
								default:
								}
							}
							break
						} else {
							// nextTxIndex already merged, update receipts
							// update receipt, and receipt can be nil if there is err in execution.
							receipt := p.receipts[nextTxIndex]
							if receipt != nil {
								stateDB.SetTxContext(receipt.TxHash, nextTxIndex)
								for _, l := range receipt.Logs {
									stateDB.AddLog(l)
								}
								CumulativeGasUsed += receipt.GasUsed
								log.Debug("update CumulativeGasUse already executed", "TxIndex", nextTxIndex, "CumulativeGasUsed", CumulativeGasUsed)
								p.receipts[nextTxIndex].CumulativeGasUsed = CumulativeGasUsed
							}
						}
					}
					nextTxIndex++
				}
			}

			log.Debug("handlePendingResult break from OOO Loop", "nextTxIndex", nextTxIndex, "txCount", txCount)
			// all merged.
			if nextTxIndex == txCount {
				log.Debug("handlePendingResult merged all in wait loop", "error", p.error,
					"nextTxIndex", nextTxIndex, "mergedIndex", p.mergedTxIndex.Load(), "CumulativeGasUsed", CumulativeGasUsed)
				p.txResultChan <- &ParallelTxResult{txReq: nil, result: &ExecutionResult{UsedGas: CumulativeGasUsed}}
				// clear the pending chan.
				for len(p.resultAppendChan) > 0 {
					<-p.resultAppendChan
				}
				break
			}

			nextToMergeIndex := nextTxIndex
			nextToMergeResult, ok := p.pendingConfirmResults.Load(nextTxIndex)
			if !ok {
				log.Debug("handlePendingResult - can not load next form pendingConfirmResult", "txIndex", nextTxIndex)
				if !OutOfOrderMerge {
					break
				}
				// we trust DAG. so try to see whether we can merge one.
				nextToMergeResult, ok = p.pendingConfirmResults.Load(receivedTxIdx)
				if !ok {
					log.Debug("handlePendingResult - can not load form pendingConfirmResult", "txIndex", receivedTxIdx)
					break
				}
				// try to see whether the dependency is already merged
				r := nextToMergeResult.(*ParallelTxResult)
				depIndex := r.txReq.conflictIndex.Load()
				if depIndex >= 0 {
					if p.commonTxs[depIndex] == nil {
						break
					}
				}
				nextToMergeIndex = receivedTxIdx
			}
			p.pendingConfirmResults.Delete(nextToMergeIndex)

			log.Debug("Start to check result", "TxIndex", int(nextToMergeIndex), "stateDBTx", stateDB.TxIndex(), "gp", gp.String())

			result := p.confirmTxResults(stateDB, gp, nextToMergeIndex, nextToMergeResult.(*ParallelTxResult), &CumulativeGasUsed)
			if result == nil {
				break
			} else {
				log.Debug("in Confirm Loop - after confirmTxResults",
					"mergedIndex", p.mergedTxIndex.Load(),
					"confirmedIndex", result.txReq.txIndex,
					"result.err", result.err)
			}
			p.resultMutex.Lock()
			// update tx result
			if result.err != nil {
				log.Debug("handlePendingResultLoop result has error",
					"txIndex", result.txReq.txIndex, "txBase", result.slotDB.BaseTxIndex(),
					"mergedIndex", p.mergedTxIndex.Load())
				if result.slotDB.BaseTxIndex() >= int(p.mergedTxIndex.Load()) || OutOfOrderMerge {
					// should skip the merge phase
					log.Error("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
						"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
					pos := result.txReq.txIndex
					p.commonTxs[pos] = result.txReq.tx
					p.receipts[pos] = nil
					p.resultMutex.Unlock()
					// update mergedTxIndex and go on.
					p.mergedTxIndex.Store(int32(result.txReq.txIndex))

					// trigger all slot to run left conflicted txs
					for _, slot := range p.slotState {
						var wakeupChan chan struct{}
						if slot.activatedType == parallelPrimarySlot {
							wakeupChan = slot.primaryWakeUpChan
						} else {
							wakeupChan = slot.shadowWakeUpChan
						}
						select {
						case wakeupChan <- struct{}{}:
						default:
						}
					}
					continue
				}
				break
			}
			pos := result.txReq.txIndex
			p.commonTxs[pos] = result.txReq.tx
			p.receipts[pos] = result.receipt
			p.resultMutex.Unlock()
		}
	}
}

func (p *ParallelStateProcessor) transferTxs(txs types.Transactions, i int, signer types.Signer, block *types.Block, statedb *state.StateDB, cfg vm.Config, latestExcludedTx int) error {
	if p.allTxReqs[i] != nil {
		return nil
	}
	tx := txs[i]
	txDAG := cfg.TxDAG
	msg, err := TransactionToMessage(tx, signer, block.Header().BaseFee)
	if err != nil {
		return fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
	}

	// find the latestDepTx from TxDAG or latestExcludedTx
	latestDepTx := -1
	if dep := types.TxDependency(txDAG, i); len(dep) > 0 {
		latestDepTx = int(dep[len(dep)-1])
	}
	if latestDepTx < latestExcludedTx {
		latestDepTx = latestExcludedTx
	}

	// parallel start, wrap an exec message, which will be dispatched to a slot
	txReq := &ParallelTxRequest{
		txIndex:         i,
		baseStateDB:     statedb,
		staticSlotIndex: -1,
		tx:              tx,
		gasLimit:        block.GasLimit(), // gp.Gas().
		msg:             msg,
		block:           block,
		vmConfig:        cfg,
		curTxChan:       make(chan int, 1),
		runnable:        1, // 0: not runnable, 1: runnable
		useDAG:          txDAG != nil,
	}
	txReq.executedNum.Store(0)
	txReq.conflictIndex.Store(-2)
	if latestDepTx >= 0 {
		txReq.conflictIndex.Store(int32(latestDepTx))
	}
	p.allTxReqs[i] = txReq
	return nil
}

func applyTransactionStageExecution(msg *Message, gp *GasPool, statedb *state.ParallelStateDB, evm *vm.EVM, delayGasFee bool) (*vm.EVM, *ExecutionResult, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	var (
		result *ExecutionResult
		err    error
	)
	if delayGasFee {
		result, err = ApplyMessageDelayGasFee(evm, msg, gp)
	} else {
		result, err = ApplyMessage(evm, msg, gp)
	}

	if err != nil {
		return nil, nil, err
	}

	return evm, result, err
}

func applyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg Message, config *params.ChainConfig,
	statedb *state.ParallelStateDB, block *types.Block, tx *types.Transaction, nonce *uint64) (*types.Receipt, error) {

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx.
	// Don't assign CumulativeGasUsed here as OOO Merge may get wrong value, leave it after merge.
	receipt := &types.Receipt{Type: tx.Type(), PostState: nil, CumulativeGasUsed: 0}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	if msg.IsDepositTx && config.IsOptimismRegolith(evm.Context.Time) {
		// The actual nonce for deposit transactions is only recorded from Regolith onwards and
		// otherwise must be nil.
		receipt.DepositNonce = nonce
		// The DepositReceiptVersion for deposit transactions is only recorded from Canyon onwards
		// and otherwise must be nil.
		if config.IsOptimismCanyon(evm.Context.Time) {
			receipt.DepositReceiptVersion = new(uint64)
			*receipt.DepositReceiptVersion = types.CanyonDepositReceiptVersion
		}
	}
	if tx.Type() == types.BlobTxType {
		receipt.BlobGasUsed = uint64(len(tx.BlobHashes()) * params.BlobTxBlobGasPerBlob)
		receipt.BlobGasPrice = evm.Context.BlobBaseFee
	}
	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, *nonce)
	}
	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), block.NumberU64(), block.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = block.Hash()
	receipt.BlockNumber = block.Number()
	receipt.TransactionIndex = uint(statedb.TxIndex())

	// Debug purpose
	// b, _ := receipt.MarshalJSON()
	// log.Debug("applyTransactionStageFinalization", "receipt", string(b))
	//
	return receipt, nil
}
