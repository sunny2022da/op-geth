package compiler

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"sync"
)

// CodeCacheGCThreshold TODO: make codecache threshold configurable.
const CodeCacheGCThreshold = 1024 * 1024 * 1024 /* 1 GB */
// CodeCacheGCSoftLimit is used to trigger GC for memory control.
// upper limit of bytecodes of smart contract is ~25MB.
const CodeCacheGCSoftLimit = 200 * 1024 * 1024 /* 200MB */

// preInstalledCode
var preInstalledAddr = common.HexToAddress("0x4200000000000000000000000000000000000015")
var preInstalledCode = common.Hex2Bytes("d880d0405260043610d7005ed060003560e01c80635c60da1b11d70043d080635c60da1b14d700bed080638f28397014d700f8d08063f851a44014d70118d0d6006dd05b80633659cfe614d70075d080634f1ef28614d70095d0d6006dd05b36d7006dd061006bd6012dd05b005b61006bd6012dd05b3480e2d00081d0db00d0fd5b5061006b610090366004d606d9d05bd60224d05b6100a86100a3366004d606f4d05bd60296d05b6040516100b5dfd0d60777d05b60405180910390f35b3480e2d000cad0db00d0fd5b506100d3d60419d05b60405173ffffffffffffffffffffffffffffffffffffffff9091168152d920d0d600b5d05b3480e2d00104d0db00d0fd5b5061006b610113366004d606d9d05bd604b0d05b3480e2d00124d0db00d0fd5b506100d3d60517d05b60006101577f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc5490565bdcd073ffffffffffffffffffffffffffffffffffffffff8116d70201d06040517f08c379a0000000000000000000000000000000000000000000000000000000008152d820d004820152d825d0248201527f50726f78793a20696d706c656d656e746174696f6e206e6f7420696e6974696160448201527f6c697a65640000000000000000000000000000000000000000000000000000006064820152d984d05b60405180910390fd5b36db00d037db00d0366000845af43ddb00d03e80d7021ed03d6000fd5b503d6000f35b7fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035473ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480d7027dd05033155be2d0028ed061028b81d605a3d05bddd05b61028bd6012dd05b60606102c07fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035490565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480d702f7d05033155be2d0040ad061030584d605a3d05bdb00d08573ffffffffffffffffffffffffffffffffffffffff16858560405161032f92dfd0d607ead05bd800d0405180830381855af4e0d0503d8060008114d7036ad0604051e0d0601f19603f3d011682016040523d82523dd800d02084013ed6036fd05b6060e0d05b50e0d0e0d081d70401d06040517f08c379a0000000000000000000000000000000000000000000000000000000008152d820d004820152d839d0248201527f50726f78793a2064656c656761746563616c6c20746f206e657720696d706c6560448201527f6d656e746174696f6e20636f6e7472616374206661696c6564000000000000006064820152d984d0d601f8d05be0d0610412dcd0565b610412d6012dd05b9392ded0ddd05b60006104437fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035490565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480d7047ad05033155be2d004a5d0507f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc5490565b6104add6012dd05b90565b7fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035473ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480d70509d05033155be2d0028ed061028b81d6060bd05b60006105417fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035490565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480d70578d05033155be2d004a5d0507fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035490565b7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc81905560405173ffffffffffffffffffffffffffffffffffffffff8216907fbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b90600090a2ddd05b60006106357fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035490565b7fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103839055db40d05173ffffffffffffffffffffffffffffffffffffffff8084168252851660208201529192507f7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f910160405180910390a1ded0565b803573ffffffffffffffffffffffffffffffffffffffff81168114d706d4d0db00d0fd5bd3d0d0d05bd800d02082840312e2d006ebd0db00d0fd5b61041282d606b0d05bdb00d0d800d04084860312e2d00709d0db00d0fd5b61071284d606b0d05b9250602084013567ffffffffffffffff808211e2d0072fd0db00d0fd5b818601e0d086601f830112d70743d0db00d0fd5b8135818111e2d00752d0db00d0fd5b8760208285010111e2d00764d0db00d0fd5b6020830194508093ded0ded09250925092565bd800d0208083528351808285015260005b81e1d0e2d007a4d0858101830151858201d940d0528201d60788d05b818111e2d007b6d0d800d04083870101525b50d91fd07fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe01692909201d940d09392ded0ddd05b8183823760009101908152d3d0d0d0fea164736f6c634300080f000a")

type OptCode []byte

// ThreeU8Operands Aux struct for code fusion of 3 uint8 operands
type ThreeU8Operands struct {
	x, y, z uint8
}

type OpCodeCache struct {
	opcodesCache   map[common.Address]map[common.Hash]OptCode
	codeCacheMutex sync.RWMutex
	codeCacheSize  uint64
	/* map of shl and sub arguments and results*/
	shlAndSubMap      map[ThreeU8Operands]*uint256.Int
	shlAndSubMapMutex sync.RWMutex
}

func (c *OpCodeCache) GetCachedCode(address common.Address, codeHash common.Hash) OptCode {

	c.codeCacheMutex.RLock()

	processedCode, ok := c.opcodesCache[address][codeHash]
	if !ok {
		processedCode = nil
	}
	c.codeCacheMutex.RUnlock()
	return processedCode
}

func (c *OpCodeCache) RemoveCachedCode(address common.Address, hash common.Hash) {
	c.codeCacheMutex.Lock()
	if c.opcodesCache == nil || c.codeCacheSize == 0 {
		c.codeCacheMutex.Unlock()
		return
	}
	if hash == common.BytesToHash(nil) {
		_, ok := c.opcodesCache[address]
		if ok {
			delete(c.opcodesCache, address)
		}
	} else {
		_, ok := c.opcodesCache[address][hash]
		if ok {
			delete(c.opcodesCache[address], hash)
		}
	}
	c.codeCacheMutex.Unlock()
}

func (c *OpCodeCache) UpdateCodeCache(address common.Address, code OptCode, codeHash common.Hash) error {

	c.codeCacheMutex.Lock()

	if c.codeCacheSize+CodeCacheGCSoftLimit > CodeCacheGCThreshold {
		log.Warn("Code cache GC triggered\n")
		// TODO: should we depends on Golang GC here?
		// TODO: the current implementation of clear all is not reasonable.
		// must have better algorithm such as LRU and should consider hot addresses such as ones in accesslist.
		for k := range c.opcodesCache {
			delete(c.opcodesCache, k)
		}
		c.codeCacheSize = 0
	}
	if c.opcodesCache[address] == nil {
		c.opcodesCache[address] = make(map[common.Hash]OptCode)
	}
	c.opcodesCache[address][codeHash] = code
	c.codeCacheSize += uint64(len(code))
	c.codeCacheMutex.Unlock()

	return nil
}

func (c *OpCodeCache) CacheShlAndSubMap(x uint8, y uint8, z uint8, val *uint256.Int) {
	c.shlAndSubMapMutex.Lock()
	if c.shlAndSubMap[ThreeU8Operands{x, y, z}] == nil {
		c.shlAndSubMap[ThreeU8Operands{x, y, z}] = val
	}
	c.shlAndSubMapMutex.Unlock()
}

func (c *OpCodeCache) GetValFromShlAndSubMap(x uint8, y uint8, z uint8) *uint256.Int {
	c.shlAndSubMapMutex.RLock()
	val, ok := c.shlAndSubMap[ThreeU8Operands{x, y, z}]
	c.shlAndSubMapMutex.RUnlock()
	if !ok {
		return nil
	}
	return val
}

var once sync.Once
var opcodeCache *OpCodeCache

func getOpCodeCacheInstance() *OpCodeCache {
	once.Do(func() {
		opcodeCache = &OpCodeCache{
			opcodesCache:   make(map[common.Address]map[common.Hash]OptCode, CodeCacheGCThreshold>>10),
			shlAndSubMap:   make(map[ThreeU8Operands]*uint256.Int, 4096),
			codeCacheMutex: sync.RWMutex{},
		}
		// opcodeCache.UpdateCodeCache(preInstalledAddr, preInstalledCode)
	})
	return opcodeCache
}
