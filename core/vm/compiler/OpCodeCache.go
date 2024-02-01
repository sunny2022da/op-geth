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
const CodeCacheGCSoftLimit = 2 * 1024 * 1024 /* 2MB */

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

func (c *OpCodeCache) RemoveCachedCode(address common.Address) {
	c.codeCacheMutex.Lock()
	if c.opcodesCache == nil || c.codeCacheSize == 0 {
		c.codeCacheMutex.Unlock()
		return
	}
	_, ok := c.opcodesCache[address]
	if ok {
		delete(c.opcodesCache, address)
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
		c.opcodesCache[address] = make(map[common.Hash]OptCode, 3)
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

func newOpCodeCache() *OpCodeCache {
	codeCache := new(OpCodeCache)
	codeCache.opcodesCache = make(map[common.Address]map[common.Hash]OptCode, CodeCacheGCThreshold>>10)
	codeCache.shlAndSubMap = make(map[ThreeU8Operands]*uint256.Int, 4096)
	codeCache.codeCacheMutex = sync.RWMutex{}
	opcodeCache = codeCache
	return codeCache
}

func GetOpCodeCacheInstance() *OpCodeCache {
	once.Do(func() {
		opcodeCache = newOpCodeCache()
	})
	return opcodeCache
}
