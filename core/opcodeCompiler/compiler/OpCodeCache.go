package compiler

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// CodeCacheGCThreshold TODO: make codecache threshold configurable.
const CodeCacheGCThreshold = 1024 * 1024 * 1024 /* 1 GB */
// CodeCacheGCSoftLimit is used to trigger GC for memory control.
// upper limit of bytecodes of smart contract is ~25MB.
const CodeCacheGCSoftLimit = 200 * 1024 * 1024 /* 200MB */

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

		// Handle Sigusr2 signal
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGUSR2)
		go func() {
			for { // Infinite loop to wait for signals
				signal := <-sigCh
				switch signal {
				case syscall.SIGUSR2:
					opcodeCache.codeCacheMutex.RLock()
					dumpJSON(opcodeCache.opcodesCache)
					opcodeCache.codeCacheMutex.RUnlock()
				}
			}
		}()
	})
	return opcodeCache
}

func dumpJSON(codeCache map[common.Address]map[common.Hash]OptCode) {
	filename := "codecache.json"
	// Marshal data to JSON
	jsonData, err := json.MarshalIndent(codeCache, "", "  ")
	if err != nil {
		log.Error("Error marshaling codecache to JSON:", "err", err)
		return
	}

	// Print JSON to standard output
	//fmt.Println("Data JSON:")
	// fmt.Println(string(jsonData))

	log.Info("OpcodeCache Dump:", "File", filename)
	// Optional: write JSON to file
	err = writeToFile(filename, jsonData)
	if err != nil {
		log.Error("Error writing JSON file:", "err", err)
	}
}

func writeToFile(filename string, data []byte) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	log.Error("dump codecache to codecache.json")
	return err
}
