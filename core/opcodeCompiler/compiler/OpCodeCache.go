package compiler

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
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

type OpCodeCache struct {
	opcodesCache   map[common.Address]OptCode
	codeCacheMutex sync.RWMutex
	codeCacheSize  uint64
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

func (c *OpCodeCache) GetCachedCode(address common.Address) OptCode {
	c.codeCacheMutex.RLock()
	processedCode, ok := c.opcodesCache[address]
	if !ok {
		processedCode = nil
	}
	c.codeCacheMutex.RUnlock()
	return processedCode
}

func (c *OpCodeCache) UpdateCodeCache(address common.Address, code OptCode) error {

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
	c.opcodesCache[address] = code
	c.codeCacheSize += uint64(len(code))
	c.codeCacheMutex.Unlock()

	return nil
}

var opcodeCache *OpCodeCache

const codeCacheFileName = "codecache.json"

func init() {
	opcodeCache = &OpCodeCache{
		opcodesCache:   make(map[common.Address]OptCode, CodeCacheGCThreshold>>10),
		codeCacheMutex: sync.RWMutex{},
	}

	// Try load code cache
	loadCodeCacheFromFile(codeCacheFileName, opcodeCache)
	// Handle Sigusr2 signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR2)
	go func() {
		for { // Infinite loop to wait for signals
			signal := <-sigCh
			switch signal {
			case syscall.SIGUSR2:
				opcodeCache.codeCacheMutex.RLock()
				dumpCodeCache(codeCacheFileName, opcodeCache.opcodesCache)
				opcodeCache.codeCacheMutex.RUnlock()
			}
		}
	}()
}

func getOpCodeCacheInstance() *OpCodeCache {
	return opcodeCache
}

func dumpCodeCache(filename string, codeCache map[common.Address]OptCode) {

	// Marshal data to JSON
	jsonData, err := json.MarshalIndent(codeCache, "", "  ")
	if err != nil {
		log.Error("Error marshaling codecache to JSON:", "err", err)
		return
	}

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

	size, err := f.Write(data)
	if err != nil {
		log.Warn("Fail dump data", "error", err)
		return err
	}
	log.Warn("dump data: ", "File", filename, "Size:", size)
	return err
}

func loadCodeCacheFromFile(filename string, cacheInstance *OpCodeCache) {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Warn("Fail to load Code Cache", "File", filename, "error", err)
		return
	}

	err = json.Unmarshal(data, &cacheInstance.opcodesCache)
	if err != nil {
		log.Warn("Fail to load Code Cache", "File", filename, "error", err)
		return
	}
	log.Info("Load Code Cache success", "File", filename, "Size", len(data))
	return
}
