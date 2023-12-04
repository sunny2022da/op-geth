package vm

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm/compiler"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
	"os"
)

type CodeType uint8

const (
	Opcode = iota
	/* CompiledCode */
)

// OpCodeProcessor is thread local
type OpCodeProcessor struct {
}

type OpCodeProcessorConfig struct {
	DoOpcodeFusion bool
}

func (p *OpCodeProcessor) GetProcessedCode(kind int, contract *Contract, config OpCodeProcessorConfig) (compiler.OptCode, error) {
	if kind != Opcode {
		fmt.Fprintf(os.Stderr, "Only support optimizing of the opcode")
		return nil, ErrFailPreprocessing
	}

	/* TODO: create a func like DoOpcodeOptimization */
	if !config.DoOpcodeFusion {
		return nil, nil
	}

	/* Try load from cache */
	codeHash := contract.CodeHash
	if contract.CodeHash == (common.Hash{}) {
		codeHash = crypto.Keccak256Hash(contract.Code)
	}

	codeCache := compiler.GetOpCodeCacheInstance()
	// TODO-dav:The lock on the whole codecache is not optimal, consider use smaller granularity.
	processedCode := codeCache.GetCachedCode(contract.Address(), codeHash)

	if processedCode == nil || len(processedCode) == 0 {
		var err error
		processedCode, err = p.processByteCodes(contract, config)
		if err != nil {
			log.Warn("Can not generate optimized code: %s\n", err.Error())
			return nil, err
		}
		//TODO - dav: - make following in one func.
		//todo - dav: if cache number > 5. clear. maybe instead reinstall at setcallcode().
		err = codeCache.UpdateCodeCache(contract.Address(), codeHash, processedCode)
		if err != nil {
			log.Warn("Not update code cache", "err", err)
		}
	}
	return processedCode, nil
}

func (p *OpCodeProcessor) processByteCodes(contract *Contract, config OpCodeProcessorConfig) (compiler.OptCode, error) {
	return p.doOpcodesProcess(contract, config)
}

func (p *OpCodeProcessor) doOpcodesProcess(contract *Contract, config OpCodeProcessorConfig) (compiler.OptCode, error) {
	if !config.DoOpcodeFusion {
		return nil, ErrFailPreprocessing
	}
	code, err := p.doCodeFusion(contract)
	if err != nil {
		return nil, ErrFailPreprocessing
	}
	return code, nil
}

func (p *OpCodeProcessor) doCodeFusion(contract *Contract) ([]byte, error) {
	fusedCode := make([]byte, len(contract.Code))
	length := copy(fusedCode, contract.Code)
	skipToNext := false
	for i := 0; i < length; i++ {
		cur := i
		skipToNext = false

		if length > cur+7 {
			code0 := OpCode(fusedCode[cur+0])
			code1 := OpCode(fusedCode[cur+1])
			code2 := OpCode(fusedCode[cur+2])
			code3 := OpCode(fusedCode[cur+3])
			code4 := OpCode(fusedCode[cur+4])
			code5 := OpCode(fusedCode[cur+5])
			code6 := OpCode(fusedCode[cur+6])
			code7 := OpCode(fusedCode[cur+7])
			// shift and then sub - this is mostly used to generate a 160bit addr from 256bit value.
			// The following 7 bytes are usually used to generate the bit mast of 150 bits of 1s
			// TODO-dav: more specifically, testing the arguments are 0x1, 0x1 and 0xa0, and then these can be
			// simplified to single push20 0xff...f
			if code0 == PUSH1 && code2 == PUSH1 && code4 == PUSH1 && code6 == SHL && code7 == SUB {
				x := uint8(code1)
				y := uint8(code3)
				z := uint8(code5)
				// (y<<z) - x
				val := uint256.NewInt(uint64(y))
				val.Lsh(val, uint(z))
				val.Sub(val, uint256.NewInt(uint64(x)))

				// update the code.
				// ShlAndSub is actually worked like pushed an uint256,
				// todo-dav: replace with push32.
				op := ShlAndSub
				fusedCode[cur] = byte(op)
				codeCache := compiler.GetOpCodeCacheInstance()
				codeCache.CacheShlAndSubMap(x, y, z, val)

				// now add three operands in code.
				fusedCode[cur+1], fusedCode[cur+2], fusedCode[cur+3] = x, y, z
				// fill reminders as nop
				for j := 4; j < 8; j++ {
					fusedCode[cur+j] = byte(Nop)
				}
				i += 7
				continue
			}
		}

		if length > cur+4 {
			code0 := OpCode(fusedCode[cur+0])
			code1 := OpCode(fusedCode[cur+1])
			code2 := OpCode(fusedCode[cur+2])
			code3 := OpCode(fusedCode[cur+3])
			code4 := OpCode(fusedCode[cur+4])
			if code0 == AND && code1 == SWAP1 && code2 == POP && code3 == SWAP2 && code4 == SWAP1 {
				op := AndSwap1PopSwap2Swap1
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				fusedCode[cur+2] = byte(Nop)
				fusedCode[cur+3] = byte(Nop)
				fusedCode[cur+4] = byte(Nop)
				skipToNext = true
			}

			// Test zero and Jump. target offset at code[2-3]
			if code0 == ISZERO && code1 == PUSH2 && code4 == JUMPI {
				op := JumpIfZero
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				fusedCode[cur+4] = byte(Nop)

				startMin := cur + 2
				endMin := cur + 4
				integer := new(uint256.Int)
				integer.SetBytes(common.RightPadBytes(
					fusedCode[startMin:endMin], 2))

				skipToNext = true
			}

			if skipToNext {
				i += 4
				continue
			}
		}

		if length > cur+3 {
			code0 := OpCode(fusedCode[cur+0])
			code1 := OpCode(fusedCode[cur+1])
			code2 := OpCode(fusedCode[cur+2])
			code3 := OpCode(fusedCode[cur+3])
			if code0 == SWAP2 && code1 == SWAP1 && code2 == POP && code3 == JUMP {
				op := Swap2Swap1PopJump
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				fusedCode[cur+2] = byte(Nop)
				fusedCode[cur+3] = byte(Nop)
				skipToNext = true
			}

			if code0 == SWAP1 && code1 == POP && code2 == SWAP2 && code3 == SWAP1 {
				op := Swap1PopSwap2Swap1
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				fusedCode[cur+2] = byte(Nop)
				fusedCode[cur+3] = byte(Nop)
				skipToNext = true
			}

			if code0 == POP && code1 == SWAP2 && code2 == SWAP1 && code3 == POP {
				op := PopSwap2Swap1Pop
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				fusedCode[cur+2] = byte(Nop)
				fusedCode[cur+3] = byte(Nop)
				skipToNext = true
			}
			// push and jump
			if code0 == PUSH2 && code3 == JUMP {
				op := Push2Jump
				fusedCode[cur] = byte(op)
				fusedCode[cur+3] = byte(Nop)
				skipToNext = true
			}

			if code0 == PUSH2 && code3 == JUMPI {
				op := Push2JumpI
				fusedCode[cur] = byte(op)
				fusedCode[cur+3] = byte(Nop)
				skipToNext = true
			}

			if code0 == PUSH1 && code2 == PUSH1 {
				op := Push1Push1
				fusedCode[cur] = byte(op)
				fusedCode[cur+2] = byte(Nop)
				skipToNext = true
			}

			if skipToNext {
				i += 3
				continue
			}
		}

		if length > cur+2 {
			code0 := OpCode(fusedCode[cur+0])
			_ = OpCode(fusedCode[cur+1])
			code2 := OpCode(fusedCode[cur+2])
			if code0 == PUSH1 {
				if code2 == ADD {
					op := Push1Add
					fusedCode[cur] = byte(op)
					fusedCode[cur+2] = byte(Nop)
					skipToNext = true
				}
				if code2 == SHL {
					op := Push1Shl
					fusedCode[cur] = byte(op)
					fusedCode[cur+2] = byte(Nop)
					skipToNext = true
				}

				if code2 == DUP1 {
					op := Push1Dup1
					fusedCode[cur] = byte(op)
					fusedCode[cur+2] = byte(Nop)
					skipToNext = true
				}

			}
			if skipToNext {
				i += 2
				continue
			}
		}

		if length > cur+1 {
			code0 := OpCode(fusedCode[cur+0])
			code1 := OpCode(fusedCode[cur+1])

			if code0 == SWAP1 && code1 == POP {
				op := Swap1Pop
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}
			if code0 == POP && code1 == JUMP {
				op := PopJump
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}

			if code0 == POP && code1 == POP {
				op := Pop2
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}

			if code0 == SWAP2 && code1 == SWAP1 {
				op := Swap2Swap1
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}

			if code0 == SWAP2 && code1 == POP {
				op := Swap2Pop
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}

			if code0 == DUP2 && code1 == LT {
				op := Dup2LT
				fusedCode[cur] = byte(op)
				fusedCode[cur+1] = byte(Nop)
				skipToNext = true
			}

			if skipToNext {
				i++
				continue
			}
		}

		skip, steps := calculateSkipSteps(fusedCode, cur)
		if skip {
			i += steps
			continue
		}

	}
	return fusedCode, nil
}

func calculateSkipSteps(code []byte, cur int) (skip bool, steps int) {
	inst := OpCode(code[cur])
	if inst >= PUSH1 && inst <= PUSH32 {
		// skip the data.
		steps = int(inst - PUSH1 + 1)
		skip = true
		return skip, steps
	}

	switch inst {
	case ShlAndSub:
		steps = 7
		skip = true
	case Push2Jump, Push2JumpI:
		steps = 3
		skip = true
	case Push1Push1:
		steps = 3
		skip = true
	case Push1Add, Push1Shl, Push1Dup1:
		steps = 2
		skip = true
	case JumpIfZero:
		steps = 4
		skip = true
	default:
		return false, 0
	}
	return skip, steps
}
