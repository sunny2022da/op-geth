// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"fmt"
	"sync"

	"github.com/holiman/uint256"
)

var stackPool = sync.Pool{
	New: func() interface{} {
		return &Stack{data: make([]uint256.Int, 0, 16)}
	},
}

// Stack is an object for basic stack operations. Items popped to the stack are
// expected to be changed and modified. stack does not take care of adding newly
// initialised objects.
type Stack struct {
	Tos1      uint256.Int // Todo-dav: make this private
	Tos2      uint256.Int // Todo-dav: make this private
	Tos1InUse bool        // Todo-dav: make this private
	Tos2InUse bool        // Todo-dav: make this private
	data      []uint256.Int
}

func newstack() *Stack {
	stack := stackPool.Get().(*Stack)
	stack.Tos1InUse = false
	stack.Tos2InUse = false
	return stack
}

func returnStack(s *Stack) {
	s.data = s.data[:0]
	stackPool.Put(s)
	s.Tos1InUse = false
	s.Tos2InUse = false
}

// Data returns the underlying uint256.Int array.
func (st *Stack) Data() []uint256.Int {
	return st.data
}

// tos2->tos1->st.data[Len-1]-> ...

// push push to tos
func (st *Stack) push(d *uint256.Int) {
	if !st.Tos1InUse {
		st.Tos1 = *d
		st.Tos1InUse = true
	} else {
		if st.Tos2InUse {
			// push tos2 to st.data
			st.data = append(st.data, st.Tos1)
			st.Tos1 = st.Tos2
			st.Tos2 = *d
		} else {
			// push to tos2
			st.Tos2 = *d
			st.Tos2InUse = true
		}
	}
	// NOTE push limit (1024) is checked in baseCheck
	// st.data = append(st.data, *d)
}

// pop the top most elem in stack or cache
func (st *Stack) pop() (ret uint256.Int) {
	if st.Tos2InUse {
		ret = st.Tos2
		st.Tos2InUse = false
	} else {
		// check tos1
		if st.Tos1InUse {
			ret = st.Tos1
			st.Tos1InUse = false
		} else if len(st.data) > 0 {
			ret = st.data[len(st.data)-1]
			st.data = st.data[:len(st.data)-1]
		}
	}
	return
}

func (st *Stack) Len() int {
	length := len(st.data)
	if st.Tos1InUse {
		length++
	}
	if st.Tos2InUse {
		length++
	}
	return length
}

func (st *Stack) swap(n int) {
	// n is >=2
	// it is not possible that tos1 in use and tos2 not in use when n >=2
	if st.Tos2InUse {
		// todo-dav: debug only, delete me
		if !st.Tos1InUse {
			fmt.Errorf("FATAL: stack underflow! swap: %v\n", n)
			panic(n)
		}
		// if n == 2, swap tos1 and tos2
		if n == 2 {
			st.Tos2, st.Tos1 = st.Tos1, st.Tos2
			return
		} else {
			// n > 2, swap tos2 and st.data.
			st.Tos2, st.data[len(st.data)-n+2] = st.data[len(st.data)-n+2], st.Tos2
		}
	} else if st.Tos1InUse {
		// tos2 not in use. tos1 is the top most. swap with st.data
		st.Tos1, st.data[len(st.data)-n+1] = st.data[len(st.data)-n+1], st.Tos1
	} else {
		// st.data[st.Len()-1] is the top of stack
		st.data[len(st.data)-n], st.data[len(st.data)-1] = st.data[len(st.data)-1], st.data[len(st.data)-n]
	}
	// st.data[st.Len()-n], st.data[st.Len()-1] = st.data[st.Len()-1], st.data[st.Len()-n]
}

func (st *Stack) dup(n int) {
	if !st.Tos2InUse {
		if !st.Tos1InUse {
			// tos is st.data
			st.Tos1 = st.data[len(st.data)-n]
			st.Tos1InUse = true
		} else {
			// tos is Tos1
			if n == 1 {
				st.Tos2 = st.Tos1
				st.Tos2InUse = true
			} else {
				st.Tos2 = st.data[len(st.data)-n+1]
				st.Tos2InUse = true
			}
		}
	} else {
		// tos is Tos2
		if !st.Tos1InUse {
			// this is not possible
			fmt.Printf("FATAL: stack underflow!! dup %v\n", n)
			panic(st.Len())
		} else {
			// all tos in use, need push to st.data
			var tmp uint256.Int
			if n == 1 {
				tmp = st.Tos2
			} else if n == 2 {
				tmp = st.Tos1
			} else {
				tmp = st.data[len(st.data)-n+2]
			}
			// push Tos2.
			st.data = append(st.data, st.Tos1)
			st.Tos1 = st.Tos2
			st.Tos2 = tmp
		}
	}
}

func (st *Stack) peek() *uint256.Int {
	if st.Tos2InUse {
		return &st.Tos2
	}
	if st.Tos1InUse {
		return &st.Tos1
	}
	return &st.data[len(st.data)-1]
}

// Back returns the n'th item in stack
func (st *Stack) Back(n int) *uint256.Int {
	if st.Tos2InUse {
		if n == 0 {
			return &st.Tos2
		}
		if n == 1 {
			return &st.Tos1
		}
		return &st.data[len(st.data)-n+1]
	} else {
		if st.Tos1InUse {
			if n == 0 {
				return &st.Tos1
			}
			return &st.data[len(st.data)-n]
		}
	}
	return &st.data[len(st.data)-n-1]
}
