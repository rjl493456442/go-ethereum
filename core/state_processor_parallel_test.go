// Copyright 2026 The go-ethereum Authors
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

package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/params"
)

func TestParallelExecutionEnv(t *testing.T) {
	config := *params.MergedTestChainConfig
	config.AmsterdamTime = new(uint64)
	accessList := new(bal.BlockAccessList)

	t.Setenv(disableParallelExecutionEnv, "")
	if !supportsParallelExecution(accessList, &config, big.NewInt(1), 1, false, false) {
		t.Fatal("parallel execution disabled without environment flag")
	}
	t.Setenv(disableParallelExecutionEnv, "true")
	if supportsParallelExecution(accessList, &config, big.NewInt(1), 1, false, false) {
		t.Fatal("parallel execution enabled with environment flag")
	}
}
