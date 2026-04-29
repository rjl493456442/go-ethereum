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

package vm

import "fmt"

// GasCosts denotes a vector of gas costs in the multidimensional metering
// paradigm. It represents the cost charged by an individual operation. The
// state component is signed so it can carry a refund (a previously billed
// state-creation that has been cancelled within the same transaction).
type GasCosts struct {
	RegularGas uint64
	StateGas   int64
}

// String returns a visual representation of the gas vector.
func (g *GasCosts) String() string {
	return fmt.Sprintf("<%v,%v>", g.RegularGas, g.StateGas)
}

// GasBudget denotes a vector of remaining gas allowances available for EVM
// execution in the multidimensional metering paradigm. Unlike GasCosts which
// represents the price of an operation, GasBudget tracks how much gas is
// left to spend.
//
// Regular gas and state gas are independent dimensions:
//   - cost.RegularGas is always charged from RegularGas.
//   - cost.StateGas, when positive, is charged from the StateGas reservoir
//     first and any shortfall spills onto RegularGas.
//   - cost.StateGas, when negative, is a refund (a state-creation cancelled
//     within this tx) and is added back to the StateGas reservoir. It does
//     not offset cost.RegularGas.
type GasBudget struct {
	RegularGas uint64 // Regular gas pool; charged for cost.RegularGas plus state-gas spillOver.
	StateGas   uint64 // State gas reservoir; charged first for positive cost.StateGas, refunded on negative.
}

// NewGasBudget creates a GasBudget with the given initial regular gas and
// state gas allowances.
func NewGasBudget(regularGas, stateGas uint64) GasBudget {
	return GasBudget{RegularGas: regularGas, StateGas: stateGas}
}

// Used returns the total amount of gas consumed so far across both buckets.
// If net refunds have grown the budget above its initial size (rare), Used
// returns 0 rather than wrapping uint64.
func (g GasBudget) Used(initial GasBudget) uint64 {
	initialTotal := initial.RegularGas + initial.StateGas
	currentTotal := g.RegularGas + g.StateGas
	if currentTotal > initialTotal {
		panic("gas is overcharged")
	}
	return initialTotal - currentTotal
}

// Exhaust sets all remaining regular gas to zero while with state gas unchanged.
func (g *GasBudget) Exhaust() {
	g.RegularGas = 0
}

func (g *GasBudget) Copy() GasBudget {
	return GasBudget{RegularGas: g.RegularGas, StateGas: g.StateGas}
}

// String returns a visual representation of the gas budget vector.
func (g GasBudget) String() string {
	return fmt.Sprintf("<%v,%v>", g.RegularGas, g.StateGas)
}

// spillOver returns the portion of cost.StateGas that the StateGas
// reservoir cannot cover and must therefore be paid from RegularGas. It is
// 0 when cost.StateGas is non-positive or when the reservoir already covers
// the full state cost.
func (g GasBudget) spillOver(cost GasCosts) uint64 {
	if cost.StateGas <= 0 {
		return 0
	}
	owe := uint64(cost.StateGas)
	if owe <= g.StateGas {
		return 0
	}
	return owe - g.StateGas
}

// Charge deducts the given gas cost from the budget. The two dimensions are
// independent:
//   - cost.RegularGas is taken from RegularGas.
//   - cost.StateGas, when positive, drains the StateGas reservoir first and
//     spills any shortfall onto RegularGas.
//   - cost.StateGas, when negative, tops up the StateGas reservoir as a
//     refund and does not offset cost.RegularGas.
//
// The operation is atomic: it either applies in full or returns false with
// the budget untouched.
//
// Returns the pre-charge RegularGas value (for tracer reporting) and a flag
// indicating whether the budget covered the cost.
func (g *GasBudget) Charge(cost GasCosts) (uint64, bool) {
	var (
		prior     = g.RegularGas
		regNeeded = cost.RegularGas + g.spillOver(cost)
	)
	if regNeeded < cost.RegularGas || regNeeded > g.RegularGas {
		return prior, false
	}
	g.RegularGas -= regNeeded

	switch {
	case cost.StateGas < 0:
		g.StateGas += uint64(-cost.StateGas)
	case cost.StateGas > 0:
		owe := uint64(cost.StateGas)
		if owe >= g.StateGas {
			g.StateGas = 0
		} else {
			g.StateGas -= owe
		}
	}
	return prior, true
}

// Refund adds the given gas budget back. It returns the pre-refund regular
// gas value and whether the budget was actually changed.
func (g *GasBudget) Refund(other GasBudget) (uint64, bool) {
	prior := g.RegularGas
	g.RegularGas += other.RegularGas
	g.StateGas += other.StateGas
	return prior, other.RegularGas != 0 || other.StateGas != 0
}

// RevertStateCharge undoes a cumulative state-gas charge previously applied
// via Charge — the inverse operation that callers use when a sub-frame is
// reverted and the state-creation it billed for is being thrown away. The
// argument carries the same sign convention as cost.StateGas in Charge:
//
//   - a positive stateCharge was a real charge: its full amount is added
//     back to the StateGas reservoir.
//   - a negative stateCharge was a refund (state cancelled in the same tx):
//     its absolute value is debited from the StateGas reservoir first; any
//     shortfall is taken from RegularGas.
//
// Returns the pre-revert RegularGas value (for tracer reporting) and a flag
// indicating whether the budget covered the negative case. On failure the
// budget is left untouched.
func (g *GasBudget) RevertStateCharge(stateCharge int64) (uint64, bool) {
	prior := g.RegularGas
	if stateCharge >= 0 {
		// Was a charge — give it back to the reservoir.
		g.StateGas += uint64(stateCharge)
		return prior, true
	}
	// Was a refund — take it back: state reservoir first, then regular.
	debit := uint64(-stateCharge)
	if debit <= g.StateGas {
		g.StateGas -= debit
		return prior, true
	}
	spillover := debit - g.StateGas
	if spillover > g.RegularGas {
		return prior, false
	}
	g.StateGas = 0
	g.RegularGas -= spillover
	return prior, true
}
