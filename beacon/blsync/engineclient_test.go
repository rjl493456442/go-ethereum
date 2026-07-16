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

package blsync

import (
	"testing"

	"github.com/ethereum/go-ethereum/beacon/params"
)

func TestNewPayloadMethod(t *testing.T) {
	amsterdam := uint64(100)
	ec := &engineClient{config: &params.ClientConfig{AmsterdamTime: &amsterdam}}
	noAmsterdam := &engineClient{config: &params.ClientConfig{}}

	for _, test := range []struct {
		name      string
		fork      string
		timestamp uint64
		want      string
	}{
		{"deneb", "deneb", 99, "engine_newPayloadV3"},
		{"gloas before amsterdam", "gloas", 99, "engine_newPayloadV4"},
		{"gloas at amsterdam", "gloas", 100, "engine_newPayloadV5"},
		{"gloas after amsterdam", "gloas", 101, "engine_newPayloadV5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ec.newPayloadMethod(test.fork, test.timestamp); got != test.want {
				t.Fatalf("method = %s, want %s", got, test.want)
			}
		})
	}
	if got := noAmsterdam.newPayloadMethod("gloas", 100); got != "engine_newPayloadV4" {
		t.Fatalf("Gloas without Amsterdam: method = %s, want engine_newPayloadV4", got)
	}
}
