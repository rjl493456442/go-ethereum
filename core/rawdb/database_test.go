// Copyright 2017 The go-ethereum Authors
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

package rawdb

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"testing"
)

func TestFoo(t *testing.T) {
	owner := common.HexToHash("76e83933629aac570bd42095dd3f7418adacc5cd156cd11aa9f66dddc16d72e5")
	key := storageTrieNodeKey(owner, []byte{0xd, 0xe, 0x3, 0xe, 0x4})
	fmt.Println(hexutil.Encode(key))

	blob := common.FromHex("0xf901f1a0f66a9895c4e786990af28d63444b7015b91ba4061aaea4fe438fd9b748fad1b3a0fc9fdbaabe3dc0baab436e2ecea6ddcc4cfb1a00e1a991312f6727c23fff5842a0505f7e33dc679749fb99a9d797ed226a475b521feaeb0725296f50ac2af95325a0ce09a797f9065d2f4ed3daae7b286ec69d3ac69aaa8ae27ff1d88fd8445ea7ada03be61670fadf9debf520468a8e86d615068287984ab82982e02c1315348f7908a048f99ad6e1b7ef75d6fa14600c16446d945a34ed2bb9fccc29a0bedd0ffe6108a0ebdd529a9c8129f00140ca4ac47a9ccab0a23c4c131aa7285720147898fca778a06af528f8a55044377fdb5a37a9ff459ad6109db1bebc1b8ef9d492df61515aaba0055c9a8467e2ddb840c08c92638bbf7066f3dba71307ab6594179ff9221d323ba0bbcdaf311004e79639ad01dc868d896ee4beac9a2dff86c1d9c0114ce98b00f6a06e348e15714748044154756933e173c0d15627fb65808ec04ea8d50d0dfc8273a004f9f99861cc74804093f04733b5d1d21fade8e8c3dcf2a53ace91962507cd72a0b345ef9ef61d7e393cc479bf5abb64dd1cdd514c854aee76f1d1e9b8218f998d80a0a3dee7fa630bba136757209656c6a209859c2ef59b446222087c62e42da24db6a0bfc84f0a8daaba35c003ce2729cefb88ce54608f677d0f8058da955b0225806280")
	fmt.Println(crypto.Keccak256Hash(blob).Hex())

	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x095ddce4fd8818ad159d778e6a9898a2474933ca")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x51bd43485cda9255c78bfa9dd17f5ee063ee9d66")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x31c563632a51cb0f9c481e4502c13b11949d9554")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xe39b59f94001c648ddb0ad770ab5a2d2e475f4b7")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf458747c6d6db57970de80da6474c0a3dfe94bf1")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000042069")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x44e63823393b8c3cc39347139707944bb88b9e98")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xc1cb36667c04d10ea5e035bf3cbfb82f20ab78c6")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xdb7ce68f0123badb42f0783376249ab8271473ce")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xebdb8213043b3908d1a461bd2c6e65267473e1dc")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000032001")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x2f6a076e89b3df6d76015caeb529e4997673e659")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000017001")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000042069")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf458747c6d6db57970de80da6474c0a3dfe94bf1")).Hex())
}

func TestBar(t *testing.T) {
	blob := common.FromHex("0xf901b1a021932e6b2594e4c9fc0584806db02d3136b81ba9ecdb42b7adbecb293bf2b07980a0d40ccf3777da73dffe5c52b25af1aab403e8f920fa93f3874536f04e21ff3e81a013e9c887cdedad352e75371a0278fe251299d16b716fc9e652559051eb7ae400a0cccd36d7f79b6c91ae71d5f10afece7b0de37ebeb53ba1757603223690e49c0a80a00cda955246e7f2407f965c894a17afa4f7dda056e2ad5f98e8cc3ba95f73293ca0ef1c9510086f9697e364aa459ee91c238642446bd3af19ace7545fa707d43870a0a7d663671a7c501e5d7d08b6dd8a883ad94167660fb6714a2d6387282f6c9e13a06ac42542197838df1099206bacfbad0b0d9bc02532fdacf5733a883a87e27bdea00c6217fef68ebc460743522204c139f39e314ae85829420583528da6329ce5f6a049137619df106be625c19efe96f9103e90cd98f76d8898967512b79c5a08cec6a08bdb2d94c71094e659ca46c4ae1521fa08b6407e08f885ea6f7218da1dac6747a0e76ce3599a7ec94360bb4edaa100d27a682522cfbfd81a0f1388fe29eb908c27a04438adc79962996adf485dc88ecdf15f186d0a36b0024aeedddc08c64a8509bc8080")
	fmt.Println(crypto.Keccak256Hash(blob).Hex())

	parent := common.FromHex("0xf90211a0b5282895f3e73c1c503109e845e8e390c1272e6b4d2e56ef869c09625bb0d6d7a0f23c9c637b0ce233cff0fb2cf523c908388ad3dcced217503880cd02ab1b7cb9a05cfc0d55f2e2a8234dfb9815646d0fc1ac2c24793ef85a2412163fe013761affa073a578bf8013e4895806ecde46967b1233747f9a87ec6f23257a3a0d034d3250a015ca411f8df6bc9f028d0c8f8aab95a8cc06c6f2dea3514dad7a5955c9da8a47a0b2485c81c31db51cc56f1b1d2923258598570d814e6fdd2417d5da7a02b5dce8a0576f7e39eb8fc666ec778eaaeb7bd1e9dbc8b99e53166c8489a098ff701522f1a0bda7012521b345566775b8b72ed8202055d96ee8710a5091128faee9e0b9b6c1a053907c6dd8a260b27135b4baf6520c30a57215df803c5279c0ee2eed0dc9278ca0858a11512fba11d5945121c71775ac1c1191781330aef98bb88cedc3663fcac4a08601339fbc1450b6ee3ef47f5c2ced8c66f06f49c4c1aab86418f0107fd0db51a050e11f44e8e34f693d9ed6be619dcd2c4b312a111138ffc7d6a1cd26dcb371dba0f547b62d2f72cca93cd59201e4b25d37d5311e75e71569d684b3d5b04ce4d1daa0596ca73a457f4f56efa6813c0c7dfc8d7709e57627b314deef57672d4db58344a05a963e92d34d98ceeddd3b2f40af1c5f8090090f52d3fce6d598c9fcec25b400a0417fb3a0d4fb4661bd6903876b93d8285b095f4dd93c742e8b97faf401a97ec780")
	fmt.Println(crypto.Keccak256Hash(parent).Hex())

	a := "de3e4"
	prefix := []byte(a)

	key := common.FromHex("de3e441e798822302eb47f5cd72bd88d5bf9f774079cb6841ff0a70c7f0d3fc1")
	fmt.Println(prefix)
	fmt.Println([]byte(common.Bytes2Hex(key)))
	fmt.Println(bytes.HasPrefix([]byte(common.Bytes2Hex(key)), prefix))
}
