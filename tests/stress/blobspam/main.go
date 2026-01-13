package main

import (
	"context"
	"crypto/ecdsa"
	"flag"
	"log"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	gokzg4844 "github.com/crate-crypto/go-eth-kzg"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"
)

var (
	rpcURL    = "http://127.0.0.1:8545"
	toAddress = "0x0000000000000000000000000000000000000000"
	chainID   = 1337 // dev mode

	batchSize   = 20
	batchPeriod = 2 * time.Second
	txFeeTip    = 1000_000_000 // 1 gwei
	txFeeCap    = 2000_000_000 // 2 gwei

	fundAddress common.Address
	fundKey     *ecdsa.PrivateKey
)

type account struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

func main() {
	var mode = flag.String("mode", "spam", "spam|build")
	flag.Parse()

	switch *mode {
	case "spam":
		spam()
	case "build":
		build()
	default:
		log.Fatal("invalid mode")
	}
	return
}

func spam() {
	ctx := context.Background()

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		log.Fatal(err)
	}
	fundKey = key
	fundAddress = crypto.PubkeyToAddress(fundKey.PublicKey)
	log.Print("fund address: ", fundAddress.Hex())

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Fatal(err)
	}
	accounts := genAccounts(500)

	if err := fundAccounts(ctx, client, accounts); err != nil {
		log.Fatal(err)
	}
	ticker := time.NewTicker(batchPeriod)
	defer ticker.Stop()

	log.Printf("RPC spam started: %d tx / %v using %d accounts", batchSize, batchPeriod, len(accounts))

	var acctIdx int
	for range ticker.C {
		for i := 0; i < batchSize; i++ {
			acct := accounts[acctIdx%len(accounts)]
			acctIdx++

			go func(a *account) {
				tx, err := buildBlobTx(a, func(address common.Address) (uint64, error) {
					return client.PendingNonceAt(ctx, a.addr)
				}, true)
				if err != nil {
					log.Println("build tx error:", err)
					return
				}
				if err := client.SendTransaction(ctx, tx); err != nil {
					log.Println("send tx error:", err)
					return
				}
				log.Printf("sent %s nonce=%d, hash=%v", a.addr.Hex(), tx.Nonce(), tx.Hash().Hex())
			}(acct)
		}
	}
}

func build() {
	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())

	var (
		total     = 10000
		slots     = make(chan struct{}, total)
		start     = time.Now()
		lock      sync.Mutex
		durations []time.Duration
	)
	for i := 0; i < runtime.NumCPU(); i++ {
		eg.Go(func() error {
			start := time.Now()
			defer func() {
				lock.Lock()
				durations = append(durations, time.Since(start))
				lock.Unlock()
			}()

			for {
				select {
				case <-slots:
				default:
					return nil
				}
				buildBlobTx(genAccounts(1)[0], func(address common.Address) (uint64, error) {
					return 0, nil
				}, false)
			}
		})
	}
	eg.Wait()

	// Aggregate total time
	var totalThreadTime time.Duration
	for _, t := range durations {
		totalThreadTime += t
	}

	elapsed := time.Since(start)
	throughput := float64(total) / (1024 * 1024) / elapsed.Seconds()

	log.Printf("Total Time: %s", time.Since(start))
	log.Printf("Aggregated Thread Time: %s", totalThreadTime)
	log.Printf("Throughput: %.2f ops", throughput)
}
func genAccounts(n int) []*account {
	var accounts []*account
	for range n {
		key, err := crypto.GenerateKey()
		if err != nil {
			log.Fatal(err)
		}
		addr := crypto.PubkeyToAddress(key.PublicKey)

		accounts = append(accounts, &account{
			key:  key,
			addr: addr,
		})
		log.Print("Generated account", addr.Hex())
	}
	return accounts
}

func fundAccounts(ctx context.Context, client *ethclient.Client, accounts []*account) error {
	nonce, err := client.PendingNonceAt(ctx, fundAddress)
	if err != nil {
		log.Fatal(err)
	}
	balance, err := client.BalanceAt(ctx, fundAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
	balanceInEther := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(params.Ether))
	log.Println("Initial balance (ether)", balanceInEther)

	var (
		hashes []common.Hash
		signer = types.LatestSignerForChainID(big.NewInt(int64(chainID)))
	)
	for _, acc := range accounts {
		tx := types.NewTx(&types.LegacyTx{
			Nonce:    nonce,
			To:       &acc.addr,
			Value:    big.NewInt(params.Ether),
			Gas:      21000,
			GasPrice: big.NewInt(int64(txFeeTip)),
		})
		signedTx, err := types.SignTx(tx, signer, fundKey)
		if err != nil {
			return err
		}
		if err := client.SendTransaction(ctx, signedTx); err != nil {
			return err
		}
		nonce += 1
		hashes = append(hashes, signedTx.Hash())
	}
	for i, hash := range hashes {
		receipt, err := bind.WaitMined(ctx, client, hash)
		if err != nil {
			return err
		}
		balance, err := client.BalanceAt(ctx, accounts[i].addr, nil)
		if err != nil {
			log.Fatal(err)
		}
		balanceInEther := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(params.Ether))

		log.Println("Transaction included", hash.Hex(), "status", receipt.Status, "balance (ether)", balanceInEther)
	}
	return nil
}

func randPoly4096() []fr.Element {
	poly := make([]fr.Element, 4096)
	for i := 0; i < 4096; i++ {
		var eval fr.Element
		_, err := eval.SetRandom()
		if err != nil {
			panic(err)
		}
		poly[i] = eval
	}
	return poly
}

func randBlob() kzg4844.Blob {
	poly := randPoly4096()
	data := gokzg4844.SerializePoly(poly)
	var blob kzg4844.Blob
	copy(blob[:], data[:])
	return blob
}

func buildBlobTx(a *account, getNonce func(address common.Address) (uint64, error), report bool) (*types.Transaction, error) {
	start := time.Now()
	nonce, err := getNonce(a.addr)
	if err != nil {
		log.Fatal(err)
	}
	fetchNonce := time.Since(start)

	blob := randBlob()
	start = time.Now()
	commitment, err := kzg4844.BlobToCommitment(&blob)
	if err != nil {
		return nil, err
	}
	buildCommitment := time.Since(start)

	start = time.Now()
	proofs, err := kzg4844.ComputeCellProofs(&blob)
	if err != nil {
		return nil, err
	}
	buildProof := time.Since(start)

	sidecar := types.NewBlobTxSidecar(types.BlobSidecarVersion1, []kzg4844.Blob{blob}, []kzg4844.Commitment{commitment}, proofs)
	tx := types.NewTx(&types.BlobTx{
		ChainID:    uint256.NewInt(uint64(chainID)),
		Nonce:      nonce,
		GasTipCap:  uint256.NewInt(uint64(txFeeTip)),
		GasFeeCap:  uint256.NewInt(uint64(txFeeCap)),
		Gas:        21000,
		To:         common.HexToAddress(toAddress),
		Value:      uint256.NewInt(0),
		BlobFeeCap: uint256.NewInt(uint64(txFeeCap)),
		BlobHashes: sidecar.BlobHashes(),
		Sidecar:    sidecar,
	})
	signer := types.LatestSignerForChainID(big.NewInt(int64(chainID)))

	signedTx, err := types.SignTx(tx, signer, a.key)
	if err != nil {
		return nil, err
	}
	if report {
		log.Printf("Built the blob transaction, nonce: %v, commitment: %v, proof: %v", fetchNonce, buildCommitment, buildProof)
	}
	return signedTx, nil
}
