package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/bbolt"
)

const (
	numItems    = 1_000_000
	testSize    = 10_000 // 각 테스트에 사용할 아이템 개수
	keySize     = 20
	bboltDBFile = "bbolt.db"
	badgerDir   = "badger"
	pebbleDir   = "pebble"
	bucketName  = "benchmark"
)

type BenchmarkResult struct {
	Name                       string
	WriteTime                  time.Duration
	DBSize                     int64
	SeqExistingReadTime        time.Duration
	SeqExistingMembershipTime  time.Duration
	RandExistingReadTime       time.Duration
	RandExistingMembershipTime time.Duration
	NonExistentMembershipTime  time.Duration
}

func main() {
	// --- 1. 데이터 생성 ---
	fmt.Printf("%d개의 테스트 데이터를 생성합니다...\n", numItems)
	existingKeys := make([][keySize]byte, numItems)
	values := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		binary.BigEndian.PutUint64(existingKeys[i][:], uint64(i))
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(i))
		values[i] = val
	}
	rand.Seed(time.Now().UnixNano())
	latestExistingKeys := existingKeys[numItems-testSize:]
	randExistingKeys := make([][keySize]byte, testSize)
	for i := 0; i < testSize; i++ {
		randExistingKeys[i] = existingKeys[rand.Intn(numItems)]
	}
	nonExistentKeys := make([][keySize]byte, testSize)
	for i := 0; i < testSize; i++ {
		binary.BigEndian.PutUint64(nonExistentKeys[i][:], uint64(numItems+i))
	}

	// --- 2. 벤치마크 실행 ---
	bboltResult, err := runBboltBenchmark(existingKeys, values, latestExistingKeys, randExistingKeys, nonExistentKeys)
	if err != nil {
		log.Fatalf("bbolt 실패: %v", err)
	}
	badgerResult, err := runBadgerBenchmark(existingKeys, values, latestExistingKeys, randExistingKeys, nonExistentKeys)
	if err != nil {
		log.Fatalf("BadgerDB 실패: %v", err)
	}
	pebbleResult, err := runPebbleBenchmark(existingKeys, values, latestExistingKeys, randExistingKeys, nonExistentKeys)
	if err != nil {
		log.Fatalf("PebbleDB 실패: %v", err)
	}

	// --- 3. 결과 출력 ---
	printResults([]BenchmarkResult{bboltResult, badgerResult, pebbleResult})
}

func runBboltBenchmark(keys [][keySize]byte, values [][]byte, latestKeys [][keySize]byte, randKeys [][keySize]byte, nonExistentKeys [][keySize]byte) (BenchmarkResult, error) {
	fmt.Println("\n--- bbolt 벤치마크 시작 ---")
	os.Remove(bboltDBFile)
	defer os.Remove(bboltDBFile)
	result := BenchmarkResult{Name: "bbolt"}

	start := time.Now()
	db, _ := bbolt.Open(bboltDBFile, 0600, nil)
	db.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))
		for i := 0; i < numItems; i++ {
			b.Put(keys[i][:], values[i])
		}
		return nil
	})
	db.Close()
	result.WriteTime = time.Since(start)

	db, _ = bbolt.Open(bboltDBFile, 0600, &bbolt.Options{ReadOnly: true})
	defer db.Close()
	fi, _ := os.Stat(bboltDBFile)
	result.DBSize = fi.Size()

	db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()
		start = time.Now()
		// bbolt는 Next()가 자동으로 마지막에서 멈추므로 카운터가 필요 없음
		for k, v := c.Seek(latestKeys[0][:]); k != nil; k, v = c.Next() {
			_ = v
		}
		result.SeqExistingReadTime = time.Since(start)
		start = time.Now()
		for k, _ := c.Seek(latestKeys[0][:]); k != nil; k, _ = c.Next() {
			_ = k
		}
		result.SeqExistingMembershipTime = time.Since(start)
		start = time.Now()
		for _, key := range randKeys {
			_ = b.Get(key[:])
		}
		result.RandExistingReadTime = time.Since(start)
		start = time.Now()
		for _, key := range randKeys {
			if b.Get(key[:]) != nil {
			}
		}
		result.RandExistingMembershipTime = time.Since(start)
		start = time.Now()
		for _, key := range nonExistentKeys {
			if b.Get(key[:]) == nil {
			}
		}
		result.NonExistentMembershipTime = time.Since(start)
		return nil
	})
	return result, nil
}

// (★★★★★ 최종 수정된 함수 ★★★★★)
func runBadgerBenchmark(keys [][keySize]byte, values [][]byte, latestKeys [][keySize]byte, randKeys [][keySize]byte, nonExistentKeys [][keySize]byte) (BenchmarkResult, error) {
	fmt.Println("\n--- BadgerDB 벤치마크 시작 ---")
	os.RemoveAll(badgerDir)
	defer os.RemoveAll(badgerDir)
	result := BenchmarkResult{Name: "BadgerDB"}
	opts := badger.DefaultOptions(badgerDir).WithLogger(nil)

	start := time.Now()
	db, _ := badger.Open(opts)
	wb := db.NewWriteBatch()
	for i := 0; i < numItems; i++ {
		wb.Set(keys[i][:], values[i])
	}
	wb.Flush()
	db.Close()
	result.WriteTime = time.Since(start)

	db, _ = badger.Open(opts.WithReadOnly(true))
	defer db.Close()
	result.DBSize, _ = getDirSize(badgerDir)

	db.View(func(txn *badger.Txn) error {
		// 2. 순차 (있는 데이터)
		start = time.Now()
		itRead := txn.NewIterator(badger.DefaultIteratorOptions)
		count := 0
		for itRead.Seek(latestKeys[0][:]); itRead.Valid() && count < testSize; itRead.Next() {
			item := itRead.Item()
			_, _ = item.ValueCopy(nil)
			count++
		}
		itRead.Close()
		result.SeqExistingReadTime = time.Since(start)

		mem_opts := badger.DefaultIteratorOptions
		mem_opts.PrefetchValues = false
		start = time.Now()
		itMem := txn.NewIterator(mem_opts)
		count = 0
		for itMem.Seek(latestKeys[0][:]); itMem.Valid() && count < testSize; itMem.Next() {
			_ = itMem.Item().Key()
			count++
		}
		itMem.Close()
		result.SeqExistingMembershipTime = time.Since(start)

		// 3. 임의 (있는 데이터)
		start = time.Now()
		for _, key := range randKeys {
			if item, err := txn.Get(key[:]); err == nil {
				_, _ = item.ValueCopy(nil)
			}
		}
		result.RandExistingReadTime = time.Since(start)
		start = time.Now()
		for _, key := range randKeys {
			_, _ = txn.Get(key[:])
		}
		result.RandExistingMembershipTime = time.Since(start)

		// 4. 임의 (없는 데이터)
		start = time.Now()
		for _, key := range nonExistentKeys {
			_, _ = txn.Get(key[:])
		}
		result.NonExistentMembershipTime = time.Since(start)
		return nil
	})
	return result, nil
}

func runPebbleBenchmark(keys [][keySize]byte, values [][]byte, latestKeys [][keySize]byte, randKeys [][keySize]byte, nonExistentKeys [][keySize]byte) (BenchmarkResult, error) {
	fmt.Println("\n--- PebbleDB 벤치마크 시작 ---")
	os.RemoveAll(pebbleDir)
	defer os.RemoveAll(pebbleDir)
	result := BenchmarkResult{Name: "PebbleDB"}

	start := time.Now()
	db, _ := pebble.Open(pebbleDir, &pebble.Options{Logger: nil})
	batch := db.NewBatch()
	for i := 0; i < numItems; i++ {
		batch.Set(keys[i][:], values[i], pebble.NoSync)
	}
	batch.Commit(pebble.NoSync)
	db.Close()
	result.WriteTime = time.Since(start)

	db, _ = pebble.Open(pebbleDir, &pebble.Options{ReadOnly: true, Logger: nil})
	defer db.Close()
	result.DBSize, _ = getDirSize(pebbleDir)

	start = time.Now()
	itRead, _ := db.NewIter(&pebble.IterOptions{})
	count := 0
	for itRead.SeekGE(latestKeys[0][:]); itRead.Valid() && count < testSize; itRead.Next() {
		_ = itRead.Value()
		count++
	}
	itRead.Close()
	result.SeqExistingReadTime = time.Since(start)
	start = time.Now()
	itMem, _ := db.NewIter(&pebble.IterOptions{})
	count = 0
	for itMem.SeekGE(latestKeys[0][:]); itMem.Valid() && count < testSize; itMem.Next() {
		_ = itMem.Key()
		count++
	}
	itMem.Close()
	result.SeqExistingMembershipTime = time.Since(start)

	start = time.Now()
	for _, key := range randKeys {
		val, closer, err := db.Get(key[:])
		if err == nil {
			_ = val
			closer.Close()
		}
	}
	result.RandExistingReadTime = time.Since(start)
	start = time.Now()
	for _, key := range randKeys {
		_, closer, err := db.Get(key[:])
		if err == nil {
			closer.Close()
		}
	}
	result.RandExistingMembershipTime = time.Since(start)

	start = time.Now()
	for _, key := range nonExistentKeys {
		_, closer, err := db.Get(key[:])
		if err == nil {
			closer.Close()
		}
	}
	result.NonExistentMembershipTime = time.Since(start)

	return result, nil
}

func getDirSize(path string) (int64, error) {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, nil
}

func printResults(results []BenchmarkResult) {
	fmt.Println("\n\n--- 최종 벤치마크 결과 ---")
	fmt.Println("==================================================================================================================")
	fmt.Printf("%-32s | %-18s | %-18s | %-18s\n", "항목", "bbolt", "BadgerDB", "PebbleDB")
	fmt.Println("-------------------------------------- [1. 쓰기 성능] -----------------------------------------------------------")
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "저장 시간 (100만 건)", results[0].WriteTime.Round(time.Millisecond), results[1].WriteTime.Round(time.Millisecond), results[2].WriteTime.Round(time.Millisecond))
	fmt.Printf("%-32s | %-18s | %-18s | %-18s\n", "저장 공간", fmt.Sprintf("%.2f MB", float64(results[0].DBSize)/1024/1024), fmt.Sprintf("%.2f MB", float64(results[1].DBSize)/1024/1024), fmt.Sprintf("%.2f MB", float64(results[2].DBSize)/1024/1024))
	fmt.Println("-------------------------------------- [2. 순차 접근 (있는 데이터)] -------------------------------------------------")
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "읽기", results[0].SeqExistingReadTime.Round(time.Microsecond), results[1].SeqExistingReadTime.Round(time.Microsecond), results[2].SeqExistingReadTime.Round(time.Microsecond))
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "멤버십 확인", results[0].SeqExistingMembershipTime.Round(time.Microsecond), results[1].SeqExistingMembershipTime.Round(time.Microsecond), results[2].SeqExistingMembershipTime.Round(time.Microsecond))
	fmt.Println("-------------------------------------- [3. 임의 접근 (있는 데이터)] -------------------------------------------------")
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "읽기", results[0].RandExistingReadTime.Round(time.Microsecond), results[1].RandExistingReadTime.Round(time.Microsecond), results[2].RandExistingReadTime.Round(time.Microsecond))
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "멤버십 확인", results[0].RandExistingMembershipTime.Round(time.Microsecond), results[1].RandExistingMembershipTime.Round(time.Microsecond), results[2].RandExistingMembershipTime.Round(time.Microsecond))
	fmt.Println("-------------------------------------- [4. 임의 접근 (없는 데이터)] -------------------------------------------------")
	fmt.Printf("%-32s | %-18v | %-18v | %-18v\n", "멤버십 확인", results[0].NonExistentMembershipTime.Round(time.Microsecond), results[1].NonExistentMembershipTime.Round(time.Microsecond), results[2].NonExistentMembershipTime.Round(time.Microsecond))
	fmt.Println("==================================================================================================================")
}
