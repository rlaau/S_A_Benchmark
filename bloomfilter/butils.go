package main

import (
	"crypto/rand"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ====================================================================================
// 테스트 및 벤치마크 함수들
// ====================================================================================

// TestResult 테스트 결과
type TestResult struct {
	Name            string
	InsertTime      time.Duration
	QueryTime       time.Duration
	TotalTime       time.Duration
	MeasuredFPR     float64
	MemoryUsageMB   float64
	InsertOpsPerSec float64
	QueryOpsPerSec  float64
	TotalOpsPerSec  float64
}

// generateTestData 테스트 데이터 생성
func generateTestData(count int) [][]byte {
	data := make([][]byte, count)
	for i := 0; i < count; i++ {
		bytes := make([]byte, 12)
		rand.Read(bytes)
		data[i] = bytes
	}
	return data
}

// formatNumber 숫자 포맷팅
func formatNumber(n uint64) string {
	str := fmt.Sprintf("%d", n)
	result := ""

	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(char)
	}

	return result
}

// testBasicBloomFilter 기본 블룸 필터 테스트
func testBasicBloomFilter(expectedItems uint64, targetFPR float64, testCases int) TestResult {
	fmt.Println("🧪 기본 블룸 필터 테스트 중...")

	bf := NewBloomFilter(expectedItems, targetFPR)

	// 테스트 데이터 생성
	insertData := generateTestData(int(expectedItems))
	queryData := generateTestData(testCases)

	// 삽입 테스트
	insertStart := time.Now()
	for _, data := range insertData {
		bf.Add(data)
	}
	insertTime := time.Since(insertStart)

	// 쿼리 테스트
	queryStart := time.Now()
	falsePositives := 0
	for _, data := range queryData {
		if bf.Contains(data) {
			falsePositives++
		}
	}
	queryTime := time.Since(queryStart)

	measuredFPR := float64(falsePositives) / float64(testCases)
	memoryMB := float64(len(bf.bitArray)*8) / (1024 * 1024)
	totalTime := insertTime + queryTime

	return TestResult{
		Name:            "기본 블룸 필터",
		InsertTime:      insertTime,
		QueryTime:       queryTime,
		TotalTime:       totalTime,
		MeasuredFPR:     measuredFPR,
		MemoryUsageMB:   memoryMB,
		InsertOpsPerSec: float64(expectedItems) / insertTime.Seconds(),
		QueryOpsPerSec:  float64(testCases) / queryTime.Seconds(),
		TotalOpsPerSec:  float64(expectedItems+uint64(testCases)) / totalTime.Seconds(),
	}
}

// testShardedBloomFilter 샤딩 블룸 필터 테스트
func testShardedBloomFilter(expectedItems uint64, targetFPR float64, testCases int) TestResult {
	fmt.Println("🚀 샤딩 블룸 필터 테스트 중...")

	sbf := NewShardedBloomFilter(expectedItems, targetFPR)

	// 테스트 데이터 생성
	insertData := generateTestData(int(expectedItems))
	queryData := generateTestData(testCases)

	// 병렬 삽입 테스트
	insertStart := time.Now()

	numWorkers := runtime.NumCPU()
	chunkSize := int(expectedItems) / numWorkers

	var wg sync.WaitGroup
	for i := range numWorkers {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := min(start+chunkSize, int(expectedItems))

			for j := start; j < end; j++ {
				sbf.Add(insertData[j])
			}
		}(i * chunkSize)
	}
	wg.Wait()

	insertTime := time.Since(insertStart)

	// 병렬 쿼리 테스트
	queryStart := time.Now()

	var falsePositives int64
	chunkSize = testCases / numWorkers

	for i := range numWorkers {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := min(start+chunkSize, testCases)

			localFP := 0
			for j := start; j < end; j++ {
				if sbf.Contains(queryData[j]) {
					localFP++
				}
			}
			atomic.AddInt64(&falsePositives, int64(localFP))
		}(i * chunkSize)
	}
	wg.Wait()

	queryTime := time.Since(queryStart)

	measuredFPR := float64(falsePositives) / float64(testCases)

	// 메모리 사용량 계산
	totalMemoryMB := 0.0
	for _, shard := range sbf.shards {
		totalMemoryMB += float64(len(shard.bitArray)*8) / (1024 * 1024)
	}

	totalTime := insertTime + queryTime

	return TestResult{
		Name:            "샤딩 블룸 필터",
		InsertTime:      insertTime,
		QueryTime:       queryTime,
		TotalTime:       totalTime,
		MeasuredFPR:     measuredFPR,
		MemoryUsageMB:   totalMemoryMB,
		InsertOpsPerSec: float64(expectedItems) / insertTime.Seconds(),
		QueryOpsPerSec:  float64(testCases) / queryTime.Seconds(),
		TotalOpsPerSec:  float64(expectedItems+uint64(testCases)) / totalTime.Seconds(),
	}
}

// printResult 결과 출력
func printResult(result TestResult) {
	fmt.Printf("\n📊 %s 결과:\n", result.Name)
	fmt.Printf("   ⚡ 성능:\n")
	fmt.Printf("      - 삽입 시간: %v (%.0f ops/sec)\n",
		result.InsertTime, result.InsertOpsPerSec)
	fmt.Printf("      - 쿼리 시간: %v (%.0f ops/sec)\n",
		result.QueryTime, result.QueryOpsPerSec)
	fmt.Printf("      - 전체 시간: %v (%.0f ops/sec)\n",
		result.TotalTime, result.TotalOpsPerSec)
	fmt.Printf("   📈 정확도:\n")
	fmt.Printf("      - 측정 오탐률: %.4f%%\n", result.MeasuredFPR*100)
	fmt.Printf("   💾 메모리:\n")
	fmt.Printf("      - 사용량: %.2f MB\n", result.MemoryUsageMB)
}

// analyzeShardBalance 샤드 균형 분석
func analyzeShardBalance(sbf *ShardedBloomFilter) {
	fmt.Println("\n🔍 샤드 균형 분석:")

	stats := sbf.GetShardStats()

	var minItems, maxItems uint64
	var minFill, maxFill float64
	var totalItems uint64

	minItems = stats[0].Items
	maxItems = stats[0].Items
	minFill = stats[0].FillRatio
	maxFill = stats[0].FillRatio

	for _, stat := range stats {
		totalItems += stat.Items

		if stat.Items < minItems {
			minItems = stat.Items
		}
		if stat.Items > maxItems {
			maxItems = stat.Items
		}
		if stat.FillRatio < minFill {
			minFill = stat.FillRatio
		}
		if stat.FillRatio > maxFill {
			maxFill = stat.FillRatio
		}
	}

	avgItems := totalItems / uint64(len(stats))
	imbalance := float64(maxItems-minItems) / float64(avgItems) * 100

	fmt.Printf("   📊 아이템 분포:\n")
	fmt.Printf("      - 평균: %s개\n", formatNumber(avgItems))
	fmt.Printf("      - 최소: %s개\n", formatNumber(minItems))
	fmt.Printf("      - 최대: %s개\n", formatNumber(maxItems))
	fmt.Printf("      - 불균형도: %.1f%%\n", imbalance)

	fmt.Printf("   📊 충전률 분포:\n")
	fmt.Printf("      - 최소 충전률: %.2f%%\n", minFill*100)
	fmt.Printf("      - 최대 충전률: %.2f%%\n", maxFill*100)
	fmt.Printf("      - 충전률 차이: %.2f%%\n", (maxFill-minFill)*100)

	if imbalance < 10 {
		fmt.Println("   ✅ 샤드 균형이 양호합니다")
	} else if imbalance < 25 {
		fmt.Println("   ⚠️ 샤드 불균형이 약간 있습니다")
	} else {
		fmt.Println("   ❌ 샤드 불균형이 심각합니다")
	}
}

// comparePerformance 성능 비교
func comparePerformance(basic, sharded TestResult) {
	fmt.Println("\n⚡ === 성능 비교 ===")

	insertSpeedup := float64(basic.InsertTime) / float64(sharded.InsertTime)
	querySpeedup := float64(basic.QueryTime) / float64(sharded.QueryTime)
	totalSpeedup := float64(basic.TotalTime) / float64(sharded.TotalTime)

	memoryRatio := sharded.MemoryUsageMB / basic.MemoryUsageMB

	fmt.Printf("🏃 속도 개선:\n")
	fmt.Printf("   - 삽입 가속비: %.2fx\n", insertSpeedup)
	fmt.Printf("   - 쿼리 가속비: %.2fx\n", querySpeedup)
	fmt.Printf("   - 전체 가속비: %.2fx\n", totalSpeedup)

	fmt.Printf("💾 메모리 사용:\n")
	fmt.Printf("   - 기본: %.2f MB\n", basic.MemoryUsageMB)
	fmt.Printf("   - 샤딩: %.2f MB\n", sharded.MemoryUsageMB)
	fmt.Printf("   - 메모리 비율: %.2fx\n", memoryRatio)

	fmt.Printf("🎯 정확도:\n")
	fmt.Printf("   - 기본 오탐률: %.4f%%\n", basic.MeasuredFPR*100)
	fmt.Printf("   - 샤딩 오탐률: %.4f%%\n", sharded.MeasuredFPR*100)
	fmt.Printf("   - 오탐률 차이: %.4f%%\n", math.Abs(basic.MeasuredFPR-sharded.MeasuredFPR)*100)

	// 효율성 평가
	efficiency := totalSpeedup / memoryRatio
	fmt.Printf("\n📈 전체 효율성: %.2f (속도 향상 / 메모리 증가)\n", efficiency)

	if efficiency > 2.0 {
		fmt.Println("✅ 샤딩이 매우 효과적입니다!")
	} else if efficiency > 1.5 {
		fmt.Println("✅ 샤딩이 효과적입니다.")
	} else if efficiency > 1.0 {
		fmt.Println("⚠️ 샤딩 효과가 제한적입니다.")
	} else {
		fmt.Println("❌ 샤딩이 비효율적입니다.")
	}
}

// 추가 함수들

// runExtensiveTest 확장 테스트 (여러 크기 비교)
func runExtensiveTest() {
	fmt.Println("\n🔬 === 확장 테스트 (다양한 크기) ===")

	testSizes := []uint64{100000, 1000000, 10000000}

	fmt.Printf("%-12s %-15s %-15s %-12s %-12s\n",
		"크기", "기본(ops/s)", "샤딩(ops/s)", "가속비", "메모리 비율")
	fmt.Println(strings.Repeat("-", 70))

	for _, size := range testSizes {
		// 기본 블룸 필터
		basicResult := testBasicBloomFilter(size, 0.001, 10000)

		// 샤딩 블룸 필터
		shardedResult := testShardedBloomFilter(size, 0.001, 10000)

		speedup := shardedResult.TotalOpsPerSec / basicResult.TotalOpsPerSec
		memRatio := shardedResult.MemoryUsageMB / basicResult.MemoryUsageMB

		fmt.Printf("%-12s %-15.0f %-15.0f %-12.2fx %-12.2fx\n",
			formatNumber(size),
			basicResult.TotalOpsPerSec,
			shardedResult.TotalOpsPerSec,
			speedup, memRatio)
	}
}

// simulateShardBalance 샤드 균형 시뮬레이션
func simulateShardBalance() {
	fmt.Println("\n⚖️ === 샤드 균형 시뮬레이션 ===")

	// 실제 샤딩 블룸 필터 생성하여 균형 테스트
	sbf := NewShardedBloomFilter(1000000, 0.001)

	// 100만개 데이터 추가
	testData := generateTestData(1000000)

	fmt.Println("100만개 데이터 추가 후 샤드 분석...")

	numWorkers := runtime.NumCPU()
	chunkSize := 1000000 / numWorkers

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + chunkSize
			if end > 1000000 {
				end = 1000000
			}

			for j := start; j < end; j++ {
				sbf.Add(testData[j])
			}
		}(i * chunkSize)
	}
	wg.Wait()

	// 샤드 균형 분석
	analyzeShardBalance(sbf)
}

// benchmarkHashDistribution 해시 분산 벤치마크
func benchmarkHashDistribution() {
	fmt.Println("\n📊 === 해시 분산 벤치마크 ===")

	numShards := 8
	testCount := 100000

	// 샤드별 카운트
	shardCounts := make([]int, numShards)

	// 해시 분산 테스트
	start := time.Now()
	for i := 0; i < testCount; i++ {
		data := fmt.Sprintf("item_%d", i)

		h := fnv.New64a()
		h.Write([]byte(data))
		hash := h.Sum64()

		shardIndex := int(hash % uint64(numShards))
		shardCounts[shardIndex]++
	}
	hashTime := time.Since(start)

	// 분산 분석
	mean := float64(testCount) / float64(numShards)
	variance := 0.0

	fmt.Printf("샤드별 분포 (%d개 데이터):\n", testCount)
	for i, count := range shardCounts {
		deviation := float64(count) - mean
		variance += deviation * deviation

		fmt.Printf("   샤드 %d: %d개 (%.1f%%)\n",
			i, count, float64(count)*100/float64(testCount))
	}

	variance /= float64(numShards)
	stdDev := math.Sqrt(variance)

	fmt.Printf("\n통계:\n")
	fmt.Printf("   평균: %.1f개\n", mean)
	fmt.Printf("   표준편차: %.1f개\n", stdDev)
	fmt.Printf("   변동계수: %.2f%%\n", stdDev/mean*100)
	fmt.Printf("   해시 속도: %.0f ops/sec\n", float64(testCount)/hashTime.Seconds())

	if stdDev/mean < 0.05 {
		fmt.Println("   ✅ 해시 분산이 매우 균등합니다")
	} else if stdDev/mean < 0.1 {
		fmt.Println("   ✅ 해시 분산이 양호합니다")
	} else {
		fmt.Println("   ⚠️ 해시 분산 개선이 필요합니다")
	}
}
