package main

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"sync/atomic"
)

// ====================================================================================
// 샤딩 기반 블룸 필터
//!! 햇갈리지 말 것
//!! 이건 "Add, Contain" 연산을 샤딩한거임
//!! 공간을 많이 써서 빠른거지, 병렬처리는 아님
//!! 다만 샤딩을 이용해서 향후 BatchAdd등을 구현할 순 있을듯.
// ====================================================================================

// ShardedBloomFilter 샤딩 기반 블룸 필터
type ShardedBloomFilter struct {
	shards    []*BloomFilter // 독립적인 블룸 필터 샤드들
	numShards int            // 샤드 개수
	numItems  uint64         // 전체 삽입된 아이템 수 (원자적)
	shardMask uint64         // 샤드 선택용 마스크
	shardBits uint           // 샤드 인덱스 비트 수
}

// NewShardedBloomFilter 새로운 샤딩 블룸 필터 생성
func NewShardedBloomFilter(expectedItems uint64, falsePositiveRate float64) *ShardedBloomFilter {
	// 샤드 개수는 CPU 코어 수의 2배 (더 세밀한 분산)
	numShards := runtime.NumCPU() * 2

	// 2의 거듭제곱으로 조정 (비트 마스킹 최적화)
	actualShards := 1
	shardBits := uint(0)
	for actualShards < numShards {
		actualShards <<= 1
		shardBits++
	}

	// 각 샤드는 전체 데이터의 1/샤드수 만큼 처리
	itemsPerShard := max(expectedItems/uint64(actualShards), 100)

	// 샤드들 생성
	shards := make([]*BloomFilter, actualShards)
	for i := range actualShards {
		shards[i] = NewBloomFilter(itemsPerShard, falsePositiveRate)
	}

	fmt.Printf("🔧 샤딩 블룸 필터 설계:\n")
	fmt.Printf("   - 예상 아이템 수: %s개\n", formatNumber(expectedItems))
	fmt.Printf("   - 목표 오탐률: %.3f%%\n", falsePositiveRate*100)
	fmt.Printf("   - 샤드 개수: %d개 (CPU 코어: %d)\n", actualShards, runtime.NumCPU())
	fmt.Printf("   - 샤드당 아이템: %s개\n", formatNumber(itemsPerShard))
	fmt.Printf("   - 샤드 비트 수: %d비트\n", shardBits)

	// 각 샤드의 메모리 사용량
	totalMemoryMB := 0.0
	for _, shard := range shards {
		totalMemoryMB += float64(len(shard.bitArray)*8) / (1024 * 1024)
	}
	fmt.Printf("   - 총 메모리 사용량: %.2f MB\n", totalMemoryMB)
	fmt.Printf("   - 샤드당 메모리: %.2f MB\n\n", totalMemoryMB/float64(actualShards))

	return &ShardedBloomFilter{
		shards:    shards,
		numShards: actualShards,
		numItems:  0,
		// actualShards가 2의 거듭수므로 shardMask는 actualShards - 1로 설정
		// ex) shardMask는 111,11111...처럼 됨
		//** => 비트마스크의 AND연산 시 분배가 아주 빠름.(단, 마스크 필요)
		//* 모듈러와 결과가 같진 않지만 출력공간이 동일함.
		shardMask: uint64(actualShards - 1),
		shardBits: shardBits,
	}
}

// getShardIndex 데이터에서 샤드 인덱스 계산
func (sbf *ShardedBloomFilter) getShardIndex(data []byte) int {
	//* 빠른 해시 함수로 샤드 선택
	h := fnv.New64a()
	h.Write(data)
	hash := h.Sum64()

	//** 비트 마스킹으로 빠른 모듈로 연산
	//* ex) 샤드마스크가 111이고, 이걸로 임의의 수와 and연산 시
	//* 이는 모듈러와 유사한 효과를 냄(값 공간이 000~111까지이므로)
	return int(hash & sbf.shardMask)
}

// Add 아이템 추가 (락 없는 병렬 처리)
func (sbf *ShardedBloomFilter) Add(data []byte) {
	shardIndex := sbf.getShardIndex(data)
	sbf.shards[shardIndex].Add(data)
	atomic.AddUint64(&sbf.numItems, 1)
}

// Contains 아이템 존재 여부 확인 (락 없는 병렬 처리)
func (sbf *ShardedBloomFilter) Contains(data []byte) bool {
	shardIndex := sbf.getShardIndex(data)
	return sbf.shards[shardIndex].Contains(data)
}

// GetStats 통계 정보 반환
func (sbf *ShardedBloomFilter) GetStats() (uint64, float64, float64) {
	totalSetBits := uint64(0)
	totalSize := uint64(0)
	totalFPR := 0.0

	for _, shard := range sbf.shards {
		setBits, _, fpr := shard.GetStats()
		totalSetBits += setBits
		totalSize += shard.size
		totalFPR += fpr
	}

	avgFillRatio := float64(totalSetBits) / float64(totalSize)
	avgFPR := totalFPR / float64(sbf.numShards)

	return totalSetBits, avgFillRatio, avgFPR
}

// GetShardStats 개별 샤드 통계 (불균형 분석용)
func (sbf *ShardedBloomFilter) GetShardStats() []ShardStat {
	stats := make([]ShardStat, sbf.numShards)

	for i, shard := range sbf.shards {
		setBits, fillRatio, fpr := shard.GetStats()
		stats[i] = ShardStat{
			Index:     i,
			Items:     shard.numItems,
			SetBits:   setBits,
			FillRatio: fillRatio,
			FPR:       fpr,
		}
	}

	return stats
}

// ShardStat 샤드 통계 정보
type ShardStat struct {
	Index     int
	Items     uint64
	SetBits   uint64
	FillRatio float64
	FPR       float64
}
