package main

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

func main() {
	fmt.Println("🔍 === 샤딩 vs 기본 블룸 필터 비교 (1천만개) ===")
	fmt.Printf("CPU 코어 수: %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS: %d\n\n", runtime.GOMAXPROCS(0))

	// 테스트 설정
	expectedItems := uint64(10000000) // 1천만개
	targetFPR := 0.001                // 0.1%
	testCases := 10000000               // 1000만개 쿼리

	fmt.Printf("📋 테스트 설정:\n")
	fmt.Printf("   - 데이터 크기: %s개\n", formatNumber(expectedItems))
	fmt.Printf("   - 목표 오탐률: %.3f%%\n", targetFPR*100)
	fmt.Printf("   - 테스트 케이스: %s개\n\n", formatNumber(uint64(testCases)))

	// 전체 시작 시간
	totalStart := time.Now()

	// 기본 블룸 필터 테스트
	fmt.Println("=" + strings.Repeat("=", 50))
	basicResult := testBasicBloomFilter(expectedItems, targetFPR, testCases)
	printResult(basicResult)

	// 메모리 정리
	runtime.GC()
	time.Sleep(1 * time.Second)

	// 샤딩 블룸 필터 테스트
	fmt.Println("\n" + strings.Repeat("=", 50))
	shardedResult := testShardedBloomFilter(expectedItems, targetFPR, testCases)
	printResult(shardedResult)

	// 샤드 균형 분석 (샤딩 테스트 후 실행)
	fmt.Println("\n" + strings.Repeat("=", 50))
	// 참고: 실제로는 sbf 인스턴스가 필요하지만, 데모용으로 결과만 출력

	// 성능 비교
	comparePerformance(basicResult, shardedResult)

	totalTime := time.Since(totalStart)
	fmt.Printf("\n🎉 전체 실행 시간: %v\n", totalTime)

	// 권장사항
	fmt.Println("\n💡 === 권장사항 ===")
	if shardedResult.TotalOpsPerSec > basicResult.TotalOpsPerSec {
		fmt.Println("✅ 대용량 데이터에서는 샤딩 블룸 필터를 사용하세요!")
		fmt.Println("   - 락 경합 없는 진정한 병렬 처리")
		fmt.Println("   - 예측 가능한 성능")
		fmt.Println("   - 수평 확장 가능")
	} else {
		fmt.Println("⚠️ 이 환경에서는 기본 블룸 필터가 더 적합할 수 있습니다.")
	}

	fmt.Println("\n🔧 샤딩 블룸 필터 최적화 팁:")
	fmt.Println("   - 샤드 수를 CPU 코어의 배수로 설정")
	fmt.Println("   - 해시 분산이 균등한지 주기적으로 확인")
	fmt.Println("   - 메모리 여유가 있다면 샤드 수를 늘려 병렬성 향상")
	fmt.Println("   - 각 샤드의 오탐률을 독립적으로 관리")
}
