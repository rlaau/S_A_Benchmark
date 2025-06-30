package main

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

func main() {
	fmt.Println("정렬 알고리즘 벤치마크 시작...")
	fmt.Printf("CPU 코어 수: %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS: %d\n\n", runtime.GOMAXPROCS(0))

	// 워커 풀 초기화
	initWorkerPool()

	var allResults []BenchmarkResult
	algorithms := []string{"quicksort", "parallel_quicksort", "mergesort", "parallel_mergesort"}

	// 1. 1천개 데이터 - 인메모리
	fmt.Println("1천개 데이터 (인메모리) 테스트 중...")
	data1k := generateRandomData(1000)

	for _, algo := range algorithms {
		for run := 1; run <= 3; run++ {
			fmt.Printf("  %s - 테스트 %d\n", algo, run)
			result := runBenchmark(algo, data1k, false)
			result.TestRun = run
			allResults = append(allResults, result)
			time.Sleep(50 * time.Millisecond) // 시스템 안정화 시간 단축
		}
	}

	// 2. 1만개 데이터 - 인메모리
	fmt.Println("1만개 데이터 (인메모리) 테스트 중...")
	data10k := generateRandomData(10000)

	for _, algo := range algorithms {
		for run := 1; run <= 3; run++ {
			fmt.Printf("  %s - 테스트 %d\n", algo, run)
			result := runBenchmark(algo, data10k, false)
			result.TestRun = run
			allResults = append(allResults, result)
			time.Sleep(50 * time.Millisecond)
		}
	}

	// 3. 10만개 데이터 - 파일 방식
	fmt.Println("10만개 데이터 (파일) 테스트 중...")
	data100k := generateRandomData(100000)
	filename := "test_data_100k.txt"

	if err := writeDataToFile(data100k, filename); err != nil {
		fmt.Printf("파일 쓰기 오류: %v\n", err)
		return
	}
	defer os.Remove(filename)

	for _, algo := range algorithms {
		for run := 1; run <= 3; run++ {
			fmt.Printf("  %s - 테스트 %d\n", algo, run)

			// 매번 파일에서 읽기
			fileData, err := readDataFromFile(filename)
			if err != nil {
				fmt.Printf("파일 읽기 오류: %v\n", err)
				continue
			}

			result := runBenchmark(algo, fileData, true)
			result.TestRun = run
			allResults = append(allResults, result)
			time.Sleep(50 * time.Millisecond)
		}
	}

	// 결과 저장
	fmt.Println("결과 저장 중...")

	if err := saveResultsToMarkdown(allResults); err != nil {
		fmt.Printf("마크다운 저장 오류: %v\n", err)
	} else {
		fmt.Println("benchmark_results.md 파일이 생성되었습니다.")
	}

	if err := saveResultsToJSON(allResults); err != nil {
		fmt.Printf("JSON 저장 오류: %v\n", err)
	} else {
		fmt.Println("benchmark_results.json 파일이 생성되었습니다.")
	}

	fmt.Println("벤치마크 완료!")
}
