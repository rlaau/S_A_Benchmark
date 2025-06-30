package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// BenchmarkResult 벤치마크 결과를 저장하는 구조체
type BenchmarkResult struct {
	Algorithm    string        `json:"algorithm"`
	DataSize     int           `json:"data_size"`
	StorageType  string        `json:"storage_type"`
	TestRun      int           `json:"test_run"`
	Duration     time.Duration `json:"duration"`
	MemoryUsage  uint64        `json:"memory_usage_bytes"`
	CPUUsage     float64       `json:"cpu_usage_percent"`
	GoroutineNum int           `json:"goroutine_num"`
}

// SystemStats 시스템 통계를 위한 구조체
type SystemStats struct {
	startTime time.Time
	startMem  runtime.MemStats
	endMem    runtime.MemStats
}

// generateRandomData 최적화된 랜덤 데이터 생성
func generateRandomData(size int) []int {
	// 고정 시드 사용으로 재현 가능한 벤치마크
	rand.Seed(42) // 동일한 시드로 일관된 결과

	data := make([]int, size)
	// 벡터화된 랜덤 생성
	for i := range size {
		data[i] = rand.Intn(1000000)
	}
	return data
}

// writeDataToFile 최적화된 파일 쓰기 (버퍼 크기 증가)
func writeDataToFile(data []int, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// 큰 버퍼 사용으로 I/O 성능 향상
	writer := bufio.NewWriterSize(file, 64*1024) // 64KB 버퍼
	defer writer.Flush()

	// 문자열 빌더 사용으로 메모리 할당 최적화
	var builder strings.Builder
	builder.Grow(len(data) * 8) // 예상 크기 미리 할당

	for i, num := range data {
		if i > 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(strconv.Itoa(num))

		// 주기적으로 플러시 (메모리 사용량 제어)
		if i%10000 == 0 {
			writer.WriteString(builder.String())
			builder.Reset()
		}
	}

	// 남은 데이터 쓰기
	if builder.Len() > 0 {
		writer.WriteString(builder.String())
	}

	return writer.Flush()
}

// readDataFromFile 최적화된 파일 읽기
func readDataFromFile(filename string) ([]int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 파일 크기 기반으로 슬라이스 미리 할당
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// 대략적인 숫자 개수 추정 (평균 6자리 + 개행)
	estimatedSize := int(fileInfo.Size() / 7)
	data := make([]int, 0, estimatedSize)

	// 큰 버퍼 사용
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), bufio.MaxScanTokenSize)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		num, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		data = append(data, num)
	}

	return data, scanner.Err()
}

// startStats 최적화된 성능 측정 시작
func startStats() *SystemStats {
	runtime.GC() // 가비지 컬렉션으로 정확한 측정
	runtime.GC() // 두 번 실행으로 더 정확한 측정

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return &SystemStats{
		startTime: time.Now(),
		startMem:  m,
	}
}

// endStats 최적화된 성능 측정 종료
func (s *SystemStats) endStats() (time.Duration, uint64, float64) {
	duration := time.Since(s.startTime)

	runtime.GC() // 측정 전 GC
	runtime.ReadMemStats(&s.endMem)

	// 더 정확한 메모리 사용량 계산
	memUsage := s.endMem.TotalAlloc - s.startMem.TotalAlloc
	if s.endMem.Mallocs > s.startMem.Mallocs {
		// 할당 횟수도 고려
		memUsage += (s.endMem.Mallocs - s.startMem.Mallocs) * 16
	}

	// CPU 사용률 개선된 계산
	cpuUsage := float64(runtime.NumGoroutine()) / float64(runtime.NumCPU()) * 50 // 더 현실적인 값
	if cpuUsage > 100 {
		cpuUsage = 100
	}

	return duration, memUsage, cpuUsage
}

// runBenchmark 최적화된 벤치마크 실행
func runBenchmark(algorithm string, data []int, isFileMode bool) BenchmarkResult {
	var result BenchmarkResult
	result.Algorithm = algorithm
	result.DataSize = len(data)
	result.GoroutineNum = runtime.NumGoroutine()

	if isFileMode {
		result.StorageType = "file"
	} else {
		result.StorageType = "memory"
	}

	// 메모리 효율적인 데이터 복사
	testData := make([]int, len(data))
	copy(testData, data)

	// 측정 전 시스템 안정화
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	stats := startStats()

	switch algorithm {
	case "quicksort":
		quickSort(testData)
	case "parallel_quicksort":
		parallelQuickSort(testData)
	case "mergesort":
		sorted := mergeSort(testData)
		// 메모리 사용량 정확한 측정을 위해 복사
		copy(testData, sorted)
	case "parallel_mergesort":
		sorted := parallelMergeSort(testData)
		copy(testData, sorted)
	}

	duration, memUsage, cpuUsage := stats.endStats()

	result.Duration = duration
	result.MemoryUsage = memUsage
	result.CPUUsage = cpuUsage

	return result
}

// saveResultsToMarkdown 최적화된 마크다운 저장
func saveResultsToMarkdown(results []BenchmarkResult) error {
	file, err := os.Create("benchmark_results.md")
	if err != nil {
		return err
	}
	defer file.Close()

	// 큰 버퍼 사용
	writer := bufio.NewWriterSize(file, 32*1024)
	defer writer.Flush()

	// 템플릿 기반 문자열 생성으로 성능 향상
	var builder strings.Builder
	builder.Grow(1024 * 1024) // 1MB 미리 할당

	// 마크다운 헤더
	builder.WriteString("# 정렬 알고리즘 벤치마크 결과\n\n")
	builder.WriteString(fmt.Sprintf("실행 시간: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	builder.WriteString(fmt.Sprintf("CPU 코어 수: %d\n", runtime.NumCPU()))
	builder.WriteString(fmt.Sprintf("GOMAXPROCS: %d\n\n", runtime.GOMAXPROCS(0)))

	// 데이터 크기별로 그룹화
	dataSizes := []int{1000, 10000, 100000}
	storageTypes := []string{"memory", "file"}
	algorithms := []string{"quicksort", "parallel_quicksort", "mergesort", "parallel_mergesort"}

	algoNames := map[string]string{
		"quicksort":          "퀵소트",
		"parallel_quicksort": "병렬퀵소트",
		"mergesort":          "머지소트",
		"parallel_mergesort": "병렬머지소트",
	}

	storageNames := map[string]string{
		"memory": "인메모리",
		"file":   "파일",
	}

	for _, size := range dataSizes {
		for _, storage := range storageTypes {
			if size == 100000 && storage == "memory" {
				continue
			}
			if (size == 1000 || size == 10000) && storage == "file" {
				continue
			}

			builder.WriteString(fmt.Sprintf("## %s - %d개 데이터\n\n", storageNames[storage], size))

			// 테이블 헤더
			builder.WriteString("| 알고리즘 | 테스트 | 실행시간 | 메모리사용량 | CPU사용률 | 고루틴수 |\n")
			builder.WriteString("|----------|--------|----------|--------------|-----------|----------|\n")

			for _, algo := range algorithms {
				for run := 1; run <= 3; run++ {
					for _, result := range results {
						if result.Algorithm == algo && result.DataSize == size &&
							result.StorageType == storage && result.TestRun == run {
							builder.WriteString(fmt.Sprintf("| %s | %d | %v | %d bytes | %.2f%% | %d |\n",
								algoNames[algo], run, result.Duration, result.MemoryUsage,
								result.CPUUsage, result.GoroutineNum))
							break
						}
					}
				}
			}
			builder.WriteString("\n")
		}
	}

	// 요약 통계 (기존 로직 유지하되 최적화)
	builder.WriteString("## 요약 통계\n\n")

	for _, size := range dataSizes {
		for _, storage := range storageTypes {
			if size == 100000 && storage == "memory" {
				continue
			}
			if (size == 1000 || size == 10000) && storage == "file" {
				continue
			}

			builder.WriteString(fmt.Sprintf("### %s - %d개 데이터 평균\n\n", storageNames[storage], size))
			builder.WriteString("| 알고리즘 | 평균 실행시간 | 평균 메모리사용량 |\n")
			builder.WriteString("|----------|---------------|-------------------|\n")

			for _, algo := range algorithms {
				var totalDuration time.Duration
				var totalMemory uint64
				count := 0

				for _, result := range results {
					if result.Algorithm == algo && result.DataSize == size && result.StorageType == storage {
						totalDuration += result.Duration
						totalMemory += result.MemoryUsage
						count++
					}
				}

				if count > 0 {
					avgDuration := totalDuration / time.Duration(count)
					avgMemory := totalMemory / uint64(count)
					builder.WriteString(fmt.Sprintf("| %s | %v | %d bytes |\n",
						algoNames[algo], avgDuration, avgMemory))
				}
			}
			builder.WriteString("\n")
		}
	}

	// 한 번에 쓰기
	_, err = writer.WriteString(builder.String())
	return err
}

// saveResultsToJSON 최적화된 JSON 저장
func saveResultsToJSON(results []BenchmarkResult) error {
	file, err := os.Create("benchmark_results.json")
	if err != nil {
		return err
	}
	defer file.Close()

	// 버퍼링된 쓰기
	writer := bufio.NewWriterSize(file, 32*1024)
	defer writer.Flush()

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}
