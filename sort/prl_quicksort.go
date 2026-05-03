package main

import (
	"runtime"
	"sync"
)

// parallelQuickSort 최적화된 병렬 퀵소트 (버그 수정)
func parallelQuickSort(arr []int) {
	if len(arr) < 2 {
		return
	}

	initWorkerPool()
	parallelQuickSortHelper(arr, 0, len(arr)-1, runtime.NumCPU())
}

func parallelQuickSortHelper(arr []int, low, high, depth int) {
	size := high - low + 1

	// 동적 임계값 계산
	threshold := getOptimalThreshold(len(arr), size)

	if low < high {
		if depth <= 1 || size <= threshold {
			quickSortHelper(arr, low, high)
			return
		}

		// 3-way 파티셔닝 사용
		lt, gt := partition3Way(arr, low, high)

		var wg sync.WaitGroup

		// 첫 번째 고루틴 - 왼쪽 부분
		if !tryRunMeasuredGoroutine(&wg, func() {
			parallelQuickSortHelper(arr, low, lt-1, depth/2)
		}) {
			quickSortHelper(arr, low, lt-1)
		}

		// 두 번째 고루틴 - 오른쪽 부분
		if !tryRunMeasuredGoroutine(&wg, func() {
			parallelQuickSortHelper(arr, gt+1, high, depth/2)
		}) {
			quickSortHelper(arr, gt+1, high)
		}

		wg.Wait()
	}
}

// 동적 임계값 계산
func getOptimalThreshold(totalSize, currentSize int) int {
	switch {
	case totalSize < 1000:
		return totalSize // 작은 데이터는 병렬처리 안함
	case totalSize < 10000:
		return 300
	case totalSize < 100000:
		return 800
	default:
		return 1500
	}
}
