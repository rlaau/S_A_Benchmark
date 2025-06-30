package main

import (
	"runtime"
	"sync"
)

// parallelMergeSort 최적화된 병렬 머지소트 (버그 수정)
func parallelMergeSort(arr []int) []int {
	// 워커 풀 초기화 확인
	initWorkerPool()
	return parallelMergeSortHelper(arr, runtime.NumCPU())
}

func parallelMergeSortHelper(arr []int, depth int) []int {
	if len(arr) <= 1 {
		return arr
	}

	// 동적 임계값 사용
	threshold := getOptimalThreshold(len(arr), len(arr))

	if depth <= 1 || len(arr) < threshold {
		return mergeSort(arr)
	}

	mid := len(arr) / 2
	var left, right []int

	// ✅ 수정: 각 고루틴이 독립적으로 워커 풀 관리
	var wg sync.WaitGroup
	wg.Add(2)

	// 첫 번째 고루틴 - 왼쪽 부분
	go func() {
		defer wg.Done()

		select {
		case workerPool <- struct{}{}: // 슬롯 획득 시도
			defer func() { <-workerPool }() // ✅ 확실히 반환
			left = parallelMergeSortHelper(arr[:mid], depth/2)
		default:
			// 슬롯 없으면 순차 처리
			left = mergeSort(arr[:mid])
		}
	}()

	// 두 번째 고루틴 - 오른쪽 부분
	go func() {
		defer wg.Done()

		select {
		case workerPool <- struct{}{}: // 슬롯 획득 시도
			defer func() { <-workerPool }() // ✅ 확실히 반환
			right = parallelMergeSortHelper(arr[mid:], depth/2)
		default:
			// 슬롯 없으면 순차 처리
			right = mergeSort(arr[mid:])
		}
	}()

	wg.Wait()
	return merge(left, right)
}

// ✅ 추가: 워커 풀 상태 확인 함수 (디버깅용)
func getWorkerPoolStatus() (used int, capacity int) {
	if workerPool == nil {
		return 0, 0
	}
	return len(workerPool), cap(workerPool)
}

// ✅ 추가: 워커 풀 강제 정리 함수 (비상시 사용)
func resetWorkerPool() {
	if workerPool == nil {
		return
	}

	// 기존 채널의 모든 슬롯 정리
	for len(workerPool) > 0 {
		<-workerPool
	}
}

// ✅ 추가: 안전한 병렬 정렬 래퍼 (에러 복구 포함)
func safeParallelQuickSort(arr []int) {
	defer func() {
		if r := recover(); r != nil {
			// 패닉 발생 시 워커 풀 정리 후 순차 정렬로 폴백
			resetWorkerPool()
			quickSort(arr)
		}
	}()

	parallelQuickSort(arr)
}

func safeParallelMergeSort(arr []int) []int {
	defer func() {
		if r := recover(); r != nil {
			// 패닉 발생 시 워커 풀 정리
			resetWorkerPool()
		}
	}()

	result := parallelMergeSort(arr)

	// nil 체크 및 폴백
	if result == nil {
		return mergeSort(arr)
	}

	return result
}
