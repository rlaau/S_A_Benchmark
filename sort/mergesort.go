package main

// mergeSort 최적화된 머지소트
func mergeSort(arr []int) []int {
	if len(arr) <= 1 {
		return arr
	}

	// 작은 배열은 삽입정렬 사용
	if len(arr) <= 16 {
		result := make([]int, len(arr))
		copy(result, arr)
		insertionSort(result, 0, len(result)-1)
		return result
	}

	mid := len(arr) / 2
	left := mergeSort(arr[:mid])
	right := mergeSort(arr[mid:])

	return merge(left, right)
}

// merge 최적화된 병합
func merge(left, right []int) []int {
	result := make([]int, 0, len(left)+len(right))
	i, j := 0, 0

	// 더 효율적인 병합 루프
	for i < len(left) && j < len(right) {
		if left[i] <= right[j] {
			result = append(result, left[i])
			i++
		} else {
			result = append(result, right[j])
			j++
		}
	}

	// 남은 요소들 한 번에 추가
	if i < len(left) {
		result = append(result, left[i:]...)
	}
	if j < len(right) {
		result = append(result, right[j:]...)
	}

	return result
}
