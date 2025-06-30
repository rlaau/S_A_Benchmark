package main

// quickSort 최적화 (하이브리드 접근)
func quickSort(arr []int) {
	if len(arr) < 2 {
		return
	}
	quickSortHelper(arr, 0, len(arr)-1)
}

func quickSortHelper(arr []int, low, high int) {
	for low < high {
		size := high - low + 1

		// 작은 배열에는 삽입정렬 사용 (더 빠름)
		if size <= 16 {
			insertionSort(arr, low, high)
			return
		}

		// 3-way 파티셔닝으로 중복값 처리 최적화
		lt, gt := partition3Way(arr, low, high)

		// 꼬리 재귀 최적화 (더 작은 부분을 재귀로)
		if lt-low < high-gt {
			quickSortHelper(arr, low, lt-1)
			low = gt + 1 // 꼬리 재귀 최적화
		} else {
			quickSortHelper(arr, gt+1, high)
			high = lt - 1 // 꼬리 재귀 최적화
		}
	}
}

// 3-way 파티셔닝 (중복값 최적화)
func partition3Way(arr []int, low, high int) (int, int) {
	// 중앙값을 피벗으로 선택 (더 균형잡힌 분할)
	medianOfThree(arr, low, (low+high)/2, high)
	pivot := arr[low]

	lt := low      // arr[low..lt-1] < pivot
	i := low + 1   // arr[lt..i-1] == pivot
	gt := high + 1 // arr[gt..high] > pivot

	for i < gt {
		if arr[i] < pivot {
			arr[lt], arr[i] = arr[i], arr[lt]
			lt++
			i++
		} else if arr[i] > pivot {
			gt--
			arr[i], arr[gt] = arr[gt], arr[i]
		} else {
			i++
		}
	}

	return lt, gt - 1
}

// 중앙값 선택 (피벗 최적화)
func medianOfThree(arr []int, a, b, c int) {
	if arr[a] > arr[b] {
		arr[a], arr[b] = arr[b], arr[a]
	}
	if arr[b] > arr[c] {
		arr[b], arr[c] = arr[c], arr[b]
	}
	if arr[a] > arr[b] {
		arr[a], arr[b] = arr[b], arr[a]
	}
	// 중앙값을 첫 번째 위치로
	arr[a], arr[b] = arr[b], arr[a]
}

// 삽입정렬 (작은 배열 최적화)
func insertionSort(arr []int, low, high int) {
	for i := low + 1; i <= high; i++ {
		key := arr[i]
		j := i - 1

		// 이진 삽입으로 최적화
		for j >= low && arr[j] > key {
			arr[j+1] = arr[j]
			j--
		}
		arr[j+1] = key
	}
}

// 기본 파티션 (호환성 유지)
func partition(arr []int, low, high int) int {
	pivot := arr[high]
	i := low - 1

	for j := low; j < high; j++ {
		if arr[j] <= pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}
	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}
