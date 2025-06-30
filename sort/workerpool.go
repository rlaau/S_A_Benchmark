package main

import (
	"runtime"
	"sync"
)

// 전역 워커 풀 (재사용을 위해)
var (
	workerPool     chan struct{}
	workerPoolOnce sync.Once
)

// 워커 풀 초기화
// * 채널 통한 세마포 구현.
func initWorkerPool() {
	workerPoolOnce.Do(func() {
		workerPool = make(chan struct{}, runtime.NumCPU())
	})
}
