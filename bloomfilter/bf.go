package main

import (
	"crypto/rand"
	"hash/fnv"
	"math"
)

// ====================================================================================
// 기본 블룸 필터 (기존과 동일)
// ====================================================================================

type BloomFilter struct {
	bitArray []uint64
	size     uint64
	numHash  uint
	numItems uint64
	hashSeed uint64
}

func NewBloomFilter(expectedItems uint64, falsePositiveRate float64) *BloomFilter {
	size := uint64(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))
	numHash := min(max(uint(float64(size)/float64(expectedItems)*math.Log(2)), 1), 15)

	wordCount := (size + 63) / 64

	seedBytes := make([]byte, 8)
	rand.Read(seedBytes)
	var seed uint64
	for i, b := range seedBytes {
		seed |= uint64(b) << (8 * i)
	}

	return &BloomFilter{
		bitArray: make([]uint64, wordCount),
		size:     size,
		numHash:  numHash,
		numItems: 0,
		hashSeed: seed,
	}
}

func (bf *BloomFilter) hash(data []byte, i uint) uint64 {
	h1 := fnv.New64a()
	h1.Write(data)
	seedBytes := make([]byte, 8)
	for j := range 8 {
		seedBytes[j] = byte(bf.hashSeed >> (8 * j))
	}
	h1.Write(seedBytes)
	hash1 := h1.Sum64()

	hash2 := hash1>>17 ^ hash1<<47 ^ uint64(i)*0x9e3779b97f4a7c15
	if hash2%2 == 0 {
		hash2++
	}

	return (hash1 + uint64(i)*hash2) % bf.size
}

func (bf *BloomFilter) Add(data []byte) {
	for i := uint(0); i < bf.numHash; i++ {
		pos := bf.hash(data, i)
		wordIndex := pos / 64
		bitIndex := pos % 64
		bf.bitArray[wordIndex] |= (1 << bitIndex)
	}
	bf.numItems++
}

func (bf *BloomFilter) Contains(data []byte) bool {
	for i := uint(0); i < bf.numHash; i++ {
		pos := bf.hash(data, i)
		wordIndex := pos / 64
		bitIndex := pos % 64
		if (bf.bitArray[wordIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) GetStats() (uint64, float64, float64) {
	setBits := uint64(0)
	for _, word := range bf.bitArray {
		setBits += uint64(popcount(word))
	}

	fillRatio := float64(setBits) / float64(bf.size)
	actualFPR := math.Pow(fillRatio, float64(bf.numHash))

	return setBits, fillRatio, actualFPR
}

func popcount(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}
