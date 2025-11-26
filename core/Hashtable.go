package core

import (
	"errors"
	"slices"
	"unicode/utf8"
)

type HashTableBucket struct {
	Key string
	Val any
}

type HashTable struct {
	data [][]HashTableBucket
}

func NewHashTable(size int) *HashTable {

	data := make([][]HashTableBucket, size)

	return &HashTable{data}

}

func (ht HashTable) _hash(key string) int {

	hash := 0

	for i := 0; i < len(key); i++ {
		hash = (hash + int(charCode(key, i))*i) % len(ht.data)
	}

	return hash

}

func (ht HashTable) Set(key string, val any) {

	address := ht._hash(key)

	if ht.data[address] == nil {
		ht.data[address] = []HashTableBucket{}
	}

	ht.data[address] = append(ht.data[address], HashTableBucket{key, val})

}

func (ht HashTable) Get(key string) (any, error) {

	address := ht._hash(key)

	if ht.data[address] != nil {

		currentBucket := ht.data[address]

		if len(currentBucket) == 1 && currentBucket[0].Key == key {
			return currentBucket[0].Val, nil
		}

		for i := len(currentBucket) - 1; i >= 0; i-- {
			if currentBucket[i].Key == key {
				return currentBucket[i].Val, nil
			}
		}

	}

	return nil, errors.New("no index found for this key")

}

func (ht HashTable) Keys() []string {

	var foundKeys []string

	if len(ht.data) == 0 {

		return foundKeys
	}

	for _, buckets := range ht.data {

		if len(buckets) == 1 {

			foundKeys = append(foundKeys, buckets[0].Key)

		} else {

			for _, bucket := range buckets {
				if !slices.Contains(foundKeys, bucket.Key) {
					foundKeys = append(foundKeys, bucket.Key)
				}
			}

		}

	}

	return foundKeys

}

func charCode(str string, index int) rune {

	if indexSum := index + 1; indexSum <= len(str) {

		r, _ := utf8.DecodeRuneInString(str[index:indexSum])

		return r
	}

	return 0

}
