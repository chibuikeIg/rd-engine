package storage

import (
	"unicode/utf8"
)

type HashTableBucket struct {
	key string
	val any
}

type HashTable struct {
	data []any
}

func NewHashTable(size int) *HashTable {

	data := make([]any, size)

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

	ht.data[address] = append(ht.data[address].([]HashTableBucket), HashTableBucket{key, val})

}

func (ht HashTable) Get(key string) any {

	address := ht._hash(key)

	if ht.data[address] != nil {

		currentBucket := ht.data[address].([]HashTableBucket)

		if len(currentBucket) == 1 {
			return currentBucket[0].val
		}

		for i := len(currentBucket) - 1; i >= 0; i-- {
			if currentBucket[i].key == key {
				return currentBucket[i].val
			}
		}

	}

	return nil

}

func (ht HashTable) Keys() []string {

	var foundKeys []string

	if len(ht.data) == 0 {

		return foundKeys
	}

	for _, buckets := range ht.data {

		buckets := buckets.([]HashTableBucket)

		if len(buckets) == 1 {

			foundKeys = append(foundKeys, buckets[0].key)

		} else {

			for _, bucket := range buckets {

				foundKeys = append(foundKeys, bucket.key)

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
