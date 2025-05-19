package core

import (
	"os"
)

type IndexValue struct {
	SegmentId int
	Offset    int64
}

type KeyDir struct {
	SegmentID int
	HashTable *HashTable
}

type WriteRequest struct {
	Key         string
	Value       any
	HashTable   *HashTable
	StorageFile *os.File
}
