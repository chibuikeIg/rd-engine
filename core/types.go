package core

import (
	"os"
)

type IndexValue struct {
	FileId int
	Offset int64
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
