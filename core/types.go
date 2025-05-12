package core

import (
	"os"
)

type KeyDirValue struct {
	FileId int
	Offset int64
}

type WriteRequest struct {
	Key         string
	Value       any
	HashTable   *HashTable
	StorageFile *os.File
}
