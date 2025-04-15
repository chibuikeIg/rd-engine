package main

import (
	"os"

	"reversed-database.engine/storage"
)

type Data struct {
	Key         string
	Value       any
	HashTable   *storage.HashTable
	StorageFile *os.File
}
