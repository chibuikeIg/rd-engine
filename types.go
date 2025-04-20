package main

import (
	"os"

	"reversed-database.engine/core"
)

type Data struct {
	Key         string
	Value       any
	HashTable   *core.HashTable
	StorageFile *os.File
}
