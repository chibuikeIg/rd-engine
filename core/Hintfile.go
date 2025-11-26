package core

import (
	"encoding/binary"
	"fmt"
	"os"

	"reversed-database.engine/config"
	"reversed-database.engine/utilities"
)

type HintFile struct{}

func NewHintFile() *HintFile {

	// Open hint file for writing
	return &HintFile{}
}

func (hf *HintFile) Write(keyDir KeyDir) error {

	formatedSegmentID := utilities.SegmentIDToString(keyDir.SegmentID)

	file := config.HintFileStoragePath + "/" + formatedSegmentID + ".data.hint.tmp"
	// Create a single instance of file
	f, err := os.Create(file)
	if err != nil {
		f.Close()
		return err
	}

	for _, buckets := range keyDir.HashTable.data {
		for _, kv := range buckets {
			binary.Write(f, binary.LittleEndian, int32(len(kv.Key)))
			f.Write([]byte(kv.Key))
			bucketVal := kv.Val.(IndexValue)
			binary.Write(f, binary.LittleEndian, int32(bucketVal.SegmentId))
			binary.Write(f, binary.LittleEndian, bucketVal.Offset)
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync temp file failed: %w", err)
	}

	f.Close()

	// rename temp file to final hint file
	finalFile := config.HintFileStoragePath + "/" + formatedSegmentID + ".data.hint"
	finalFileExist, err := os.Stat(finalFile)
	if err == nil && finalFileExist != nil {
		if err = os.Remove(finalFile); err != nil {
			return err
		}
	}

	err = os.Rename(file, finalFile)

	if err != nil {
		return err
	}

	return nil
}

func (hf *HintFile) Read(segmentId int) (*HashTable, error) {
	// Read existing hint files and rebuild KeyDirs
	formatedSegmentID := utilities.SegmentIDToString(segmentId)
	filePath := config.HintFileStoragePath + "/" + formatedSegmentID + ".data.hint"
	tmpFilePath := config.HintFileStoragePath + "/" + formatedSegmentID + ".data.hint.tmp"
	keyDirHashTable := NewHashTable(config.HashTableSize)

	_, err := os.Stat(tmpFilePath)
	if !os.IsNotExist(err) {
		return keyDirHashTable, fmt.Errorf("temp hint file %s exists, hint file write may be incomplete", tmpFilePath)
	}

	_, err = os.Stat(filePath)
	if os.IsNotExist(err) {
		return keyDirHashTable, fmt.Errorf("hint file %s does not exist", filePath)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return keyDirHashTable, err
	}
	defer f.Close()

	for {
		var keySize int32
		err := binary.Read(f, binary.LittleEndian, &keySize)
		if err != nil {
			break
		}
		key := make([]byte, keySize)
		_, err = f.Read(key)
		if err != nil {
			return keyDirHashTable, err
		}
		var segmentId int32
		err = binary.Read(f, binary.LittleEndian, &segmentId)
		if err != nil {
			return keyDirHashTable, err
		}
		var offset int64
		err = binary.Read(f, binary.LittleEndian, &offset)
		if err != nil {
			return keyDirHashTable, err
		}

		keyDirHashTable.Set(string(key), IndexValue{
			SegmentId: int(segmentId),
			Offset:    offset,
		})
	}

	return keyDirHashTable, nil
}
