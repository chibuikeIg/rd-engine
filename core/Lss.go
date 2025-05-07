package core

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"reversed-database.engine/config"
)

// Log structured storage
type LSS struct {
	Ht          *HashTable
	File        *os.File
	ActiveSegID int
}

type KeyDirValue struct {
	FileId int
	Offset int64
}

func NewLSS() *LSS {

	segment := NewSegment()
	activeSegID, err := segment.GetActiveSegmentID()
	if err != nil {
		log.Fatal(err)
	}

	f, err := segment.CreateSegment(activeSegID)

	if err != nil {
		log.Fatalln(err)
	}

	return &LSS{File: f, ActiveSegID: activeSegID}
}

func (lss *LSS) Set(key string, value any) (string, error) {

	// Get Byte offset for indexing
	byteOffset, err := lss.File.Seek(0, io.SeekEnd)
	if err != nil {
		return key, err
	}

	// Convert data to bytes and combine both
	keyInByte := []byte(key)
	val, err := json.Marshal(value)
	data := bytes.Join([][]byte{keyInByte, val}, []byte(","))
	data = append(data, '\n')
	if err != nil {
		return key, err
	}

	if _, err := lss.File.Write(data); err != nil {
		return key, err
	}

	// Set Index data
	lss.Ht.Set(key, KeyDirValue{lss.ActiveSegID, byteOffset})

	return key, nil
}

func (lss *LSS) Get(key string) ([]byte, error) {

	// Get value position from index
	val, err := lss.Ht.Get(key)
	if err != nil {
		return nil, err
	}

	indexVal := val.(KeyDirValue)
	// Open file for reading
	formatedSegmentID := "0" + strconv.Itoa(indexVal.FileId)
	filePath := config.SegmentStorageBasePath + "/" + formatedSegmentID + ".data.txt"
	f, err := os.Open(filePath)

	if err != nil {
		return nil, err
	}
	ioReader := bufio.NewReader(f)

	// Set the position for the next read.
	_, err = f.Seek(indexVal.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var value []byte

	for {
		// Read until newline ('\n')
		line, err := ioReader.ReadBytes('\n')

		trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
		dataSlice := strings.SplitN(trimmedData, ",", 2)

		if len(dataSlice) == 2 && dataSlice[0] == key {
			value = []byte(dataSlice[1])
			break
		}

		if err != nil {
			return nil, err
		}
	}

	return value, nil
}
