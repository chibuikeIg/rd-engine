package core

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"

	"reversed-database.engine/config"
)

// Log structured storage
type LSS struct {
	File        *os.File
	ActiveSegID int
	KeyDirs     []*HashTable
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

	f, err := segment.CreateSegment(activeSegID, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC)

	if err != nil {
		log.Fatalln(err)
	}

	// Trigger compaction
	// Creates manifest file to trigger compaction
	if segments, err := segment.Segments(); err == nil && len(segments) > 1 {
		os.OpenFile(config.Manifest, os.O_CREATE, 0644)
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
	lss.KeyDirs[len(lss.KeyDirs)-1].Set(key, KeyDirValue{lss.ActiveSegID, byteOffset})

	return key, nil
}

func (lss *LSS) Get(key string) ([]byte, error) {
	var value []byte

	for i := len(lss.KeyDirs) - 1; i >= 0; i-- {

		// Get value position from index
		val, err := lss.KeyDirs[i].Get(key)
		if err != nil {
			continue
		}

		indexVal := val.(KeyDirValue)

		// Open file for reading
		segment := NewSegment()
		f, err := segment.CreateSegment(indexVal.FileId, os.O_RDONLY)

		if err != nil {
			return nil, err
		}

		ioReader := bufio.NewReader(f)

		// Set the position for the next read.
		_, err = f.Seek(indexVal.Offset, io.SeekStart)
		if err != nil {
			return nil, err
		}

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

		if len(value) != 0 {
			break
		}
	}

	return value, nil
}
