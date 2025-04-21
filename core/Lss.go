package core

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

// Log structured storage
type LSS struct {
	Ht          *HashTable
	File        *os.File
	ActiveSegID int
}

func NewLSS(ht *HashTable) *LSS {

	segment := NewSegment()
	activeSegID, err := segment.GetActiveSegmentID()
	if err != nil {
		log.Fatal(err)
	}

	f, err := segment.CreateSegment(activeSegID)

	if err != nil {
		log.Fatalln(err)
	}

	return &LSS{ht, f, activeSegID}
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
		log.Fatal(err)
	}

	if _, err := lss.File.Write(data); err != nil {
		return key, err
	}

	// Set Index data
	lss.Ht.Set(key, byteOffset)

	return key, nil
}

func (lss *LSS) Get(key string) ([]byte, error) {
	ioReader := bufio.NewReader(lss.File)

	// Get value position from index
	byteOffset := lss.Ht.Get(key)
	if byteOffset == nil {
		return nil, errors.New("no index found for this key")
	}

	// Set the position for the next read.
	_, err := lss.File.Seek(byteOffset.(int64), io.SeekStart)
	if err != nil {
		return nil, err
	}

	var value []byte

	for {
		// Read until newline ('\n')
		line, err := ioReader.ReadBytes('\n')

		trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
		dataSlice := strings.SplitN(trimmedData, ",", 2)

		if len(dataSlice) == 2 {

			if dataSlice[0] == key {
				value = []byte(dataSlice[1])
				break
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return value, nil
}
