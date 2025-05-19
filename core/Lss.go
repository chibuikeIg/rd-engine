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
	Segment     *os.File
	ActiveSegID int
	KeyDirs     []KeyDir
	Manifest    *os.File
}

func NewLSS() *LSS {

	segment := NewSegment()
	activeSegID, err := segment.GetActiveSegmentID()
	if err != nil {
		log.Fatal(err)
	}
	segmentF, err := segment.CreateSegment(activeSegID, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC)

	if err != nil {
		log.Fatalln(err)
	}

	// Trigger compaction
	// Creates manifest file to trigger compaction
	segments, err := segment.Segments()
	var manifest *os.File
	if err == nil && len(segments) > 1 {
		_, err := os.Stat(config.Manifest)
		if os.IsNotExist(err) {
			manifest, err = os.OpenFile(config.Manifest, os.O_CREATE, 0644)
			if err != nil {
				log.Println("unable to create manifest for compaction")
			}
		}
	}

	return &LSS{Segment: segmentF, ActiveSegID: activeSegID, Manifest: manifest}
}

func (lss *LSS) Set(key string, value any) (string, error) {

	// Get Byte offset for indexing
	byteOffset, err := lss.Segment.Seek(0, io.SeekEnd)
	if err != nil {
		return key, err
	}

	// Convert data to bytes and combine both
	keyInByte := []byte(key)
	val, err := json.Marshal(value)
	if err != nil {
		return key, err
	}
	data := bytes.Join([][]byte{keyInByte, val}, []byte(","))
	data = append(data, '\n')

	if _, err := lss.Segment.Write(data); err != nil {
		return key, err
	}

	// Set Index data
	keydir := lss.KeyDirs[len(lss.KeyDirs)-1]
	keydir.HashTable.Set(key, IndexValue{lss.ActiveSegID, byteOffset})

	return key, nil
}

func (lss *LSS) Get(key string) ([]byte, error) {
	var value []byte

	for i := len(lss.KeyDirs) - 1; i >= 0; i-- {

		// Get value position from index
		val, err := lss.KeyDirs[i].HashTable.Get(key)
		if err != nil {
			continue
		}

		indexVal := val.(IndexValue)

		// Open file for reading
		segment := NewSegment()
		f, err := segment.CreateSegment(indexVal.SegmentId, os.O_RDONLY)

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
