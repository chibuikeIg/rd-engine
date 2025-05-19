package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"reversed-database.engine/config"
	"reversed-database.engine/core"
	"reversed-database.engine/utilities"
)

var writeReqests = make(chan core.WriteRequest, config.WriteRequestBufferSize)
var toDeleteSegmentQueue = make(chan string, config.ToDeleteSegmentBufferSize)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:1379")
	if err != nil {
		fmt.Println("Failed to bind to port 1379")
		os.Exit(1)
	}

	// Rebuilds HashTable
	lss := core.NewLSS()
	lss.KeyDirs = rebuildHashTable()

	go handleDataWrites(lss)
	go handleMerge(lss)
	go deleteSegments()

	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go readConn(conn, lss)
	}
}

func readConn(conn net.Conn, lss *core.LSS) {
	conn.Write([]byte("Connected\r\n"))

	for {
		bufReader := bufio.NewReader(conn)

		for {
			cmd, err := bufReader.ReadBytes('\n')
			if err != nil {
				log.Println("unable to read connection")
				return
			}

			cmd = bytes.TrimSuffix(cmd, []byte("\n"))
			cmd = bytes.TrimSuffix(cmd, []byte("\r"))
			cmd = bytes.Trim(cmd, "\u0008")
			cmd = bytes.Trim(cmd, "\b")
			commands := strings.SplitN(string(cmd), " ", 3)

			if len(commands) == 0 {
				conn.Write([]byte("no valid commands provided\r\n"))
			}

			if commands[0] != "set" && commands[0] != "get" && commands[0] != "delete" {
				conn.Write([]byte("no valid commands provided\r\n"))
			}

			if len(commands) == 3 && commands[0] == "set" {
				writeReqests <- core.WriteRequest{
					Key:   commands[1],
					Value: strings.Trim(commands[2], "\b"),
				}
			}

			if len(commands) == 2 && commands[0] == "get" {
				result, err := lss.Get(commands[1])
				result = bytes.Trim(result, "\"")
				if err != nil {
					result = []byte(err.Error())
				} else if len(result) == 0 {
					result = []byte("no record found")
				}

				result = append(result, '\r', '\n')
				conn.Write(result)
			}

			if len(commands) == 2 && commands[0] == "delete" {

				writeReqests <- core.WriteRequest{
					Key:   commands[1],
					Value: "",
				}

				conn.Write([]byte("deleted record"))
			}

		}
	}
}

// Handle write serialization
func handleDataWrites(lss *core.LSS) {

	for data := range writeReqests {
		// Checks file size and creates new segment
		// if full
		fInfo, err := lss.Segment.Stat()
		if err != nil {
			log.Fatal(err)
		}

		if fInfo.Size() >= config.MFS {
			lss.ActiveSegID += 1
			segment := core.NewSegment()
			lss.Segment, err = segment.CreateSegment(lss.ActiveSegID, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC)
			if err != nil {
				log.Fatal(err)
			}
			lss.KeyDirs = append(lss.KeyDirs, core.KeyDir{SegmentID: lss.ActiveSegID, HashTable: core.NewHashTable(config.HashTableSize)})

			// Creates manifest file to trigger merge worker
			// Write should continue even if it fails to create manifest
			_, err := os.Stat(config.Manifest)
			if os.IsNotExist(err) {
				lss.Manifest, _ = os.OpenFile(config.Manifest, os.O_CREATE, 0644)
			}
		}

		_, err = lss.Set(data.Key, data.Value)
		if err != nil {
			log.Printf("failed to write data. Here is why: %v", err)
			continue
		}

	}
}

func handleMerge(lss *core.LSS) {
	for {

		// Wait for 3secs before checking for manifest file
		time.Sleep(5 * time.Second)

		_, err := os.Stat(config.Manifest)
		if os.IsNotExist(err) {
			continue
		}

		index := len(lss.KeyDirs) - 1
		activeKeyDir := lss.KeyDirs[index]
		keyDirs := lss.KeyDirs[:index]
		// Starts Merge process
		for i := len(keyDirs) - 1; i >= 0; i-- {
			keyDirKeys := keyDirs[i].HashTable.Keys()

			segment := core.NewSegment()
			segmentF, err := segment.CreateSegment(keyDirs[i].SegmentID, os.O_RDWR)
			if err != nil {
				log.Printf("unable to open segment. Here is why: %v", err)
				continue
			}

			for _, key := range keyDirKeys {
				// Checks key doesn't already exist in active segment
				if slices.Contains(activeKeyDir.HashTable.Keys(), key) {
					continue
				}

				val, err := keyDirs[i].HashTable.Get(key)
				if err != nil {
					continue
				}

				indexVal := val.(core.IndexValue)

				// Set the position for the next read.
				segmentF.Seek(indexVal.Offset, io.SeekStart)
				ioReader := bufio.NewReader(segmentF)

				var data string

				for {
					// Reads until newline ('\n')
					line, err := ioReader.ReadBytes('\n')

					trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
					dataSlice := strings.SplitN(trimmedData, ",", 2)
					if len(dataSlice) == 2 && dataSlice[0] == key && dataSlice[1] != "" {
						trimmed := strings.Trim(dataSlice[1], "\r")
						data = strings.Trim(trimmed, "\"")
						break
					}

					if err != nil {
						break
					}
				}

				// Writes back data to active segment
				if data != "" {
					writeReqests <- core.WriteRequest{
						Key:   key,
						Value: data,
					}
				}

			}

			keyDirs = keyDirs[:i]

			if err := segmentF.Close(); err != nil {
				log.Printf("unable to close file %s: %v", segmentF.Name(), err)
			}

			// Queue Segment file for deletion
			toDeleteSegmentQueue <- segmentF.Name()
		}

		lss.KeyDirs = append(keyDirs, activeKeyDir)

		// Delete Manifest file when compaction is complete
		if err := lss.Manifest.Close(); err != nil {
			log.Printf("unable to close manifest file %v", err)
		}

		err = os.Remove(config.Manifest)
		if err != nil {
			log.Printf("unable to remove manifest file after compaction %v", err)
			continue
		}
	}
}

func rebuildHashTable() []core.KeyDir {

	dirEntries, err := os.ReadDir(config.SegmentStorageBasePath)

	if err != nil {
		log.Fatal(err)
	}

	keyDirs := []core.KeyDir{}
	segment := core.NewSegment()

	for _, seg := range dirEntries {

		if seg.IsDir() {
			continue
		}

		segmentID, err := utilities.GetSegmentIdFromFname(seg.Name())
		if err != nil {
			log.Fatal(err)
		}

		segmentF, err := segment.CreateSegment(segmentID, os.O_RDWR)
		if err != nil {
			log.Fatal(err)
		}

		// Instantiate Hashtable
		keyDirHashTable := core.NewHashTable(config.HashTableSize)
		ioReader := bufio.NewReader(segmentF)
		offset := int64(0)

		for {
			// Reads until newline ('\n')
			line, err := ioReader.ReadBytes('\n')
			trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
			dataSlice := strings.SplitN(trimmedData, ",", 2)

			if len(dataSlice) == 2 {
				keyDirHashTable.Set(dataSlice[0], core.IndexValue{SegmentId: segmentID, Offset: offset})
				offset += int64(len(line))
			}

			if err != nil {
				break
			}
		}
		keyDirs = append(keyDirs, core.KeyDir{SegmentID: segmentID, HashTable: keyDirHashTable})
		if err := segmentF.Close(); err != nil {
			log.Printf("unable to close segment file during hashtable rebuild %s: %v", segmentF.Name(), err)
		}
	}

	return keyDirs
}

func deleteSegments() {
	for segment := range toDeleteSegmentQueue {
		time.Sleep(10 * time.Second)

		err := os.Remove(segment)
		if err != nil {
			log.Printf("unable to remove segment %s after compaction, here's why %v", segment, err)
			continue
		}
	}
}
