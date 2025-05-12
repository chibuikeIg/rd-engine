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

var writeReqests = make(chan core.WriteRequest, 50)

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
	// go handleMerge(lss)

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

			if commands[0] != "set" && commands[0] != "get" {
				conn.Write([]byte("no valid commands provided\r\n"))
			}

			if len(commands) == 3 && commands[0] == "set" {
				writeReqests <- core.WriteRequest{
					Key:         commands[1],
					Value:       strings.Trim(commands[2], "\b"),
					StorageFile: lss.File,
				}
			}

			if len(commands) == 2 && commands[0] == "get" {
				result, err := lss.Get(commands[1])
				if err != nil {
					result = []byte(err.Error())
				} else if result == nil {
					result = []byte("no record found")
				}

				result = append(result, '\r', '\n')
				conn.Write(result)
			}

		}
	}
}

// Handle write serialization
func handleDataWrites(lss *core.LSS) {
	for {
		// Checks file size and creates new segment
		// if full
		fInfo, err := lss.File.Stat()
		if err != nil {
			log.Fatal(err)
		}

		if fInfo.Size() >= config.MFS {
			lss.ActiveSegID += 1
			segment := core.NewSegment()
			lss.File, err = segment.CreateSegment(lss.ActiveSegID, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC)
			if err != nil {
				log.Fatal(err)
			}
			lss.KeyDirs = append(lss.KeyDirs, core.NewHashTable(50))

			// Creates manifest file to trigger merge worker
			// Write should continue even if it fails to create manifest
			_, err := os.Stat(config.Manifest)
			if os.IsNotExist(err) {
				os.OpenFile(config.Manifest, os.O_CREATE, 0644)
			}
		}

		data := <-writeReqests
		_, err = lss.Set(data.Key, data.Value)
		if err != nil {
			log.Printf("failed to write data. Here is why: %s", err)
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

		// Starts Merge process
		index := len(lss.KeyDirs) - 1
		activeKeyDir := lss.KeyDirs[index]
		keyDirs := lss.KeyDirs[:index]

		for i := len(keyDirs) - 1; i >= 0; i-- {

			keyDirKeys := keyDirs[i].Keys()

			for _, key := range keyDirKeys {
				// Checks key doesn't already exist in active segment
				if slices.Contains(activeKeyDir.Keys(), key) {
					continue
				}

				val, err := keyDirs[i].Get(key)
				if err != nil {
					continue
				}

				indexVal := val.(core.KeyDirValue)

				segment := core.NewSegment()
				f, err := segment.CreateSegment(indexVal.FileId, os.O_RDWR)
				if err != nil {
					log.Fatal(err)
				}

				if err != nil {
					continue
				}

				// Set the position for the next read.
				_, err = f.Seek(indexVal.Offset, io.SeekStart)
				if err != nil {
					continue
				}

				ioReader := bufio.NewReader(f)
				var data string

				for {
					// Reads until newline ('\n')
					line, err := ioReader.ReadBytes('\n')

					trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
					dataSlice := strings.SplitN(trimmedData, ",", 2)
					if len(dataSlice) == 2 && dataSlice[0] == key {
						data = dataSlice[1]
						break
					}

					if err != nil {
						break
					}
				}
				// Writes back data to active segment
				lss.Set(key, data)
			}

			// Delete Segment file
			// filePath := config.SegmentStorageBasePath + "/" + f.Name()
			// err = os.Remove(filePath)
			// if err != nil {
			// 	log.Fatalf("failed to remove segment %s after compaction", filePath)
			// }

		}
		// Delete Manifest file when compaction is complete
		err = os.Remove(config.Manifest)
		if err != nil {
			log.Println("failed to remove manifest file after compaction")
			continue
		}
	}
}

func rebuildHashTable() []*core.HashTable {

	dirEntries, err := os.ReadDir(config.SegmentStorageBasePath)

	if err != nil {
		log.Fatal(err)
	}

	keyDirs := []*core.HashTable{}
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
		segKeyDir := core.NewHashTable(50)
		ioReader := bufio.NewReader(segmentF)
		offset := int64(0)

		for {
			// Reads until newline ('\n')
			line, err := ioReader.ReadBytes('\n')
			trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
			dataSlice := strings.SplitN(trimmedData, ",", 2)

			if len(dataSlice) == 2 {
				segKeyDir.Set(dataSlice[0], core.KeyDirValue{FileId: segmentID, Offset: offset})
				offset += int64(len(line))
			}

			if err != nil {
				break
			}
		}
		keyDirs = append(keyDirs, segKeyDir)
	}

	return keyDirs
}
