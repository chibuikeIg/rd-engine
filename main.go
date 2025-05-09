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
	"strconv"
	"strings"

	"reversed-database.engine/config"
	"reversed-database.engine/core"
	"reversed-database.engine/utilities"
)

var bufferedChannel = make(chan Data, 50)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:1379")
	if err != nil {
		fmt.Println("Failed to bind to port 1379")
		os.Exit(1)
	}

	// Rebuilds HashTable
	keyDirs := rebuildHashTable()
	lss := core.NewLSS()
	lss.Ht = keyDirs[lss.ActiveSegID-1]

	go handleDataWrites(lss)
	go handleMerge(keyDirs, lss)

	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
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
				log.Fatal("unable to read connection")
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
				bufferedChannel <- Data{
					Key:         commands[1],
					Value:       strings.Trim(commands[2], "\b"),
					HashTable:   lss.Ht,
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
	defer lss.File.Close()

	// Checks file size and creates new segment
	// if full
	fInfo, err := lss.File.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if fInfo.Size() >= config.MFS {
		if err != nil {
			log.Fatal(err)
		}
		lss.ActiveSegID += 1
		segment := core.NewSegment()
		lss.File, err = segment.CreateSegment(lss.ActiveSegID)
		if err != nil {
			log.Fatal(err)
		}

		// Creates manifest file to trigger merge worker
		// Write should continue even if it fails to create manifest
		os.OpenFile("manifest.txt", os.O_CREATE, 0644)
	}

	for {
		data := <-bufferedChannel
		_, err := lss.Set(data.Key, data.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func handleMerge(keyDirs []*core.HashTable, lss *core.LSS) {
	for {

		_, err := os.Stat("manifest.txt")
		if os.IsNotExist(err) {
			continue
		}

		// Starts Merge process
		keyDirs = keyDirs[:lss.ActiveSegID-1]
		for _, keyDir := range keyDirs {
			keyDirKeys := keyDir.Keys()

			for _, key := range keyDirKeys {
				// Checks key doesn't already exist in active segment
				if slices.Contains(lss.Ht.Keys(), key) {
					continue
				}

				val, err := keyDir.Get(key)
				if err != nil {
					continue
				}

				indexVal := val.(core.KeyDirValue)

				// Converts SegmentID to string and Opens file for reading
				segmentIdAsString := "0" + strconv.Itoa(indexVal.FileId)
				filePath := config.SegmentStorageBasePath + "/" + segmentIdAsString + ".data.txt"
				f, err := os.Open(filePath)

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
				// Delete Segment file
				err = os.Remove(filePath)
				if err != nil {
					log.Fatalf("failed to remove segment %s after compaction", filePath)
				}
			}
		}

		// Delete Manifest file when compaction is complete
		err = os.Remove("manifest.txt")
		if err != nil {
			log.Fatal("failed to remove manifest file after compaction")
		}
	}
}

func rebuildHashTable() []*core.HashTable {
	// Reads storage directories
	dirEntries, err := os.ReadDir(config.SegmentStorageBasePath)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Properly create slice length. The below is inefficient
	keyDirs := make([]*core.HashTable, len(dirEntries))

	for _, de := range dirEntries {

		if de.IsDir() {
			continue
		}

		segmentInfo, err := de.Info()
		if err != nil {
			log.Fatal(err)
		}

		segment, err := os.Open(config.SegmentStorageBasePath + "/" + segmentInfo.Name())
		if err != nil {
			err = fmt.Errorf("unable to open segment, here is why: %s", err.Error())
			log.Fatal(err)
		}

		segmentId, err := utilities.GetSegmentIdFromFname(segmentInfo.Name())
		if err != nil {
			err = fmt.Errorf("unable to get segment id from %s, here is why: %s", segment.Name(), err.Error())
			log.Fatal(err)
		}

		// Instantiate Hashtable
		i := segmentId - 1
		keyDirs[i] = core.NewHashTable(50)

		ioReader := bufio.NewReader(segment)
		offset := int64(0)

		for {
			// Reads until newline ('\n')
			line, err := ioReader.ReadBytes('\n')
			trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))
			dataSlice := strings.SplitN(trimmedData, ",", 2)

			if len(dataSlice) == 2 {
				keyDirs[i].Set(dataSlice[0], core.KeyDirValue{FileId: segmentId, Offset: offset})
				offset += int64(len(line))
			}

			if err != nil {
				break
			}
		}
	}

	return keyDirs
}
