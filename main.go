package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"reversed-database.engine/config"
	"reversed-database.engine/core"
	"reversed-database.engine/utilities"
)

var writeRequests = make(chan core.WriteRequest, config.WriteRequestBufferSize)
var toDeleteSegmentQueue = make(chan string, config.ToDeleteSegmentBufferSize)

var writeCounter atomic.Int64

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:1379")
	if err != nil {
		log.Printf("Failed to bind to port 1379")
		os.Exit(1)
	}

	fmt.Println("Listening on port 1379...")

	// Create segments and hint files folder if they doesn't exist
	if _, err := os.Stat(config.SegmentStorageBasePath); os.IsNotExist(err) {
		err := os.MkdirAll(config.SegmentStorageBasePath, os.ModePerm)

		if err != nil {
			log.Fatal("unable to create segment directory")
		}
	}

	if _, err := os.Stat(config.HintFileStoragePath); os.IsNotExist(err) {
		err := os.MkdirAll(config.HintFileStoragePath, os.ModePerm)

		if err != nil {
			log.Fatal("unable to create hint file storage directory")
		}
	}

	// Rebuilds HashTable
	lss := core.NewLSS()
	lss.KeyDirs = rebuildHashTable()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
		l.Close()
		wg.Wait()
	}()

	wg.Add(4)
	go handleDataWrites(ctx, lss, writeRequests, wg)
	go handleMerge(ctx, lss, wg, writeRequests, toDeleteSegmentQueue)
	go deleteSegments(ctx, toDeleteSegmentQueue, wg)
	go persistKeyDirs(ctx, lss, wg)

	// Accept incoming connections
	for {

		select {
		case <-ctx.Done():
			return
		default:
			wg.Add(1)
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %s", err.Error())
				continue
			}
			go readConn(ctx, conn, lss, writeRequests, wg)
		}

	}
}

func readConn(ctx context.Context, conn net.Conn, lss *core.LSS, writeRequests chan<- core.WriteRequest, wg *sync.WaitGroup) {

	defer wg.Done()
	defer conn.Close()
	conn.Write([]byte("Connected\r\n"))

	bufReader := bufio.NewReader(conn)

	for {

		select {
		case <-ctx.Done():
			return
		default:
			cmd, err := bufReader.ReadBytes('\n')
			if err != nil {
				log.Printf("unable to read connection")
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

				select {
				case writeRequests <- core.WriteRequest{
					Key:   commands[1],
					Value: strings.Trim(commands[2], "\b"),
				}:
				case <-ctx.Done():
					return
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

				select {
				case <-ctx.Done():
					return
				case writeRequests <- core.WriteRequest{
					Key:   commands[1],
					Value: "",
				}:
				}

				conn.Write([]byte("deleted record\r\n"))
			}
		}

	}
}

// Handle write serialization
func handleDataWrites(ctx context.Context, lss *core.LSS, writeRequests <-chan core.WriteRequest, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-writeRequests:
			if !ok {
				return
			}

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

			// Store data in LSS
			_, err = lss.Set(data.Key, data.Value)
			if err != nil {
				log.Printf("failed to write data. Here is why: %v", err)
				continue
			}
			// Increment write counter
			writeCounter.Add(1)
		}
	}

}

func handleMerge(ctx context.Context, lss *core.LSS, wg *sync.WaitGroup, writeRequests chan<- core.WriteRequest, toDeleteSegmentQueue chan<- string) {

	defer wg.Done()

	for {

		select {
		case <-ctx.Done():
			return
		default:
			// Wait for some secs before checking for manifest file
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
				segmentID := keyDirs[i].SegmentID
				log.Printf("Merging segment %d\n", segmentID)
				segmentF, err := segment.CreateSegment(segmentID, os.O_RDWR)
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
						select {
						case writeRequests <- core.WriteRequest{
							Key:   key,
							Value: data,
						}:
						case <-ctx.Done():
							return
						}
					}

				}

				keyDirs = keyDirs[:i]

				if err := segmentF.Close(); err != nil {
					log.Printf("unable to close file %s: %v", segmentF.Name(), err)
				}

				// Queue Segment file for deletion
				select {
				case toDeleteSegmentQueue <- utilities.SegmentIDToString(segmentID):
				case <-ctx.Done():
					return
				}

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

		// Find hint file for segment here
		hf := core.NewHintFile()
		keyDirHashTable, err := hf.Read(segmentID)
		if err == nil {
			keyDirs = append(keyDirs, core.KeyDir{SegmentID: segmentID, HashTable: keyDirHashTable})
			continue
		}

		segmentF, err := segment.CreateSegment(segmentID, os.O_RDWR)
		if err != nil {
			log.Fatal(err)
		}

		// Instantiate Hashtable
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

func deleteSegments(ctx context.Context, toDeleteSegmentQueue <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case segmentId, ok := <-toDeleteSegmentQueue:

			if !ok {
				return
			}

			segment := config.SegmentStorageBasePath + "/" + segmentId + ".data.txt"
			hintFile := config.HintFileStoragePath + "/" + segmentId + ".data.hint"
			err := os.Remove(segment)
			if err != nil {
				log.Printf("unable to remove segment %s after compaction, here's why %v", segment, err)
				continue
			}

			err = os.Remove(hintFile)
			if err != nil {
				log.Printf("unable to remove segment hintfile %s after compaction, here's why %v", hintFile, err)
				continue
			}
		}
	}
}

func persistKeyDirs(ctx context.Context, lss *core.LSS, wg *sync.WaitGroup) {

	defer wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if writeCounter.Load() >= config.KeyDirPersistThreshold {
				// Persist KeyDirs to hint file
				keyDir := lss.KeyDirs[len(lss.KeyDirs)-1]

				hintFile := core.NewHintFile()
				err := hintFile.Write(keyDir)
				if err != nil {
					log.Printf("unable to persist hint file for segment %d: %v", keyDir.SegmentID, err)
				}

				writeCounter.Store(0)
			}
		case <-ctx.Done():
			return
		}

	}
}
