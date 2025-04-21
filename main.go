package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"reversed-database.engine/core"
)

var bufferedChannel = make(chan Data, 50)

func main() {
	hashTable := core.NewHashTable(50)

	lss := core.NewLSS(hashTable)

	l, err := net.Listen("tcp", "0.0.0.0:1379")
	if err != nil {
		fmt.Println("Failed to bind to port 1379")
		os.Exit(1)
	}

	go handleDataWrites(lss)

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
	// Rebuild HashTable
	ioReader := bufio.NewReader(lss.File)
	offset := int64(0)
	for {
		// Read until newline ('\n')
		line, err := ioReader.ReadBytes('\n')

		trimmedData := string(bytes.TrimSuffix(line, []byte("\n")))

		dataSlice := strings.SplitN(trimmedData, ",", 2)

		if len(dataSlice) == 2 {
			lss.Ht.Set(dataSlice[0], offset)
			offset += int64(len(line))
		}

		if err != nil {
			break
		}

	}

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

// Maximum file/segment size
const mfs = 3025

// Handle write serialization
func handleDataWrites(lss *core.LSS) {
	defer lss.File.Close()

	// Checks file size and creates new segment
	// if full
	fInfo, err := lss.File.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if fInfo.Size() >= int64(mfs) {
		if err != nil {
			log.Fatal(err)
		}
		lss.ActiveSegID += 1
		segment := core.NewSegment()
		lss.File, err = segment.CreateSegment(lss.ActiveSegID)
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		data := <-bufferedChannel
		_, err := lss.Set(data.Key, data.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
}
