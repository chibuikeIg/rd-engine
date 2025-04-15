package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"reversed-database.engine/storage"
)

var bufferedChannel = make(chan Data, 50)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:1379")
	if err != nil {
		fmt.Println("Failed to bind to port 1379")
		os.Exit(1)
	}

	hashTable := storage.NewHashTable(50)

	lss := storage.NewLSS(hashTable, "simple-db.txt")

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

func readConn(conn net.Conn, lss *storage.LSS) {
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
				fmt.Println(commands)
			}

			if len(commands) == 3 && commands[0] == "set" {
				bufferedChannel <- Data{Key: commands[1], Value: strings.Trim(commands[2], "\b"), HashTable: lss.Ht, StorageFile: lss.File}
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
func handleDataWrites(Lss *storage.LSS) {
	for {
		data := <-bufferedChannel
		Lss.Set(data.Key, data.Value)
	}
}
