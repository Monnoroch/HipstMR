package main

import (
	"bufio"
	"HipstMR/fileserver"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
)

func runTransaction(cmd fileserver.FileServerCommand, conn net.Conn, decoder *json.Decoder) {
	fmt.Println(cmd.Action)
	if err := cmd.Send(conn); err != nil {
		panic(err)
	}

	var cmdFrom fileserver.FileServerCommand
	if err := decoder.Decode(&cmdFrom); err != nil {
		panic(err)
	}

	if cmdFrom.Status == "failed" {
		panic(fmt.Sprintf("%v", cmdFrom))
	}

	if cmdFrom.Action == "get" {
		fmt.Println("\nFile:", string(cmdFrom.Payload))
	}
}

func main() {
	file, err := ioutil.ReadFile("file.txt")
	if err != nil {
		panic(err)
	}

	conn, err := net.Dial("tcp", "localhost:8010")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	decoder := json.NewDecoder(bufio.NewReader(conn))

	cmdPut := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "put",
		Params: map[string]string{
			"to": "file1.txt",
		},
		Payload: file,
	}
	runTransaction(cmdPut, conn, decoder)

	cmdCopyLocal := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "copy",
		Params: map[string]string{
			"from": "file1.txt",
			"to":   "file2.txt",
		},
	}
	runTransaction(cmdCopyLocal, conn, decoder)

	cmdCopyLocal = fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "copy",
		Params: map[string]string{
			"from": "file1.txt",
			"to":   "file3.txt",
		},
	}
	runTransaction(cmdCopyLocal, conn, decoder)

	cmdMoveLocal := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "move",
		Params: map[string]string{
			"from": "file3.txt",
			"to":   "file4.txt",
		},
	}
	runTransaction(cmdMoveLocal, conn, decoder)

	cmdDel := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "del",
		Params: map[string]string{
			"from": "file1.txt",
		},
	}
	runTransaction(cmdDel, conn, decoder)

	cmdCopy := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "copy",
		Params: map[string]string{
			"from": "file2.txt",
			"to":   "file2.txt",
			"addr": "localhost:8011",
		},
	}
	runTransaction(cmdCopy, conn, decoder)

	cmdMove := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "move",
		Params: map[string]string{
			"from": "file4.txt",
			"to":   "file4.txt",
			"addr": "localhost:8011",
		},
	}
	runTransaction(cmdMove, conn, decoder)

	cmdGet := fileserver.FileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "get",
		Params: map[string]string{
			"from": "file2.txt",
		},
	}
	runTransaction(cmdGet, conn, decoder)
}
