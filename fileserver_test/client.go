package main

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
)

func WriteAll(writer io.Writer, buf []byte) error {
	for {
		cnt := len(buf)
		n, err := writer.Write(buf)
		if err != nil {
			return err
		}

		if n == cnt {
			break
		}

		buf = buf[n:]
	}
	return nil
}

type fileServerCommand struct {
	Id      string            `json"id"`
	Status  string            `json"status"`
	Action  string            `json"action"`
	Params  map[string]string `json"params"`
	Payload []byte            `json"payload"`
}

func (self *fileServerCommand) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	return WriteAll(conn, bytes)
}

func runTransaction(cmd fileServerCommand, conn net.Conn, decoder *json.Decoder) {
	fmt.Println(cmd.Action)
	if err := cmd.Send(conn); err != nil {
		panic(err)
	}

	var cmdFrom fileServerCommand
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

	cmdPut := fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "put",
		Params: map[string]string{
			"to": "file1.txt",
		},
		Payload: file,
	}
	runTransaction(cmdPut, conn, decoder)

	cmdCopyLocal := fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "copy",
		Params: map[string]string{
			"from": "file1.txt",
			"to":   "file2.txt",
		},
	}
	runTransaction(cmdCopyLocal, conn, decoder)

	cmdCopyLocal = fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "copy",
		Params: map[string]string{
			"from": "file1.txt",
			"to":   "file3.txt",
		},
	}
	runTransaction(cmdCopyLocal, conn, decoder)

	cmdDel := fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "del",
		Params: map[string]string{
			"from": "file1.txt",
		},
	}
	runTransaction(cmdDel, conn, decoder)

	cmdCopy := fileServerCommand{
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

	cmdMove := fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "move",
		Params: map[string]string{
			"from": "file3.txt",
			"to":   "file3.txt",
			"addr": "localhost:8011",
		},
	}
	runTransaction(cmdMove, conn, decoder)

	cmdGet := fileServerCommand{
		Id:     uuid.New(),
		Status: "started",
		Action: "get",
		Params: map[string]string{
			"from": "file2.txt",
		},
	}
	runTransaction(cmdGet, conn, decoder)
}
