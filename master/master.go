package main

import (
	"net"
	"fmt"
	"bufio"
	"encoding/json"
	"code.google.com/p/go-uuid/uuid"
	"HipstMR/lib/go/hipstmr"
)

var transactions map[string]*hipstmr.Transaction

func doWork(trans *hipstmr.Transaction) error {
	return nil
}

func sendTransToClient(conn net.Conn, trans hipstmr.Transaction) error {
	trans.Params = nil
	bytes, err := json.Marshal(trans)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)
	var trans hipstmr.Transaction
	err := decoder.Decode(&trans)
	if err != nil {
		panic(err)
	}

	if trans.Status == "starting" {
		id := uuid.New()
		trans.Id = id
		transactions[id] = &trans

		trans.Status = "started"

		if err := sendTransToClient(conn, trans); err != nil {
			trans.Status = "failed"
			panic(err)
		}

		err = doWork(&trans)
		if err != nil {
			trans.Status = "failed"
			panic(err)
		}

		trans.Status = "finished"

		if err := sendTransToClient(conn, trans); err != nil {
			panic(err)
		}
	}

	// for printing only
	for k, _ := range trans.Params.Files {
		trans.Params.Files[k] = ""
	}
	fmt.Println(trans)
}

func main() {
	// cluster := []string{"localhost:8100", "localhost:8101"}
	transactions = make(map[string]*hipstmr.Transaction)

	sock, err := net.Listen("tcp", ":8013")
	if err != nil {
		panic(err)
	}

	for {
		conn, err := sock.Accept()
		if err != nil {
			panic(err)
		}

		go handle(conn)
	}
}
