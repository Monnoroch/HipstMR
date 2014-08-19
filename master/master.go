package main

import (
	"net"
	"io"
	"fmt"
	"bufio"
	"flag"
	"encoding/json"
	"code.google.com/p/go-uuid/uuid"
	"HipstMR/lib/go/hipstmr"
)

type Task struct {
	trans *hipstmr.Transaction
	ch chan struct{}
}

var tasks chan Task
var transactions map[string]*hipstmr.Transaction


func sendTrans(conn net.Conn, trans hipstmr.Transaction) error {
	trans.Params = nil
	bytes, err := json.Marshal(trans)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func onNewClient(conn net.Conn, trans hipstmr.Transaction) error {
	id := uuid.New()
	trans.Id = id
	transactions[id] = &trans

	trans.Status = "started"

	if err := sendTrans(conn, trans); err != nil {
		trans.Status = "failed"
		sendTrans(conn, trans)
		return err
	}

	ch := make(chan struct{})
	tasks <- Task{
		trans: &trans,
		ch: ch,
	}
	for _ = range ch {
		sendTrans(conn, trans)
	}

	return nil
}

func onNewSlave(conn net.Conn, decoder *json.Decoder) error {
	for task := range tasks {
		trans := task.trans
		ch := task.ch
		go func() {
			defer func() {
				ch <- struct{}{}
				close(ch)
			}()

			if err := sendTrans(conn, *trans); err != nil {
				panic(err)
			}

			for {
				var t hipstmr.Transaction
				err := decoder.Decode(&t)
				if err == io.EOF {
					trans.Status = "finished"
					break
				}

				if err != nil {
					trans.Status = "failed"
					break
				}

				trans.Status = t.Status
				if trans.Status == "finished" {
					break
				}

				ch <- struct{}{}
			}

		}()
	}
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
		if err = onNewClient(conn, trans); err != nil {
			panic(err)
		}
	} else if trans.Status == "slave_waiting" {
		if err = onNewSlave(conn, decoder); err != nil {
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
	help := flag.Bool("help", false, "print this help")
	address := flag.String("address", "", "master adress")
	flag.Parse()
	if *help || *address == "" {
		flag.PrintDefaults()
		return
	}
	// cluster := []string{"localhost:8100", "localhost:8101"}
	transactions = make(map[string]*hipstmr.Transaction)
	tasks = make(chan Task)

	sock, err := net.Listen("tcp", *address)
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
