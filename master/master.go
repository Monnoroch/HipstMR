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
	signal chan struct{}
}

var transactions map[string]*hipstmr.Transaction


type TaskChanCollection struct {
	chans []chan Task
}

func (self *TaskChanCollection) Add(ch chan Task) {
	self.chans = append(self.chans, ch)
}

func (self *TaskChanCollection) Remove(ch chan Task) {
	for i, c := range self.chans {
		if ch == c {
			self.chans = append(self.chans[:i], self.chans[i+1:]...)
			close(ch)
			return
		}
	}
}

func (self *TaskChanCollection) Multiplex(ch chan Task) {
	for task := range ch {
		for _, c := range self.chans {
			go func() { c <- task }()
		}
	}
}


type Sheduler struct {
	tasks chan Task
}

var sheduler *Sheduler

func (self *Sheduler) AddJob(conn net.Conn, trans *hipstmr.Transaction, done chan struct{}) {
	go func() {
		ch := make(chan struct{})
		self.tasks <- Task{
			trans: trans,
			signal: ch,
		}
		for _ = range ch {
			if err := sendTrans(conn, *trans); err != nil {
				fmt.Println("Error:", err)
			}
		}
		done <- struct{}{}
	}()
}

func (self *Sheduler) AddTransaction(conn net.Conn, trans *hipstmr.Transaction, done chan struct{}) {
	go func() {
		dones := make([]chan struct{}, slavesCnt)
		for i := 0; i < len(dones); i++ {
			dones[i] = make(chan struct{})
		}

		for i := 0; i < len(dones); i++ {
			self.AddJob(conn, trans, dones[i])
		}

		for i := 0; i < len(dones); i++ {
			<-dones[i]
		}
		done <- struct{}{}
	}()
}

func sendTrans(conn net.Conn, trans hipstmr.Transaction) error {
	trans.Params = nil
	bytes, err := json.Marshal(trans)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func sendFullTrans(conn net.Conn, trans hipstmr.Transaction) error {
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

	done := make(chan struct{})
	sheduler.AddTransaction(conn, &trans, done)
	<-done

	return nil
}

var slavesCnt int = 0

func onNewSlave(conn net.Conn, decoder *json.Decoder) error {
	slavesCnt++
	fmt.Println("onNewSlave", slavesCnt)
	for task := range sheduler.tasks {
		fmt.Println("Accepted task")
		trans := task.trans
		signal := task.signal
		go func() {
			defer func() {
				signal <- struct{}{}
				close(signal)
			}()

			if err := sendFullTrans(conn, *trans); err != nil {
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

				signal <- struct{}{}
			}
		}()
	}
	slavesCnt--
	fmt.Println("close slave", slavesCnt)
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
	fmt.Println("Finished task")
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
	sheduler = &Sheduler{
		tasks: make(chan Task),
	}

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
