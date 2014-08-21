package main

import (
	"net"
	"fmt"
	"bufio"
	"flag"
	"encoding/json"
	"code.google.com/p/go-uuid/uuid"
	"HipstMR/lib/go/hipstmr"
)


func sendTrans(conn net.Conn, trans hipstmr.Transaction) error {
	trans.Params = nil
	return trans.Send(conn)
}

func sendTransOrPrint(conn net.Conn, trans hipstmr.Transaction) {
	if err := sendTrans(conn, trans); err != nil {
		fmt.Println("Error:", err)
	}
}

func sendTransOrFail(conn net.Conn, trans hipstmr.Transaction) error {
	if err := sendTrans(conn, trans); err != nil {
		trans.Status = "failed"
		sendTrans(conn, trans)
		return err
	}
	return nil
}


type Signal struct {}

type Task struct {
	trans *hipstmr.Transaction
	signal chan Signal
}

type Slave struct {
	tasks chan Task
	id string
}

func (self *Slave) Run(decoder *json.Decoder, recv chan hipstmr.Transaction) {
	for {
		var t hipstmr.Transaction
		err := decoder.Decode(&t)
		if err != nil {
			break
		}

		recv <- t
	}
}

type Sheduler struct {
	slaves []*Slave
}

type slaveTask struct {
	task Task
	slave *Slave
}

func (self *Sheduler) GetSlavesTasks(trans *hipstmr.Transaction) []slaveTask { // TODO: smart choose
	res := make([]slaveTask, len(self.slaves))
	for i := 0; i < len(res); i++ {
		res[i] = slaveTask{
			task: Task{
				trans: trans,
				signal: make(chan Signal),
			},
			slave: self.slaves[i],
		}
	}
	return res
}

func (self *Sheduler) RunTransaction(trans *hipstmr.Transaction) bool {
	slavesTasks := self.GetSlavesTasks(trans)
	if len(slavesTasks) == 0 {
		return false
	}

	for i := 0; i < len(slavesTasks); i++ {
		fmt.Println("Sending task to a slave")
		slavesTasks[i].slave.tasks <- slavesTasks[i].task
		fmt.Println("Sent task to a slave")
	}

	for i := 0; i < len(slavesTasks); i++ {
		fmt.Println("Wait for a slave")
		<-slavesTasks[i].task.signal
		fmt.Println("Slave finished!")
	}
	return true
}

func (self *Sheduler) AddSlave() *Slave {
	res := &Slave{
		tasks: make(chan Task),
		id: uuid.New(),
	}
	self.slaves = append(self.slaves, res)
	return res
}

func (self *Sheduler) RemoveSlave(slave *Slave) {
	for i, s := range self.slaves {
		if s == slave {
			self.slaves = append(self.slaves[:i], self.slaves[i+1:]...)
			break
		}
	}
	close(slave.tasks)
}

var transactions map[string]*hipstmr.Transaction
var sheduler *Sheduler

// type TaskChanCollection struct {
// 	chans []chan Task
// }

// func (self *TaskChanCollection) Add(ch chan Task) {
// 	self.chans = append(self.chans, ch)
// }

// func (self *TaskChanCollection) Remove(ch chan Task) {
// 	for i, c := range self.chans {
// 		if ch == c {
// 			self.chans = append(self.chans[:i], self.chans[i+1:]...)
// 			close(ch)
// 			return
// 		}
// 	}
// }

// func (self *TaskChanCollection) Multiplex(ch chan Task) {
// 	for task := range ch {
// 		for _, c := range self.chans {
// 			go func() { c <- task }()
// 		}
// 	}
// }

// func (self *TaskChanCollection) Combine(ch chan Task) { // TODO: fix add/remove
// 	for _, c := range self.chans {
// 		c := c
// 		go func() {
// 			for task := range c {
// 				task <- ch
// 			}
// 		}()
// 	}
// }


var FS map[string]map[string][]string

func onNewClient(conn net.Conn, trans hipstmr.Transaction) error {
	id := uuid.New()
	trans.Id = id
	transactions[id] = &trans

	fmt.Println("Accepted transaction")

	trans.Status = "started"

	if err := sendTransOrFail(conn, trans); err != nil {
		return err
	}

	fmt.Println("Run transaction")

	if !sheduler.RunTransaction(&trans) {
		trans.Status = "failed"
	}

	sendTransOrPrint(conn, trans)

	fmt.Println("Finished transaction")

	return nil
}

func suckMessages(messages chan hipstmr.Transaction) string {
	for msg := range messages {
		if msg.Status == "finished" || msg.Status == "failed" {
			return msg.Status
		}
	}
	return ""
}

func onNewSlave(conn net.Conn, decoder *json.Decoder) error {
	slave := sheduler.AddSlave()

	var chunksReq hipstmr.Transaction
	chunksReq.Status = "get_chunks"
	if err := sendTransOrFail(conn, chunksReq); err != nil {
		return err
	}

	var chunks hipstmr.Transaction
	err := decoder.Decode(&chunks)
	if err != nil {
		return err
	}

	chs := chunks.Payload.(map[string]interface{})
	for k, v := range chs {
		fsk := FS[k]
		if fsk == nil {
			fsk = make(map[string][]string)
			FS[k] = fsk
		}

		vs := v.([]interface{})
		for _, vv := range vs {
			fsk[slave.id] = append(fsk[slave.id], vv.(string))
		}
	}

	fmt.Println("Chunks:", FS)

	fmt.Println("new slave", len(sheduler.slaves))
	messages := make(chan hipstmr.Transaction)

	go func() {
		for task := range slave.tasks {
			fmt.Println("Accepted task")
			trans := task.trans
			signal := task.signal

			go func() {
				fmt.Println("Run task")
				if err := trans.Send(conn); err != nil {
					fmt.Println("Dropped task")
					close(slave.tasks)
					return
				}

				status := suckMessages(messages)
				if status == "" {
					close(slave.tasks)
					return
				}

				fmt.Println("Finished task")
				trans.Status = status
				signal <- Signal{}
			}()
		}

		fmt.Println("Slave died!")
	}()

	slave.Run(decoder, messages)
	close(messages)

	sheduler.RemoveSlave(slave)

	for _, v := range FS {
		delete(v, slave.id)
	}

	fmt.Println("close slave", len(sheduler.slaves))
	return nil
}

func handle(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(bufio.NewReader(conn))
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
		slaves: make([]*Slave, 0),
	}

	FS = make(map[string]map[string][]string)

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
