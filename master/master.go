package main

import (
	"net"
	"fmt"
	"bufio"
	"flag"
	"io"
	"errors"
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
	trans hipstmr.Transaction
	signal chan Signal
}

type Slave struct {
	tasks chan Task
	id string
	conn net.Conn
	decoder *json.Decoder
	transactions map[string]chan hipstmr.Transaction
}

func (self *Slave) Run() error {
	for {
		var t hipstmr.Transaction
		err := self.decoder.Decode(&t)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		v, ok := self.transactions[t.Id]
		if !ok {
			return errors.New(fmt.Sprintf("Unknown transaction %s for slave %s.", t.Id, self.id))
		}

		v <- t
	}
	return nil
}

func (self *Slave) sendNewTransaction(trans hipstmr.Transaction, callback func(trans hipstmr.Transaction)) error {
	ch := make(chan hipstmr.Transaction)
	self.transactions[trans.Id] = ch
	err := trans.Send(self.conn)
	if err != nil {
		return err
	}
	go func() {
		for tr := range ch {
			callback(tr)
		}
		delete(self.transactions, trans.Id)
	}()
	return nil
}

func (self *Slave) sendNewOnceTransaction(trans hipstmr.Transaction, callback func(trans hipstmr.Transaction)) error {
	ch := make(chan hipstmr.Transaction)
	self.transactions[trans.Id] = ch
	err := trans.Send(self.conn)
	if err != nil {
		return err
	}
	go func() {
		tr := <-ch
		callback(tr)
		close(ch)
		delete(self.transactions, tr.Id)
	}()
	return nil
}

func (self *Slave) askForFS() error {
	err := self.sendNewOnceTransaction(hipstmr.NewTransaction("get_chunks"), func(trans hipstmr.Transaction) {
		if trans.Status == "chunks" {
			chs := trans.Payload.(map[string]interface{})
			for k, v := range chs {
				fsk := FS[k]
				if fsk == nil {
					fsk = make(map[string][]string)
					FS[k] = fsk
				}

				vs := v.([]interface{})
				for _, vv := range vs {
					fsk[self.id] = append(fsk[self.id], vv.(string))
				}
			}

			fmt.Println("Chunks:", FS)
		} else {
			fmt.Println("Error askForFS:", trans)
		}
	})
	if err != nil {
		return err
	}
	return nil
}

func NewSlave(conn net.Conn, decoder *json.Decoder) *Slave {
	return &Slave{
		tasks: make(chan Task),
		id: uuid.New(),
		conn: conn,
		decoder: decoder,
		transactions: make(map[string]chan hipstmr.Transaction),
	}
}

type Sheduler struct {
	slaves []*Slave
}

func (self *Sheduler) GetSlave(id string) *Slave {
	for _, v := range self.slaves {
		if v.id == id {
			return v
		}
	}
	return nil
}

type slaveTask struct {
	task Task
	slave *Slave
}

var FS map[string]map[string][]string

type slaveChunks struct {
	slave *Slave
	chunks []string
}

func getSlavesForTags(tags []string) ([]slaveChunks, error) {
	res := []slaveChunks{}
	for _, v := range tags {
		fse, ok := FS[v]
		if !ok {
			return nil, errors.New("Unknown tag " + v)
		}

		for k, vv := range fse {
			slave := sheduler.GetSlave(k)
			if slave == nil {
				return nil, errors.New("Unknown slave " + k)
			}

			found := -1
			for i, vvv := range res {
				if vvv.slave == slave {
					found = i
					break
				}
			}

			if found == -1 {
				res = append(res, slaveChunks{
					slave: slave,
					chunks: nil,
				})
				found = len(res) - 1
			}

			for _, vvv := range vv {
				res[found].chunks = append(res[found].chunks, v + ".chunk." + vvv)
			}
		}
	}
	return res, nil
}

func (self *Sheduler) GetSlavesTasks(trans hipstmr.Transaction) ([]slaveTask, error) { // TODO: smart choose
	slaves, err := getSlavesForTags(trans.Params.InputTables)
	if err != nil {
		return nil, err
	}

	res := make([]slaveTask, len(slaves))
	for i := 0; i < len(res); i++ {
		tr := trans
		tr.Params = tr.Params.Clone()
		tr.Params.Chunks = slaves[i].chunks
		res[i] = slaveTask{
			task: Task{
				trans: tr,
				signal: make(chan Signal),
			},
			slave: slaves[i].slave,
		}
	}
	return res, nil
}

func (self *Sheduler) RunTransaction(trans hipstmr.Transaction) bool {
	slavesTasks, err := self.GetSlavesTasks(trans)
	if err != nil {
		fmt.Println(err)
	}

	if err != nil || len(slavesTasks) == 0 {
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

func (self *Sheduler) AddSlave(conn net.Conn, decoder *json.Decoder) *Slave {
	res := NewSlave(conn, decoder)
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

var sheduler *Sheduler


func onNewClient(conn net.Conn, trans hipstmr.Transaction) error {
	fmt.Println("Accepted transaction")

	trans.Id = uuid.New()
	trans.Status = "started"
	if err := sendTransOrFail(conn, trans); err != nil {
		return err
	}

	fmt.Println("Run transaction")

	if !sheduler.RunTransaction(trans) {
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
	slave := sheduler.AddSlave(conn, decoder)
	err := slave.askForFS()
	if err != nil {
		panic(err)
	}

	fmt.Println("new slave", len(sheduler.slaves))

	go func() {
		for task := range slave.tasks {
			fmt.Println("Accepted task")
			fmt.Println("Run task")
			err := slave.sendNewTransaction(task.trans, func(msg hipstmr.Transaction) {
				if msg.Status == "finished" || msg.Status == "failed" {
					close(slave.transactions[msg.Id])
				}

				if msg.Status == "finished" {
					fmt.Println("Finished task")
					err := slave.askForFS()
					if err != nil {
						panic(err)
					}
				}

				if msg.Status == "failed" {
					fmt.Println("Failed task")
				}

				if msg.Status == "finished" || msg.Status == "failed" {
					task.trans.Status = msg.Status
					task.signal <- Signal{}
				}
			})

			if err != nil {
				fmt.Println("Dropped task")
				close(slave.tasks)
				return
			}
		}

		fmt.Println("Slave died!")
	}()

	err = slave.Run()
	if err != nil {
		panic(err)
	}

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
