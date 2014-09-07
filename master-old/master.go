package main

import (
	"HipstMR/helper"
	"HipstMR/lib/go/hipstmr"
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"path"
)


func Failed(trans helper.Transaction, conn net.Conn, origErr error) {
	trans.Params.Params = nil
	trans.Params.Chunks = nil
	trans.Params.OutputTables = nil
	trans.Payload = nil
	trans.Status = "failed"
	if err := trans.Send(conn); err != nil {
		fmt.Println("Errors:", origErr, err)
	}
}

type IdSet []string
type TagData map[string]IdSet

type FsData struct {
	slaves map[string]helper.FsData
	index  map[string]TagData
}

func (self *FsData) Rebuild() {
	self.index = make(map[string]TagData)
	for slave, fsData := range self.slaves {
		for chunkId, chunkData := range fsData.Chunks {
			for tag, _ := range chunkData.Tags {
				_, ok := self.index[tag]
				if !ok {
					self.index[tag] = TagData{
						slave: IdSet{chunkId},
					}
				} else {
					self.index[tag][slave] = append(self.index[tag][slave], chunkId)
				}
			}
		}
	}
}

func (self *FsData) Update(id string, slave helper.FsData) {
	self.slaves[id] = slave
	self.Rebuild()
}

func (self *FsData) Unlink(slave Slave) {
	delete(self.slaves, slave.id)
	self.Rebuild()
}

func (self *FsData) GetTablesOwners(tbls []string) []string {
	slaves := make(map[string]bool)
	for _, tbl := range tbls {
		val, ok := self.index[tbl]
		if ok {
			for slave, _ := range val {
				slaves[slave] = true
			}
		}
	}
	res := make([]string, len(slaves))
	i := 0
	for k, _ := range slaves {
		res[i] = k
		i += 1
	}
	return res
}

func (self *FsData) GetTablesOwnersChunks(tbls []string) map[string][]string {
	slaves := make(map[string][]string)
	for _, tbl := range tbls {
		val, ok := self.index[tbl]
		if ok {
			for slave, chunks := range val {
				slaves[slave] = append(slaves[slave], chunks...)
			}
		}
	}
	return slaves
}

func (self *FsData) UpdateFromTrans(slave *Slave, trans helper.Transaction) error {
	str := trans.Payload.(string)
	bs, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return err
	}

	var fsdata helper.FsData
	if err := json.Unmarshal(bs, &fsdata); err != nil {
		fmt.Println("PT 2", err)
		return err
	}

	self.Update(slave.id, fsdata)
	fmt.Println("Chunks:", self.index)
	return nil
}

func NewFsData() FsData {
	return FsData{
		slaves: make(map[string]helper.FsData),
	}
}


type Slave struct {
	id string
	master *Master
	conn net.Conn
	decoder *json.Decoder
	tasks chan Task
	transactions map[string]chan helper.Transaction
}

func (self *Slave) Failed(trans helper.Transaction, origErr error) {
	Failed(trans, self.conn, origErr)
}

func (self *Slave) sendNewTransaction(trans helper.Transaction, callback func(trans helper.Transaction)) error {
	ch := make(chan helper.Transaction)
	self.transactions[trans.Id] = ch
	if err := trans.Send(self.conn); err != nil {
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

func (self *Slave) sendNewOnceTransaction(trans helper.Transaction, callback func(trans helper.Transaction)) error {
	ch := make(chan helper.Transaction)
	self.transactions[trans.Id] = ch
	if err := trans.Send(self.conn); err != nil {
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

func (self *Slave) RunTasks() {
	for task := range self.tasks {
		fmt.Println("Accepted task")
		err := self.sendNewTransaction(task.trans, func(msg helper.Transaction) {
			fmt.Println(msg)
			if msg.Status == "received_files" {
				go func() {
					task.signal <- helper.Transaction{}
				}()
			}

			isDone := msg.Status == "finished" || msg.Status == "failed"

			if isDone {
				close(self.transactions[msg.Id])
			}

			if msg.Status == "finished" {
				fmt.Println("Finished task")
				if err := self.master.UpdateFs(self); err != nil {
					fmt.Println("Error:", err)
				}
			} else if msg.Status == "failed" {
				fmt.Println("Failed task")
			}

			if isDone {
				task.signal <- msg
			}
		})

		if err != nil {
			fmt.Println("Dropped task")
			self.Failed(task.trans, err)
			close(self.tasks) // ?!
			return
		}
	}
}

func (self *Slave) ReadMsg() (helper.Transaction, error) {
	var t helper.Transaction
	if err := self.decoder.Decode(&t); err != nil {
		return helper.Transaction{}, err
	}
	return t, nil
}

func (self *Slave) Run() error {
	go self.RunTasks()
	for {
		t, err := self.ReadMsg()
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

func NewSlave(master *Master, conn net.Conn, decoder *json.Decoder) Slave {
	return Slave{
		id: uuid.New(),
		master: master,
		conn: conn,
		decoder: decoder,
		tasks: make(chan Task),
		transactions: make(map[string]chan helper.Transaction),
	}
}


type Master struct {
	addr string
	slaves map[string]Slave
	fsdata FsData
}

func (self *Master) Run() error {
	sock, err := net.Listen("tcp", self.addr)
	if err != nil {
		return err
	}

	for {
		conn, err := sock.Accept()
		if err != nil {
			return err
		}

		go self.Handle(conn)
	}
	return nil
}

func (self *Master) HandleSlave(conn net.Conn, decoder *json.Decoder) error {
	slave := NewSlave(self, conn, decoder)
	self.slaves[slave.id] = slave
	defer func() {
		delete(self.slaves, slave.id)
		self.fsdata.Unlink(slave)
		fmt.Println("close slave", len(self.slaves))
	}()
	fmt.Println("new slaves", len(self.slaves))

	upTr := helper.NewTransaction("fs_get")
	if err := upTr.Send(conn); err != nil {
		return err
	}

	fmt.Println("asked for fs")

	trans, err := slave.ReadMsg()
	if err != nil {
		return err
	}

	fmt.Println("got fs", trans.Status)

	if trans.Status == "finished" {
		fmt.Println("updating...")
		if err := self.fsdata.UpdateFromTrans(&slave, trans); err != nil {
			return err
		}
	} else {
		fmt.Println("Error askForFS:", trans)
		return errors.New("Transaction status is " + trans.Status)
	}

	return slave.Run()
}

type Task struct {
	trans helper.Transaction
	signal chan helper.Transaction
}

type slaveTask struct {
	slave Slave
	task Task
}

func (self *Master) RunTransactionSimple(slavesTasks []slaveTask) {
	fmt.Println("Run simple transaction")
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
	fmt.Println("Finished simple transaction")
}

func (self *Master) RunTransaction(conn net.Conn, trans helper.Transaction, slavesTasks []slaveTask) {
	fmt.Println("Run transaction")
	for i := 0; i < len(slavesTasks); i++ {
		fmt.Println("Sending task to a slave")
		slavesTasks[i].slave.tasks <- slavesTasks[i].task
		fmt.Println("Sent task to a slave")
	}

	for i := 0; i < len(slavesTasks); i++ {
		fmt.Println("Sending files...")
		<-slavesTasks[i].task.signal
		fmt.Println("Files sent!")
	}

	trans.Status = "All files sent"
	if err := trans.Send(conn); err != nil {
		fmt.Println("Error:", err)
	}

	stderr := ""
	for i := 0; i < len(slavesTasks); i++ {
		fmt.Println("Wait for a slave")
		tr := <-slavesTasks[i].task.signal
		str, ok := tr.Payload.(string)
		if ok {
			stderr += str
		}
		fmt.Println("Slave finished!")
	}
	fmt.Println("Finished transaction")
}

func (self *Master) HandleClient(conn net.Conn, trans helper.Transaction) error {
	fmt.Println("Accepted transaction")
	trans.Id = uuid.New()
	trans.Status = "started"
	if err := trans.Send(conn); err != nil {
		return err
	}

	typ := trans.Params.Params.Type
	if typ == "move" || typ == "copy" || typ == "drop" {
		job := helper.NewTransaction("fs_" + typ)
		job.Params = helper.Params{
			Params: trans.Params.Params,
		}

		tables := trans.Params.Params.InputTables
		if typ != "drop" {
			ts := make([]string, len(tables) + 1)
			copy(ts, tables)
			ts[len(tables)] = trans.Params.Params.OutputTables[0]
			tables = ts
		}
		slaves := self.fsdata.GetTablesOwners(tables)
		slavesTasks := make([]slaveTask, len(slaves))
		for sn := 0; sn < len(slavesTasks); sn++ {
			slavesTasks[sn] = slaveTask{
				slave: self.slaves[slaves[sn]],
				task: Task{
					trans:  job,
					signal: make(chan helper.Transaction),
				},
			}
		}
		self.RunTransactionSimple(slavesTasks)
		trans.Status = "finished"
		if err := trans.Send(conn); err != nil {
			fmt.Println("Error:", err)
		}
	} else if typ == "map" {
		job := helper.Transaction{
			Id:     trans.Id,
			Action: "mr_map",
			Status: "started",
			Params: helper.Params{
				Params: trans.Params.Params,
			},
		}

		tables := make([]string, len(trans.Params.Params.InputTables) + len(trans.Params.Params.OutputTables))
		copy(tables, trans.Params.Params.InputTables)
		copy(tables[len(trans.Params.Params.InputTables):], trans.Params.Params.OutputTables)

		slaves := self.fsdata.GetTablesOwnersChunks(tables)
		slavesTasks := make([]slaveTask, len(slaves))
		sn := 0
		for k, v := range slaves {
			tr := job
			tr.Params.Chunks = v
			tr.Params.OutputTables = make([]string, len(tr.Params.Params.OutputTables))
			for i, v := range tr.Params.Params.OutputTables {
				tr.Params.OutputTables[i] = path.Join("tmp", tr.Id, v)
			}

			slavesTasks[sn] = slaveTask{
				slave: self.slaves[k],
				task: Task{
					trans:  tr,
					signal: make(chan helper.Transaction),
				},
			}
			sn++
		}

		self.RunTransaction(conn, trans, slavesTasks)

		// move
		fmt.Println("~~~~", trans.Params.Params.OutputTables)
		for i, tbl := range trans.Params.Params.OutputTables {
			tmpTbls := make([]string, len(slaves))
			for j, st := range slavesTasks {
				tmpTbls[j] = st.task.trans.Params.OutputTables[i]
			}

			job := helper.NewTransaction("fs_move_chunks")
			job.Params = helper.Params{
				Params: &hipstmr.Params{
					InputTables: tmpTbls,
				},
			}

			var num uint64 = 0
			moveSlaves := self.fsdata.GetTablesOwnersChunks(tmpTbls)
			slavesTasks := make([]slaveTask, len(moveSlaves))
			sp := 0
			for k, v := range moveSlaves {
				tr := job
				tr.Params.Chunks = v
				tr.Params.OutputChunkNums = make([]uint64, len(v))
				for kk := 0; kk < len(tr.Params.OutputChunkNums); kk++ {
					tr.Params.OutputChunkNums[kk] = num
					num++
				}
				tr.Params.OutputTables = []string{tbl}

				slavesTasks[sp] = slaveTask{
					slave: self.slaves[k],
					task: Task{
						trans:  tr,
						signal: make(chan helper.Transaction),
					},
				}
				sp++
			}

			self.RunTransactionSimple(slavesTasks)
		}

		trans.Status = "finished"
		trans.Params.Params = nil
		if err := trans.Send(conn); err != nil {
			fmt.Println("Error:", err)
		}
	}

	return nil
}

func (self *Master) DoHandle(conn net.Conn) error {
	decoder := json.NewDecoder(bufio.NewReader(conn))

	type handleClientTransaction struct {
		Id      string          `json"id"`
		Status  string          `json"status"`
		Params  json.RawMessage `json"params"`
		Payload json.RawMessage `json"payload"`
		Action  string          `json"action"`
	}

	var clTrans handleClientTransaction
	if err := decoder.Decode(&clTrans); err != nil {
		return err
	}

	if clTrans.Action == "" {
		var ps hipstmr.Params
		if err := json.Unmarshal(clTrans.Params, &ps); err != nil {
			return err
		}

		return self.HandleClient(conn, helper.Transaction{
			Status: clTrans.Status,
			Params: helper.Params{
				Params: &ps,
			},
		})
	} else {
		return self.HandleSlave(conn, decoder)
	}
}

func (self *Master) Handle(conn net.Conn) {
	defer conn.Close()
	if err := self.DoHandle(conn); err != nil {
		Failed(helper.Transaction{}, conn, err)
	}
}

func (self *Master) UpdateFs(slave *Slave) error {
	signal := make(chan error)
	err := slave.sendNewOnceTransaction(helper.NewTransaction("fs_get"), func(trans helper.Transaction) {
		var err error = nil
		if trans.Status == "finished" {
			err = self.fsdata.UpdateFromTrans(slave, trans)
		} else {
			err = errors.New("Transaction status is " + trans.Status)
			fmt.Println("Error askForFS:", trans)
		}
		signal <- err
	})
	if err != nil {
		return err
	}
	return <-signal
}

func NewMaster(addr string) Master {
	return Master{
		addr: addr,
		slaves: make(map[string]Slave),
		fsdata: NewFsData(),
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

	master := NewMaster(*address)
	if err := master.Run(); err != nil {
		fmt.Println("Error:", err)
		return
	}
}
