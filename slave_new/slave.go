package main

import (
	"HipstMR/helper"
	"bufio"
	// "code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"path"
)


type FsData struct {
	mnt string
	data helper.FsData
}

func (self *FsData) Read(dir string) error {
	return self.data.Read(dir)
}

func (self *FsData) ClearFs() {
	self.data.ClearFs(self.mnt)
}

func NewFsData(mnt string) FsData {
	return FsData{
		mnt: path.Clean(mnt),
		data: helper.FsData{},
	}
}


type Master struct {
	address string
	conn net.Conn
	decoder *json.Decoder
}

func NewMaster(addr string) (Master, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return Master{}, err
	}

	return Master{
		address: addr,
		conn: conn,
		decoder: json.NewDecoder(bufio.NewReader(conn)),
	}, nil
}

func (self *Master) Loop(slave *Slave) {
	for {
		var trans helper.Transaction
		err := self.decoder.Decode(&trans)
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		if err := slave.Handle(self, trans); err != nil {
			panic(err)
		}
	}
}

func (self *Master) Send(trans helper.Transaction) error {
	return trans.Send(self.conn)
}

func (self *Master) Close() error {
	return self.conn.Close()
}


type Slave struct {
	masters map[string]Master
	fsdata FsData
}

func (self *Slave) Connect(addr string) error {
	_, ok := self.masters[addr]
	if ok {
		return errors.New("There is already a connection to master " + addr)
	}

	trans := helper.NewTransaction("connect_slave")
	master, err := NewMaster(addr)
	if err != nil {
		return err
	}

	if err := master.Send(trans); err != nil {
		master.Close()
		return err
	}

	go master.Loop(self)

	self.masters[addr] = master
	return nil
}

func (self *Slave) Close() error {
	var err error = nil
	for _, v := range self.masters {
		err = v.Close()
	}
	return err
}

func (self *Slave) Handle(master *Master, trans helper.Transaction) error {
	return nil
}

func NewSlave(mnt string) Slave {
	return Slave{
		masters: make(map[string]Master),
		fsdata: NewFsData(mnt),
	}
}


func main() {
	help := flag.Bool("help", false, "print this help")
	master := flag.String("master", "", "master adress")
	mntv := flag.String("mnt", "", "mount point")
	flag.Parse()
	if *help || *master == "" || *mntv == "" {
		flag.PrintDefaults()
		return
	}

	slave := NewSlave(*mntv)
	defer slave.Close()

	if err := slave.fsdata.Read("./"); err != nil {
		panic(err)
	}
	slave.fsdata.ClearFs()

	if err := slave.Connect(*master); err != nil {
		panic(err)
	}

	select{}
}
