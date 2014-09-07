package main

import (
	"HipstMR/helper"
	"bufio"
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

type JobConfig struct {
	Mnt  string `json:"mnt"`
	Jtype        string   `json:"type"`
	Name         string   `json:"name"`
	Dir          string   `json:"dir"`
	Chunks       []string `json:"chunks"`
	OutputTables []string `json:"output_tables"`
	Object       []byte   `json:"object"`
}

type FsData struct {
	mnt  string
	dir  string
	data helper.FsData
}

func (self *FsData) Read() error {
	return self.data.Read(self.dir)
}

func (self *FsData) ClearFs() {
	self.data.ClearFs(self.mnt)
}

func (self *FsData) GetFs(trans *helper.Transaction) error {
	if err := self.Read(); err != nil {
		return err
	}

	bs, err := json.Marshal(&self.data)
	if err != nil {
		return err
	}

	trans.Payload = bs
	return nil
}

func (self *FsData) Move(inputs []string, output string) error {
	if err := self.Del([]string{output}); err != nil {
		return err
	}

	var num uint64 = 0
	for _, v := range self.data.Chunks {
		for _, in := range inputs {
			_, ok := v.Tags[in]
			if !ok {
				continue
			}

			delete(v.Tags, in)
			v.Tags[output] = append(v.Tags[output], num)
			num++
		}
	}
	return nil
}

func (self *FsData) MoveChunks(chunks []string, nums []uint64, inputs []string, output string) error {
	if err := self.Del([]string{output}); err != nil {
		return err
	}

	for i, chunk := range chunks {
		ch, ok := self.data.Chunks[chunk]
		if !ok {
			return errors.New("WAT?!")
		}

		for _, inp := range inputs {
			for tag, _ := range ch.Tags {
				if tag == inp {
					delete(ch.Tags, tag)
				}
			}
		}
		ch.Tags[output] = append(ch.Tags[output], nums[i])
	}
	return nil
}

func (self *FsData) Copy(inputs []string, output string) error {
	if err := self.Del([]string{output}); err != nil {
		return err
	}

	var num uint64 = 0
	for _, v := range self.data.Chunks {
		for _, in := range inputs {
			_, ok := v.Tags[in]
			if !ok {
				continue
			}

			v.Tags[output] = append(v.Tags[output], num)
			num++
		}
	}
	return nil
}

func (self *FsData) Del(inputs []string) error {
	for k, v := range self.data.Chunks {
		for _, in := range inputs {
			_, ok := v.Tags[in]
			if ok {
				delete(v.Tags, in)
			}
		}
		if len(v.Tags) == 0 {
			delete(self.data.Chunks, k)
			if err := os.Remove(self.GetChunkFileName(k)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *FsData) AddChunk(id, tag string, num uint64) {
	self.data.Chunks[id] = &helper.ChunkData{
		Tags: map[string][]uint64{tag: []uint64{num}},
	}
}

func (self *FsData) GetChunkFileName(chunk string) string {
	return path.Join(self.mnt, chunk+".chunk")
}

func (self *FsData) Handle(trans helper.Transaction) error {
	if trans.Action == "fs_move" {
		if err := self.Move(trans.Params.Params.InputTables, trans.Params.Params.OutputTables[0]); err != nil {
			return err
		}
	} else if trans.Action == "fs_move_chunks" {
		if err := self.MoveChunks(trans.Params.Chunks, trans.Params.OutputChunkNums, trans.Params.Params.InputTables, trans.Params.OutputTables[0]); err != nil {
			return err
		}
	} else if trans.Action == "fs_copy" {
		if err := self.Copy(trans.Params.Params.InputTables, trans.Params.Params.OutputTables[0]); err != nil {
			return err
		}
	} else if trans.Action == "fs_drop" {
		if err := self.Del(trans.Params.Params.InputTables); err != nil {
			return err
		}
	}

	if err := self.data.Write("1.fsdat"); err != nil {
		return err
	}
	return nil
}

func dumpTransaction(trans helper.Transaction) (string, error) {
	res := ""
	for k, v := range trans.Params.Params.Files {
		isBinary := false
		if k[0] == '!' {
			k = k[1:]
			isBinary = true
		}

		p := path.Join(trans.Id, k)
		f, err := os.Create(p)
		if err != nil {
			return "", err
		}

		if err := helper.WriteAll(f, []byte(v)); err != nil {
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}

		if isBinary {
			if err := os.Chmod(p, os.ModePerm); err != nil {
				return "", err
			}
			res = k
		}
	}
	return res, nil
}

func (self *FsData) DoMap(master *Master, trans *helper.Transaction) error {
	bin, err := dumpTransaction(*trans)
	if err != nil {
		return err
	}

	trans.Status = "received_files"
	if err := master.Send(*trans); err != nil {
		return err
	}

	cfg := JobConfig{
		Mnt: self.mnt,
		Dir:          path.Join(trans.Id, uuid.New()),
		Jtype:        trans.Params.Params.Type,
		Name:         trans.Params.Params.Name,
		Object:       trans.Params.Params.Object,
		Chunks:       trans.Params.Chunks,
		OutputTables: trans.Params.OutputTables,
	}

	buf, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	cmd := exec.Command(path.Join(".", trans.Id, bin), "-hipstmrjob")
	cmd.Stdin = bytes.NewReader(buf)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()

	fmt.Println("~~~~~~Stderr:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	fmt.Print(string(stderr.Bytes()))
	fmt.Println("~~~~~~Stdout:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	fmt.Print(string(stdout.Bytes()))
	fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	if err != nil {
		return err
	}

	for _, tbl := range cfg.OutputTables {
		p := path.Clean(path.Join(self.mnt, cfg.Dir, tbl))
		dir, err := ioutil.ReadDir(p)
		if err != nil {
			return err
		}

		chunkSuffix := ".chunk"
		for _, v := range dir {
			nm := v.Name()
			if v.IsDir() || !strings.HasSuffix(nm, chunkSuffix) {
				return errors.New(nm + " is not a chunk.")
			}

			num, err := strconv.ParseUint(nm[:len(nm)-len(chunkSuffix)], 10, 64)
			if err != nil {
				return err
			}

			id := uuid.New()
			if err := os.Rename(path.Join(p, nm), path.Join(self.mnt, id+chunkSuffix)); err != nil {
				return err
			}
			fmt.Println("Rename", path.Join(p, nm), "to", path.Join(self.mnt, id+chunkSuffix))
			self.AddChunk(id, tbl, num)
		}
	}

	if err := self.data.Write("1.fsdat"); err != nil {
		return err
	}

	// TODO: put in safe place (defer?)
	if err := os.RemoveAll(path.Join(self.mnt, trans.Id)); err != nil {
		return err
	}

	trans.Status = "finished"
	trans.Payload = string(stderr.Bytes())
	return nil
}

func NewFsData(mnt, dir string) FsData {
	return FsData{
		mnt:  path.Clean(mnt),
		dir:  path.Clean(dir),
		data: helper.FsData{},
	}
}

type Master struct {
	address string
	conn    net.Conn
	decoder *json.Decoder
}

func (self *Master) Loop(slave *Slave) {
	for {
		var trans helper.Transaction
		err := self.decoder.Decode(&trans)
		if err == io.EOF {
			break
		}

		if err != nil {
			self.Failed(trans, err)
			continue
		}

		if err := slave.Handle(self, trans); err != nil {
			self.Failed(trans, err)
			continue
		}
	}
	slave.OnDisconnected(self)
}

func (self *Master) Failed(trans helper.Transaction, origErr error) {
	trans.Params.Params = nil
	trans.Params.Chunks = nil
	trans.Params.OutputTables = nil
	trans.Payload = nil
	trans.Status = "failed"
	fmt.Println(origErr)
	if err := self.Send(trans); err != nil {
		fmt.Println("Errors:", origErr, err)
	}
}

func (self *Master) Send(trans helper.Transaction) error {
	return trans.Send(self.conn)
}

func (self *Master) Close() error {
	return self.conn.Close()
}

func NewMaster(addr string) (Master, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return Master{}, err
	}

	return Master{
		address: addr,
		conn:    conn,
		decoder: json.NewDecoder(bufio.NewReader(conn)),
	}, nil
}


type Slave struct {
	masters map[string]Master
	fsdata  FsData
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

func (self *Slave) OnDisconnected(master *Master) {
	delete(self.masters, master.address)
	if len(self.masters) == 0 {
		panic("No active masters!")
	}
}

func (self *Slave) Close() error {
	var err error = nil
	for _, v := range self.masters {
		err = v.Close()
	}
	return err
}

func (self *Slave) Handle(master *Master, trans helper.Transaction) error {
	fmt.Println(trans)
	if trans.Action == "fs_get" {
		err := self.fsdata.GetFs(&trans)
		if err != nil {
			return err
		}
	} else if trans.Action[:3] == "fs_" {
		if err := self.fsdata.Handle(trans); err != nil {
			return err
		}
	} else if trans.Action == "mr_map" {
		if err := os.Mkdir(trans.Id, os.ModeTemporary|os.ModeDir|os.ModePerm); err != nil {
			return err
		}

		errMap := self.fsdata.DoMap(master, &trans)
		if err := os.RemoveAll(trans.Id); err != nil {
			return err
		}
		if errMap != nil {
			return errMap
		}
	}
	trans.Status = "finished"
	fmt.Println("~", trans)
	master.Send(trans)
	return nil
}

func NewSlave(mnt, dir string) Slave {
	return Slave{
		masters: make(map[string]Master),
		fsdata:  NewFsData(mnt, dir),
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

	slave := NewSlave(*mntv, "./")
	defer slave.Close()

	if err := slave.fsdata.Read(); err != nil {
		panic(err)
	}
	slave.fsdata.data.Write("1.fsdat")
	slave.fsdata.ClearFs()

	fmt.Println(slave.fsdata)

	if err := slave.Connect(*master); err != nil {
		panic(err)
	}

	select {}
}
