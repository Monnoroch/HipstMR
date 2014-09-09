package filesystem

import (
	"errors"
	"fmt"
	"net"
	"path"
	"os/exec"
	"bufio"
	"HipstMR/utils"
	"encoding/json"
	"io"
)

type FileNumCfg struct {
	File string `json:"file"`
	Num uint `json:"num"`
}

type ChunkInfoCfg struct {
	Host string `json:"host"`
	Id string `json:"id"`
	Name string `json:"name"`
	Files []FileNumCfg `json:"files"`
}

type FileInfoCfg struct {
	Name string `json:"name"`
	Chunks []ChunkInfoCfg `json:"chunks"`
}

type FileNum struct {
	File *FileInfo
	Num uint
}

type ChunkInfo struct {
	Host string
	Id string
	Name string
	Size uint
	Files []*FileNum
}

type FileInfo struct {
	Name string
	Size uint
	ChunksCount uint
	Chunks []*ChunkInfo
}


type Slave struct {
	addr string
	mnt  string
	name string
	cfgPath string
	cfg utils.MachineCfg
}

func (self *Slave) Run() (rerr error) {
	sock, err := net.Listen("tcp", self.addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := sock.Close(); err != nil {
			if rerr != nil {
				rerr = doubleErr(rerr, err)
			} else {
				rerr = err
			}
		}
	}()

	for {
		conn, err := sock.Accept()
		if err != nil {
			return err
		}

		go self.handle(conn)
	}

	return nil
}

func (self *Slave) RunProcess(binaryPath string) (string, string, error) {
	return utils.ExecCmd(exec.Command(path.Clean(binaryPath), "-address", self.addr, "-mnt", self.mnt, "-name", self.name, "-config", self.cfgPath))
}

func (self *Slave) put(cmd FileSystemCommand, conn net.Conn) error {
	// TODO
	return nil
}

func (self *Slave) doHandle(cmd FileSystemCommand, conn net.Conn) error {
	switch cmd.Action {
	case "put":
		return self.put(cmd, conn)
	default:
		return errors.New("Unknown command " + cmd.Action)
	}
	return nil
}

func (self *Slave) handle(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(bufio.NewReader(conn))

	for {
		var cmd FileSystemCommand
		err := decoder.Decode(&cmd)
		if err == io.EOF {
			break
		}

		if err != nil {
			failed(cmd, conn, err)
			return
		}

		if err := self.doHandle(cmd, conn); err != nil {
			failed(cmd, conn, err)
			return
		}
	}
}

func NewSlave(addr, mnt, name, cfgPath string, cfg utils.MachineCfg) Slave {
	return Slave{
		addr: addr,
		mnt:  mnt,
		name: name,
		cfgPath: cfgPath,
		cfg: cfg,
	}
}

func doubleErr(err, err1 error) error {
	return errors.New(fmt.Sprintf("Errors: {%v, %v}", err, err1))
}
