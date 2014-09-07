package filesystem

import (
	"errors"
	"fmt"
	"net"
	"path"
	"os/exec"
	"HipstMR/utils"
)


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

func (self *Slave) handle(conn net.Conn) {
	defer conn.Close()
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
