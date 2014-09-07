package master

import(
	"net"
	"path"
	"bytes"
	"os/exec"
)

type Master struct {
	addr string
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

		go self.handle(conn)
	}
	return nil
}

func (self *Master) RunProcess(binaryPath string) (string, string, error) {
	cmd := exec.Command(path.Clean(binaryPath), "-address", self.addr)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return string(stdout.Bytes()), string(stderr.Bytes()), err
}

func (self *Master) handle(conn net.Conn) {

}

func NewMaster(addr string) Master {
	return Master{
		addr: addr,
	}
}
