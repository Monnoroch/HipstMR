package master

import(
	"net"
	"path"
	"os/exec"
	"HipstMR/utils"
)

type Master struct {
	addr string
	cfgPath string
	cfg utils.Config
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
	return utils.ExecCmd(exec.Command(path.Clean(binaryPath), "-address", self.addr, "-config", self.cfgPath))
}

func (self *Master) handle(conn net.Conn) {
	defer conn.Close()
}


func NewMaster(addr, cfgPath string, cfg utils.Config) Master {
	return Master{
		addr: addr,
		cfgPath: cfgPath,
		cfg: cfg,
	}
}
