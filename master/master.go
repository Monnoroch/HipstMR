package master

import(
	"net"
	"fmt"
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

func (self *Master) Go(sig chan struct{}) {
	go func() {
		if err := self.Run(); err != nil {
			fmt.Println("Error Master.Go:", err)
		}
		sig <- struct{}{}
	}()
}


func (self *Master) GoForever() {
	go func() {
		if err := self.Run(); err != nil {
			fmt.Println("Error Master.GoForever:", err)
		}
		self.GoForever()
	}()
}

func (self *Master) handle(conn net.Conn) {

}

func NewMaster(addr string) Master {
	return Master{
		addr: addr,
	}
}
