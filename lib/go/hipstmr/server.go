package hipstmr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

type Job interface {
	Name() string
}

type Map interface {
	Job
	Start()
	Do(key, subKey, value []byte, output *JobOutput)
	Finish()
}

type Reduce interface {
	Job
	Start()
	Do()
	Finish()
}

type Server struct {
	address string
}

func NewServer(address string) Server {
	return Server{
		address: address,
	}
}

func (self *Server) Map(params *Params, mapObj Map) error {
	buf, err := json.Marshal(mapObj)
	if err != nil {
		return err
	}

	params.Type = "map"
	params.Name = mapObj.Name()
	params.Object = buf

	var trans transaction
	trans.Params = params
	trans.Status = "starting"

	return self.run(&trans)
}

func (self *Server) MapIO(from, to string, mapObj Map) error {
	return self.Map(NewParamsIO(from, to), mapObj)
}

type transaction struct {
	Id      string      `json:"id"`
	Status  string      `json:"status"`
	Params  *Params     `json:"params"`
	Payload interface{} `json:"payload"`
}

func writeAll(conn net.Conn, buf []byte) error {
	total := len(buf)
	sum := 0
	for sum != total {
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		sum += n
		buf = buf[:n]
	}
	return nil
}

func (self *Server) Move(params *Params) error {
	params.Type = "move"

	var trans transaction
	trans.Params = params
	trans.Status = "starting"

	return self.run(&trans)
}

func (self *Server) MoveIO(from, to string) error {
	return self.Move(NewParamsIO(from, to))
}

func (self *Server) Copy(params *Params) error {
	params.Type = "copy"

	var trans transaction
	trans.Params = params
	trans.Status = "starting"

	return self.run(&trans)
}

func (self *Server) CopyIO(from, to string) error {
	return self.Copy(NewParamsIO(from, to))
}

func (self *Server) Drop(params *Params) error {
	params.Type = "drop"

	var trans transaction
	trans.Params = params
	trans.Status = "starting"

	return self.run(&trans)
}

func (self *Server) DropTbl(tbl string) error {
	return self.Drop(NewParams().AddInput(tbl))
}

func (self *Server) run(trans *transaction) error {
	res, err := json.Marshal(trans)
	if err != nil {
		return err
	}

	conn, err := net.Dial("tcp", self.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeAll(conn, res); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)

	for {
		var t transaction
		err = decoder.Decode(&t)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		fmt.Println("Transaction " + t.Id + ": " + t.Status)

		str, ok := t.Payload.(string)
		if ok {
			fmt.Println("Stderr:")
			fmt.Println(str)
		}
	}
	return nil
}
