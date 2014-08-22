package hipstmr

import (
	"net"
	"fmt"
	"io"
	"bufio"
	"encoding/json"
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
	params.Type = "map"
	params.Name = mapObj.Name()
	buf, err := json.Marshal(mapObj)
	if err != nil {
		return err
	}
	params.Object = buf

	var trans Transaction
	trans.Params = params
	trans.Status = "starting"

	res, err := json.Marshal(&trans)
	if err != nil {
		return err
	}

	conn, err := net.Dial("tcp", self.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	total := len(res)
	sum := 0
	for ; sum != total; {
		n, err := conn.Write(res)
		if err != nil {
			return err
		}
		sum += n
		res = res[:n]
	}

	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)

	for {
		var trans Transaction
		err = decoder.Decode(&trans)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		fmt.Println("Transaction " + trans.Id + " " + trans.Status)
	}
	return nil
}

func (self *Server) MapIO(from, to string, mapObj Map) error {
	return self.Map(NewParamsIO(from, to), mapObj)
}
