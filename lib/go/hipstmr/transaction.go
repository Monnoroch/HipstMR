package hipstmr

import (
	"net"
	"encoding/json"
)


type Transaction struct {
	Params *Params `json:"params"`
	Id string `json:"id"`
	Status string `json:"status"`
}

func (self *Transaction) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}