package hipstmr

import (
	"net"
	"encoding/json"
	"code.google.com/p/go-uuid/uuid"
)


type Transaction struct {
	Params *Params `json:"params"`
	Id string `json:"id"`
	Status string `json:"status"`
	Payload interface{} `json:"payload"`
}

func (self *Transaction) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func NewTransaction(status string) Transaction {
	return Transaction{
		Id: uuid.New(),
		Status: status,
	}
}
