package helper

import (
	// "fmt"
	"HipstMR/lib/go/hipstmr"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"net"
)

type Params struct {
	Params *hipstmr.Params `json:"params"`
	Chunks []string        `json:"chunks"`
}

type Transaction struct {
	Id      string      `json:"id"`
	Status  string      `json:"status"`
	Params  Params      `json:"params"`
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
		Id:     uuid.New(),
		Status: status,
	}
}
