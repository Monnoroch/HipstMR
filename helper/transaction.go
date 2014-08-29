package helper

import (
	"HipstMR/lib/go/hipstmr"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"net"
)

type Params struct {
	Params       *hipstmr.Params `json:"params"`
	Chunks       []string        `json:"chunks"`
	OutputTables []string        `json:"output_tables"`
	OutputChunkNums []uint64 `json:"output_chunks_nums"`
}

type Transaction struct {
	Id      string      `json:"id"`
	Action  string      `json:"action"`
	Status  string      `json:"status"`
	Params  Params      `json:"params"`
	Payload interface{} `json:"payload"`
}

func (self *Transaction) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	return WriteAll(conn, bytes)
}

func NewTransaction(action string) Transaction {
	return Transaction{
		Id:     uuid.New(),
		Status: "started",
		Action: action,
	}
}
