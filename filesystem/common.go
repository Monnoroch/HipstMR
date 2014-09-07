package filesystem

import (
	"net"
	"encoding/json"
	"HipstMR/utils"
)


type FileSystemCommand struct {
	Id      string            `json:"id"`
	Status  string            `json:"status"`
	Action  string            `json:"action"`
	From []string `json:"from"`
	To string `json:"to"`
	Payload []byte            `json:"payload"`
}

func (self *FileSystemCommand) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	return utils.WriteAll(conn, bytes)
}
