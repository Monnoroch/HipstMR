package filesystem

import (
	"net"
	"fmt"
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

func failed(cmd FileSystemCommand, conn net.Conn, origErr error) {
	cmd.Status = "failed"
	cmd.From = nil
	cmd.To = ""
	cmd.Payload = nil
	if err := cmd.Send(conn); err != nil {
		fmt.Printf("Errors failed: {%v, %v}\n", origErr, err)
	} else {
		fmt.Println("Error failed:", origErr)
	}
}
