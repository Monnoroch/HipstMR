package main

import (
	"fmt"
	"net"
	"io"
	"bufio"
	"encoding/json"
	"HipstMR/lib/go/hipstmr"
)


func main() {
	params := hipstmr.NewParamsIO("input", "output")
	params.Files["./f.txt"] = ""
	params.Type = "map"
	params.Name = "MyMap"

	var trans hipstmr.Transaction
	trans.Params = params
	trans.Status = "starting"

	res, err := json.Marshal(&trans)
	if err != nil {
		panic(err)
	}

	conn, err := net.Dial("tcp", "localhost:8013")
	if err != nil {
		panic(err)
	}

	conn.Write(res)

	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)

	for {
		var trans hipstmr.Transaction
		err = decoder.Decode(&trans)
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Println("Transaction " + trans.Id + " " + trans.Status)
	}
}
