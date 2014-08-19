package main

import (
	"fmt"
	"net"
	"io"
	"bufio"
	"flag"
	"encoding/json"
	"HipstMR/lib/go/hipstmr"
)


func main() {
	help := flag.Bool("help", false, "print this help")
	master := flag.String("master", "", "master adress")
	flag.Parse()
	if *help || *master == "" {
		flag.PrintDefaults()
		return
	}

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

	conn, err := net.Dial("tcp", *master)
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
