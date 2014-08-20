package main

import (
	"fmt"
	"net"
	"io"
	"time"
	"os"
	"os/exec"
	"path"
	"bufio"
	"flag"
	"encoding/json"
	"HipstMR/lib/go/hipstmr"
)


func dumpTransaction(trans hipstmr.Transaction) string {
	res := ""
	for k, v := range trans.Params.Files {
		isBinary := false
		if k[0] == '!' {
			k = k[1:]
			isBinary = true
		}

		p := path.Join(trans.Id, k)
		f, err := os.Create(p)
		if err != nil {
			panic(err)
		}

		f.Write([]byte(v))
		f.Close()

		if isBinary {
			if err := os.Chmod(p, 0777); err != nil {
				panic(err)
			}
			res = k
		}
	}
	return res
}

func onTransaction(trans hipstmr.Transaction, conn net.Conn) {
	if err := os.Mkdir(trans.Id, os.ModeTemporary|os.ModeDir|0777); err != nil {
		panic(err)
	}
	defer os.RemoveAll(trans.Id)

	bin := dumpTransaction(trans)

	trans.Status = "running"
	sendTrans(conn, trans)
	fmt.Println("Transaction " + trans.Id + " " + trans.Status)
	cmd := exec.Command(path.Join(".", trans.Id, bin), "-hipstmrjob", "-type", trans.Params.Type, "-name", trans.Params.Name)
	out, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	fmt.Print(string(out))

	time.Sleep(time.Millisecond * 500)
	trans.Status = "finished"
	sendTrans(conn, trans)
}

func sendTrans(conn net.Conn, trans hipstmr.Transaction) error {
	trans.Params = nil
	bytes, err := json.Marshal(trans)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func main() {
	help := flag.Bool("help", false, "print this help")
	master := flag.String("master", "", "master adress")
	flag.Parse()
	if *help || *master == "" {
		flag.PrintDefaults()
		return
	}

	var trans hipstmr.Transaction
	trans.Status = "slave_waiting"

	res, err := json.Marshal(&trans)
	if err != nil {
		panic(err)
	}

	conn, err := net.Dial("tcp", *master)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	conn.Write(res)

	decoder := json.NewDecoder(bufio.NewReader(conn))

	for {
		var trans hipstmr.Transaction
		err = decoder.Decode(&trans)
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		go onTransaction(trans, conn)
	}
}
