package main

import (
	"fmt"
	"net"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"errors"
	"bufio"
	"bytes"
	"strings"
	"flag"
	"encoding/json"
	"HipstMR/helper"
)

type JobConfig struct {
	Jtype string `json:"type"`
	Name string `json:"name"`
	Chunks []string `json:"chunks"`
	OutputTables []string `json:"output_tables"`
	Object []byte `json:"object"`
}

func traverseDirectoryRec(name string, isRoot bool) (map[string][]string, error) {
	p := path.Clean(name)
	dir, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}

	res := map[string][]string{}
	for _, v := range dir {
		if v.IsDir() {
			r, err := traverseDirectoryRec(path.Join(name, v.Name()), false)
			if err != nil {
				return nil, err
			}

			for k, v := range r {
				if !isRoot {
					res[k] = v
				} else {
					res[k[len(name):]] = v
				}
			}
		} else {
			nm := v.Name()
			i := strings.Index(nm, ".chunk.")
			if i == -1 {
				return nil, errors.New("Not a chunk: " + path.Join(name, nm))
			}

			tag := nm[:i]
			if !isRoot {
				tag = path.Join(name, tag)
			}
			id := nm[i+len(".chunk."):]
			res[tag] = append(res[tag], id)
		}
	}
	return res, nil
}

func traverseDirectory(name string) (map[string][]string, error) {
	return traverseDirectoryRec(name, true)
}

func dumpTransaction(trans helper.Transaction) string {
	res := ""
	for k, v := range trans.Params.Params.Files {
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

func onTransaction(trans helper.Transaction, conn net.Conn) {
	if trans.Status == "get_chunks" {
		dir, err := traverseDirectory("data/")
		if err != nil {
			trans.Status = "failed"
			sendTrans(conn, trans)
			return
		}

		trans.Payload = dir
		trans.Status = "chunks"
		sendTrans(conn, trans)
		return
	} else if trans.Status == "run_job" {
		trans.Status = "received_files"
		go sendTransOrPrint(conn, trans)

		if err := os.Mkdir(trans.Id, os.ModeTemporary|os.ModeDir|0777); err != nil {
			panic(err)
		}
		defer os.RemoveAll(trans.Id)

		bin := dumpTransaction(trans)

		trans.Status = "running"
		sendTrans(conn, trans)
		fmt.Println("Transaction " + trans.Id + " " + trans.Status)
		cfg := JobConfig{
			Jtype: trans.Params.Params.Type,
			Name: trans.Params.Params.Name,
			Chunks: trans.Params.Chunks,
			Object: trans.Params.Params.Object,
			OutputTables: trans.Params.Params.OutputTables,
		}
		buf, err := json.Marshal(cfg)
		if err != nil {
			panic(err)
		}

		cmd := exec.Command(path.Join(".", trans.Id, bin), "-hipstmrjob")
		cmd.Stdin = bytes.NewReader(buf)
		out, err := cmd.CombinedOutput()
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		fmt.Print(string(out))
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

		if err != nil {
			panic(err)
		}

		trans.Status = "finished"
		sendTrans(conn, trans)
	}
}

func sendTrans(conn net.Conn, trans helper.Transaction) error {
	trans.Params.Params = nil
	bytes, err := json.Marshal(trans)
	if err != nil {
		return err
	}
	conn.Write(bytes)
	return nil
}

func sendTransOrPrint(conn net.Conn, trans helper.Transaction) {
	if err := sendTrans(conn, trans); err != nil {
		fmt.Println("Error:", err)
	}
}

func sendTransOrFail(conn net.Conn, trans helper.Transaction) error {
	if err := sendTrans(conn, trans); err != nil {
		trans.Status = "failed"
		sendTrans(conn, trans)
		return err
	}
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

	var trans helper.Transaction
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
		var trans helper.Transaction
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
