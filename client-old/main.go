package main

import (
	"HipstMR/lib/go/hipstmr"
	"flag"
)

type MyMap struct {
	Val string `json:"val"`
}

func (self *MyMap) Name() string {
	return "MyMap"
}

func (self *MyMap) Start() {}

func (self *MyMap) Do(key, subKey, value []byte, output *hipstmr.JobOutput) {
	output.Add(key, subKey, value)
}

func (self *MyMap) Finish() {}

func main() {
	hipstmr.Register(&MyMap{})
	hipstmr.Init()

	help := flag.Bool("help", false, "print this help")
	master := flag.String("master", "", "master adress")
	flag.Parse()
	if *help || *master == "" {
		flag.PrintDefaults()
		return
	}

	server := hipstmr.NewServer(*master)
	server.Map(hipstmr.NewParamsIO("tbl2", "output").AddFile("f.txt").AddInput("tbl1"), &MyMap{Val: "hello!"})
	server.Map(hipstmr.NewParamsIO("output", "output1").AddFile("f.txt"), &MyMap{Val: "hello 2!"})
	server.MoveIO("output1", "output2")
	server.CopyIO("output2", "output3")
	server.DropTbl("output2")
	server.DropTbl("output3")
	server.DropTbl("output")
}
