package main

import (
	"flag"
	"HipstMR/lib/go/hipstmr"
)

type MyMap struct {
	Val string `json:"val"`
}

func (self *MyMap) Name() string {
	return "MyMap"
}

func (self *MyMap) Start() {}
func (self *MyMap) Do() {}
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
	server.Map(hipstmr.NewParamsIO("input", "output").AddFile("f.txt"), &MyMap{Val: "hello!"})
}
