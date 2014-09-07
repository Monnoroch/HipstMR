package main

import (
	"HipstMR/master"
	"flag"
	"fmt"
)

func main() {
	help := flag.Bool("help", false, "print this help")
	address := flag.String("address", "", "master adress")
	flag.Parse()
	if *help || *address == "" {
		flag.PrintDefaults()
		return
	}

	mas := master.NewMaster(*address)
	if err := mas.Run(); err != nil {
		fmt.Println("Error:", err)
	}
}
