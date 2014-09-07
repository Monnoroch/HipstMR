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

	server := master.NewMaster(*address)
	if err := server.Run(); err != nil {
		fmt.Println("Error:", err)
	}
}
