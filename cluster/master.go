package main

import (
	"HipstMR/master"
	"HipstMR/utils"
	"flag"
	"fmt"
)

func main() {
	help := flag.Bool("help", false, "print this help")
	cfgFile := flag.String("config", "", "config file path")
	address := flag.String("address", "", "master adress")
	flag.Parse()
	if *help || *address == "" || *cfgFile == "" {
		flag.PrintDefaults()
		return
	}

	cfg, err := utils.NewConfig(*cfgFile)
	if err != nil {
		panic(err)
	}

	mas := master.NewMaster(*address, *cfgFile, cfg)
	if err := mas.Run(); err != nil {
		fmt.Println("Error:", err)
	}
}
