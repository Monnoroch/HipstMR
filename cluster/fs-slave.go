package main

import (
	"HipstMR/filesystem"
	"HipstMR/utils"
	"flag"
	"fmt"
)

func main() {
	help := flag.Bool("help", false, "print this help")
	cfgFile := flag.String("config", "", "config file path")
	address := flag.String("address", "", "fileserver adress")
	mnt := flag.String("mnt", "", "mount dir")
	name := flag.String("name", "", "machine name")
	flag.Parse()
	if *help || *address == "" || *cfgFile == "" || *name == "" {
		flag.PrintDefaults()
		return
	}

	cfg, err := utils.NewConfig(*cfgFile)
	if err != nil {
		panic(err)
	}

	nodeCfg := cfg.GetMachineCfg(*name)
	if nodeCfg == nil {
		panic("No machine with name \"" + *name + "\".")
	}

	slave := filesystem.NewSlave(*address, *mnt, *name, *cfgFile, *nodeCfg)
	if err := slave.Run(); err != nil {
		fmt.Println("Error:", err)
	}
}
