package main

import (
	"HipstMR/fileserver"
	"flag"
)

func main() {
	help := flag.Bool("help", false, "print this help")
	address := flag.String("address", "", "fileserver adress")
	mnt := flag.String("mnt", "", "mount dir")
	flag.Parse()
	if *help || *address == "" {
		flag.PrintDefaults()
		return
	}

	server := fileserver.NewServer(*address, *mnt)
	panic(server.Run())
}
