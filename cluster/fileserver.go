package main

import (
	"HipstMR/fileserver"
	"flag"
	"fmt"
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
	if err := server.Run(); err != nil {
		fmt.Println("Error:", err)
	}
}
