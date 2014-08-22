package main

import (
	"os"
	"encoding/binary"
)

func WriteValueToFile(f *os.File, value string) {
	arr := []byte{0, 0}
	binary.LittleEndian.PutUint16(arr, uint16(len(value)))
	f.Write(arr)
	f.Write([]byte(value))
}

func WriteLineToFile(f *os.File, key, subKey, value string) {
	WriteValueToFile(f, key)
	WriteValueToFile(f, subKey)
	WriteValueToFile(f, value)
}

func main() {
	f, err := os.Create("/home/monnoroch/go/src/HipstMR/slave1/data/test/tbl1.chunk.4")
	if err != nil {
		panic(err)
	}

	WriteLineToFile(f, "jjj6", "kkk6", "lll6")
}
