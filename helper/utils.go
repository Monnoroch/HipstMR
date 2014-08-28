package helper

import (
	"io"
)

func WriteAll(writer io.Writer, buf []byte) error {
	for {
		cnt := len(buf)
		n, err := writer.Write(buf)
		if err != nil {
			return err
		}

		if n == cnt {
			break
		}

		buf = buf[n:]
	}
	return nil
}
