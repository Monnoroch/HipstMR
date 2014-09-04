package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
)

func doubleErr(err, err1 error) error {
	return errors.New(fmt.Sprintf("Errors: {%v, %v}", err, err1))
}

func tripleErr(err, err1, err2 error) error {
	return errors.New(fmt.Sprintf("Errors: {%v, %v, %v}", err, err1, err2))
}

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

func createFile(to string, data []byte) (rerr error) {
	base := path.Dir(to)
	if err := os.MkdirAll(base, os.ModeDir|os.ModeTemporary|os.ModePerm); err != nil {
		return err
	}

	out, err := os.Create(to)
	if err != nil {
		return err
	}

	defer func() {
		cerr := out.Close()
		if rerr == nil {
			rerr = cerr
		} else {
			rerr = doubleErr(rerr, cerr)
		}
	}()

	if err := WriteAll(out, data); err != nil {
		return err
	}

	return nil
}


func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}

	defer func() {
		// non-fatal to transaction
		if err := in.Close(); err != nil {
			fmt.Println("Error:", err)
		}
	}()

	out, err := os.Create(to)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		if err1 := out.Close(); err1 != nil {
			return doubleErr(err, err1)
		}
		return err
	}

	return out.Close()
}

func moveFile(from, to string) error {
	base := path.Dir(to)
	if err := os.MkdirAll(base, os.ModeDir|os.ModeTemporary|os.ModePerm); err != nil {
		return err
	}

	if err := os.Rename(from, to); err != nil {
		if err1 := os.RemoveAll(base); err1 != nil {
			return doubleErr(err, err1)
		}
		return err
	}

	return nil
}

type fileServerCommand struct {
	Id      string            `json"id"`
	Status  string            `json"status"`
	Action  string            `json"action"`
	Params  map[string]string `json"params"`
	Payload []byte            `json"payload"`
}

func (self *fileServerCommand) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	return WriteAll(conn, bytes)
}

func Failed(cmd fileServerCommand, conn net.Conn, origErr error) {
	cmd.Status = "failed"
	cmd.Params = nil
	cmd.Payload = nil
	if err := cmd.Send(conn); err != nil {
		fmt.Println("Errors:", origErr, err)
	} else {
		fmt.Println("Error:", origErr)
	}
}

func Send(cmd fileServerCommand, conn net.Conn) {
	if err := cmd.Send(conn); err != nil {
		// just can't tell the client about the result
		fmt.Println("Error:", err)
	}
}

func Success(cmd fileServerCommand, conn net.Conn) {
	cmd.Status = "finished"
	Send(cmd, conn)
}

func copyLocal(from, to string, cmd fileServerCommand, conn net.Conn) error {
	if to == from {
		return nil
	}

	if err := copyFile(from, to); err != nil {
		return err
	}

	Success(cmd, conn)
	return nil
}

func copyRemote(from, to, addr string, cmd fileServerCommand, conn net.Conn) error {
	file, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}

	connTo, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	cmdTo := fileServerCommand{
		Id:     cmd.Id,
		Status: "started",
		Action: "put",
		Params: map[string]string{
			"to": to,
		},
		Payload: file,
	}
	if err := cmdTo.Send(connTo); err != nil {
		return err
	}

	var cmdFrom fileServerCommand
	if err := json.NewDecoder(bufio.NewReader(connTo)).Decode(&cmdFrom); err != nil {
		return err
	}

	Send(cmdFrom, conn)
	return nil
}

func copy(cmd fileServerCommand, mnt string, conn net.Conn) error {
	from := path.Clean(path.Join(mnt, cmd.Params["from"]))
	to := cmd.Params["to"]
	if to == "" {
		to = from
	}
	addr, ok := cmd.Params["addr"]
	if !ok || addr == "" {
		return copyLocal(from, path.Clean(path.Join(mnt, to)), cmd, conn)
	} else {
		return copyRemote(from, to, addr, cmd, conn)
	}
}

func moveLocal(from, to string, cmd fileServerCommand, conn net.Conn) error {
	if to == from {
		return nil
	}

	if err := moveFile(from, to); err != nil {
		return err
	}

	Success(cmd, conn)
	return nil
}

func moveRemote(from, to, addr string, cmd fileServerCommand, conn net.Conn) error {
	file, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}

	connTo, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	cmdTo := fileServerCommand{
		Id:     cmd.Id,
		Status: "started",
		Action: "put",
		Params: map[string]string{
			"to": to,
		},
		Payload: file,
	}
	if err := cmdTo.Send(connTo); err != nil {
		return err
	}

	decoder := json.NewDecoder(bufio.NewReader(connTo))

	var cmdFrom fileServerCommand
	if err := decoder.Decode(&cmdFrom); err != nil {
		return err
	}

	if cmdFrom.Status != "failed" {
		if err := os.Remove(from); err != nil {
			// we assume, os.Remove never fails, so no fallback on remote server =)
			fmt.Println("Error:", err)
		}
	}

	Success(cmdFrom, conn)
	return nil
}

func move(cmd fileServerCommand, mnt string, conn net.Conn) error {
	from := path.Clean(path.Join(mnt, cmd.Params["from"]))
	to := cmd.Params["to"]
	if to == "" {
		to = from
	}
	addr, ok := cmd.Params["addr"]
	if !ok || addr == "" {
		return moveLocal(from, path.Clean(path.Join(mnt, to)), cmd, conn)
	} else {
		return moveRemote(from, to, addr, cmd, conn)
	}
}

func put(cmd fileServerCommand, mnt string, conn net.Conn) error {
	to := path.Clean(path.Join(mnt, cmd.Params["to"]))
	if err := createFile(to, cmd.Payload); err != nil {
		return err
	}

	Success(cmd, conn)
	return nil
}

func del(cmd fileServerCommand, mnt string, conn net.Conn) error {
	from := path.Clean(path.Join(mnt, cmd.Params["from"]))
	if err := os.Remove(from); err != nil {
		return err
	}

	Success(cmd, conn)
	return nil
}

func onCommand(cmd fileServerCommand, mnt string, conn net.Conn) error {
	fmt.Println("Received "+cmd.Action+" command:", cmd)
	defer func() {
		fmt.Println("Done with " + cmd.Action)
	}()
	switch cmd.Action {
	case "copy":
		return copy(cmd, mnt, conn)
	case "move":
		return move(cmd, mnt, conn)
	case "put":
		return put(cmd, mnt, conn)
	case "del":
		return del(cmd, mnt, conn)
	default:
		return errors.New("Unknown command " + cmd.Action)
	}
}

func doHandle(mnt string, conn net.Conn) error {
	decoder := json.NewDecoder(bufio.NewReader(conn))
	for {
		var cmd fileServerCommand
		err := decoder.Decode(&cmd)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err := onCommand(cmd, mnt, conn); err != nil {
			Failed(cmd, conn, err)
			return nil
		}
	}
	return nil
}

func handle(mnt string, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("Error: failed to close connection %v.\n", conn)
		}
	}()
	if err := doHandle(mnt, conn); err != nil {
		Failed(fileServerCommand{}, conn, err)
	}
}

func main() {
	help := flag.Bool("help", false, "print this help")
	address := flag.String("address", "", "fileserver adress")
	mnt := flag.String("mnt", "", "mount dir")
	flag.Parse()
	if *help || *address == "" {
		flag.PrintDefaults()
		return
	}

	sock, err := net.Listen("tcp", *address)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := sock.Accept()
		if err != nil {
			panic(err)
		}

		go handle(*mnt, conn)
	}
}
