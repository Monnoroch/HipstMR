package fileserver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"os/exec"
	"HipstMR/utils"
)


type FileServerCommand struct {
	Id      string            `json:"id"`
	Status  string            `json:"status"`
	Action  string            `json:"action"`
	Params  map[string]string `json:"params"`
	Payload []byte            `json:"payload"`
}

func (self *FileServerCommand) Send(conn net.Conn) error {
	bytes, err := json.Marshal(self)
	if err != nil {
		return err
	}
	return utils.WriteAll(conn, bytes)
}

type Server struct {
	addr string
	mnt  string
}

func (self *Server) Run() (rerr error) {
	sock, err := net.Listen("tcp", self.addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := sock.Close(); err != nil {
			if rerr != nil {
				rerr = doubleErr(rerr, err)
			} else {
				rerr = err
			}
		}
	}()

	for {
		conn, err := sock.Accept()
		if err != nil {
			return err
		}

		go handle(self.mnt, conn)
	}

	return nil
}

func (self *Server) RunProcess(binaryPath string) (string, string, error) {
	return utils.ExecCmd(exec.Command(path.Clean(binaryPath), "-address", self.addr, "-mnt", self.mnt))
}

func NewServer(addr, mnt string) Server {
	return Server{
		addr: addr,
		mnt:  mnt,
	}
}

func doubleErr(err, err1 error) error {
	return errors.New(fmt.Sprintf("Errors: {%v, %v}", err, err1))
}

func tripleErr(err, err1, err2 error) error {
	return errors.New(fmt.Sprintf("Errors: {%v, %v, %v}", err, err1, err2))
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

	if err := utils.WriteAll(out, data); err != nil {
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
			fmt.Println("Error copyFile:", err)
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

func failed(cmd FileServerCommand, conn net.Conn, origErr error) {
	cmd.Status = "failed"
	cmd.Params = nil
	cmd.Payload = nil
	if err := cmd.Send(conn); err != nil {
		fmt.Printf("Errors failed: {%v, %v}\n", origErr, err)
	} else {
		fmt.Println("Error failed:", origErr)
	}
}

func send(cmd FileServerCommand, conn net.Conn) {
	if err := cmd.Send(conn); err != nil {
		// just can't tell the client about the result
		fmt.Println("Error send:", err)
	}
}

func success(cmd FileServerCommand, conn net.Conn) {
	cmd.Status = "finished"
	send(cmd, conn)
}

func copyLocal(from, to string, cmd FileServerCommand, conn net.Conn) error {
	if to == from {
		return nil
	}

	if err := copyFile(from, to); err != nil {
		return err
	}

	success(cmd, conn)
	return nil
}

func copyRemote(from, to, addr string, cmd FileServerCommand, conn net.Conn) error {
	file, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}

	connTo, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := connTo.Close(); err != nil {
			fmt.Println("Error copyRemote:", err)
		}
	}()

	cmdTo := FileServerCommand{
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

	var cmdFrom FileServerCommand
	if err := json.NewDecoder(bufio.NewReader(connTo)).Decode(&cmdFrom); err != nil {
		return err
	}

	send(cmdFrom, conn)
	return nil
}

func copy(cmd FileServerCommand, mnt string, conn net.Conn) error {
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

func moveLocal(from, to string, cmd FileServerCommand, conn net.Conn) error {
	if to == from {
		return nil
	}

	if err := moveFile(from, to); err != nil {
		return err
	}

	success(cmd, conn)
	return nil
}

func moveRemote(from, to, addr string, cmd FileServerCommand, conn net.Conn) error {
	file, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}

	connTo, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := connTo.Close(); err != nil {
			fmt.Println("Error copyRemote:", err)
		}
	}()

	cmdTo := FileServerCommand{
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

	var cmdFrom FileServerCommand
	if err := decoder.Decode(&cmdFrom); err != nil {
		return err
	}

	if cmdFrom.Status != "failed" {
		if err := os.Remove(from); err != nil {
			// we assume, os.Remove never fails, so no fallback on remote server =)
			fmt.Println("Error moveRemote:", err)
		}
	}

	success(cmdFrom, conn)
	return nil
}

func move(cmd FileServerCommand, mnt string, conn net.Conn) error {
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

func del(cmd FileServerCommand, mnt string, conn net.Conn) error {
	from := path.Clean(path.Join(mnt, cmd.Params["from"]))
	if err := os.Remove(from); err != nil {
		return err
	}

	success(cmd, conn)
	return nil
}

func put(cmd FileServerCommand, mnt string, conn net.Conn) error {
	to := path.Clean(path.Join(mnt, cmd.Params["to"]))
	if err := createFile(to, cmd.Payload); err != nil {
		return err
	}

	success(cmd, conn)
	return nil
}

func get(cmd FileServerCommand, mnt string, conn net.Conn) error {
	from := path.Clean(path.Join(mnt, cmd.Params["from"]))

	file, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}

	cmd.Payload = file
	success(cmd, conn)
	return nil
}

func onCommand(cmd FileServerCommand, mnt string, conn net.Conn) error {
	fmt.Println(mnt, "Received " + cmd.Action + " command:", cmd)
	defer func() {
		fmt.Println("Done with " + cmd.Action)
	}()
	switch cmd.Action {
	case "get":
		return get(cmd, mnt, conn)
	case "put":
		return put(cmd, mnt, conn)
	case "copy":
		return copy(cmd, mnt, conn)
	case "move":
		return move(cmd, mnt, conn)
	case "del":
		return del(cmd, mnt, conn)
	default:
		return errors.New("Unknown command " + cmd.Action)
	}
}

func doHandle(mnt string, conn net.Conn) error {
	fmt.Println("Started doHandle", mnt)
	defer fmt.Println("Done doHandle", mnt)
	decoder := json.NewDecoder(bufio.NewReader(conn))
	for {
		var cmd FileServerCommand
		err := decoder.Decode(&cmd)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err := onCommand(cmd, mnt, conn); err != nil {
			failed(cmd, conn, err)
			return nil
		}
	}
	return nil
}

func handle(mnt string, conn net.Conn) {
	err := doHandle(mnt, conn)
	if err != nil {
		failed(FileServerCommand{}, conn, err)
	}

	if err := conn.Close(); err != nil {
		fmt.Printf("Error handle: failed to close connection %v.\n", err)
	}
}
