package main

import (
	// "HipstMR/helper"
	// "bufio"
	// "bytes"
	// "code.google.com/p/go-uuid/uuid"
	// "encoding/json"
	// "errors"
	// "flag"
	// "fmt"
	// "io"
	// "io/ioutil"
	// "net"
	// "os"
	// "os/exec"
	// "path"
	// "strconv"
	// "strings"
)

// type JobConfig struct {
// 	Jtype        string   `json:"type"`
// 	Name         string   `json:"name"`
// 	Dir          string   `json:"dir"`
// 	Chunks       []string `json:"chunks"`
// 	OutputTables []string `json:"output_tables"`
// 	Object       []byte   `json:"object"`
// }

// func traverseDirectoryRec(name string, isRoot bool) (map[string][]string, error) {
// 	p := path.Clean(name)
// 	dir, err := ioutil.ReadDir(p)
// 	if err != nil {
// 		return nil, err
// 	}

// 	res := map[string][]string{}
// 	for _, v := range dir {
// 		if v.IsDir() {
// 			r, err := traverseDirectoryRec(path.Join(name, v.Name()), false)
// 			if err != nil {
// 				return nil, err
// 			}

// 			for k, v := range r {
// 				if !isRoot {
// 					res[k] = v
// 				} else {
// 					res[k[len(name)+1:]] = v
// 				}
// 			}
// 		} else {
// 			nm := v.Name()
// 			i := strings.Index(nm, ".chunk.")
// 			if i == -1 {
// 				return nil, errors.New("Not a chunk: " + path.Join(name, nm))
// 			}

// 			tag := nm[:i]
// 			if !isRoot {
// 				tag = path.Join(name, tag)
// 			}
// 			id := nm[i+len(".chunk."):]
// 			res[tag] = append(res[tag], id)
// 		}
// 	}
// 	return res, nil
// }

// func traverseDirectory(name string) (map[string][]string, error) {
// 	return traverseDirectoryRec(name, true)
// }

// func dumpTransaction(trans helper.Transaction) string {
// 	res := ""
// 	for k, v := range trans.Params.Params.Files {
// 		isBinary := false
// 		if k[0] == '!' {
// 			k = k[1:]
// 			isBinary = true
// 		}

// 		p := path.Join(trans.Id, k)
// 		f, err := os.Create(p)
// 		if err != nil {
// 			panic(err)
// 		}

// 		f.Write([]byte(v))
// 		f.Close()

// 		if isBinary {
// 			if err := os.Chmod(p, os.ModePerm); err != nil {
// 				panic(err)
// 			}
// 			res = k
// 		}
// 	}
// 	return res
// }

// func onTransaction(trans helper.Transaction, conn net.Conn) {
// 	if trans.Action == "get_chunks" {
// 		if err := fsdata.Read("./"); err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		bs, err := json.Marshal(fsdata)
// 		if err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		trans.Payload = bs
// 		trans.Status = "finished"
// 		sendTrans(conn, trans)
// 		fmt.Println("~~~~~ get_chunks")
// 		return
// 	}

// 	if trans.Action == "move" {
// 		fmt.Println(trans.Params.Params.InputTables, trans.Params.OutputTables, trans.Params.Chunks)
// 		for _, inTbl := range trans.Params.Params.InputTables {
// 			for _, v := range trans.Params.Chunks {
// 				tags := fsdata.Chunks[v].Tags
// 				for i, tag := range tags {
// 					if tag == inTbl {
// 						tags[i] = trans.Params.OutputTables[0]
// 					}
// 				}
// 			}
// 		}

// 		if err := fsdata.Write("1.fsdat"); err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		trans.Status = "finished"
// 		sendTrans(conn, trans)
// 		fmt.Println("~~~~~ move", trans)
// 	} else if trans.Action == "copy" {
// 		for _, v := range trans.Params.Chunks {
// 			fsdata.Chunks[v].Tags = append(fsdata.Chunks[v].Tags, trans.Params.OutputTables[0])
// 		}

// 		if err := fsdata.Write("1.fsdat"); err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		trans.Status = "finished"
// 		sendTrans(conn, trans)
// 		fmt.Println("~~~~~ copy", trans)
// 	} else if trans.Action == "drop" {
// 		for _, inTbl := range trans.Params.Params.InputTables {
// 			for _, v := range trans.Params.Chunks {
// 				tags := fsdata.Chunks[v].Tags
// 				ids := []int{}
// 				for i, tag := range tags {
// 					if tag == inTbl {
// 						ids = append(ids, i)
// 					}
// 				}
// 				fmt.Println(inTbl, ids)
// 				if len(ids) == len(tags) {
// 					delete(fsdata.Chunks, v)
// 					os.Remove(path.Join(mnt, v+".chunk"))
// 					continue
// 				}

// 				fmt.Println(tags)
// 				for _, i := range ids {
// 					tags = append(tags[:i], tags[i+1:]...)
// 					fmt.Println(tags)
// 				}

// 				if len(tags) == 0 {
// 					delete(fsdata.Chunks, v)
// 					os.Remove(path.Join(mnt, v+".chunk"))
// 					continue
// 				}

// 				fsdata.Chunks[v].Tags = tags
// 			}
// 		}

// 		if err := fsdata.Write("1.fsdat"); err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		trans.Status = "finished"
// 		sendTrans(conn, trans)
// 		fmt.Println("~~~~~ drop", trans)
// 	} else if trans.Action == "map" {
// 		trans.Status = "received_files"
// 		sendTransOrPrint(conn, trans)

// 		if err := os.Mkdir(trans.Id, os.ModeTemporary|os.ModeDir|os.ModePerm); err != nil {
// 			panic(err)
// 		}
// 		defer os.RemoveAll(trans.Id)

// 		bin := dumpTransaction(trans)

// 		trans.Status = "running"
// 		sendTrans(conn, trans)
// 		fmt.Println("Transaction " + trans.Id + " " + trans.Status)
// 		cfg := JobConfig{
// 			Dir:          uuid.New(),
// 			Jtype:        trans.Params.Params.Type,
// 			Name:         trans.Params.Params.Name,
// 			Object:       trans.Params.Params.Object,
// 			Chunks:       trans.Params.Chunks,
// 			OutputTables: trans.Params.OutputTables,
// 		}
// 		buf, err := json.Marshal(cfg)
// 		if err != nil {
// 			panic(err)
// 		}

// 		cmd := exec.Command(path.Join(".", trans.Id, bin), "-hipstmrjob", "-mnt", mnt)
// 		cmd.Stdin = bytes.NewReader(buf)

// 		var stdout bytes.Buffer
// 		var stderr bytes.Buffer
// 		cmd.Stdout = &stdout
// 		cmd.Stderr = &stderr
// 		if err := cmd.Run(); err != nil {
// 			panic(err)
// 		}

// 		fmt.Println("~~~~~~Stderr:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
// 		fmt.Print(string(stderr.Bytes()))
// 		fmt.Println("~~~~~~Stdout:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
// 		fmt.Print(string(stdout.Bytes()))
// 		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

// 		// cmdPrefix := "!hipstmrjob: "
// 		// for ; ; {
// 		// 	str, err := stdout.ReadString('\n')
// 		// 	if err != nil && err != io.EOF {
// 		// 		panic(err)
// 		// 	}

// 		// 	if strings.HasPrefix(str, cmdPrefix) {
// 		// 		str = str[len(cmdPrefix):len(str)-1]
// 		// 		fmt.Println("command from job", str)
// 		// 	}

// 		// 	if err == io.EOF {
// 		// 		break
// 		// 	}
// 		// }

// 		fmt.Println("!!!", cfg.OutputTables)

// 		for _, tbl := range cfg.OutputTables {
// 			p := path.Clean(path.Join(mnt, cfg.Dir, tbl))
// 			fmt.Println("Moving from dir", p)
// 			dir, err := ioutil.ReadDir(p)
// 			if err != nil {
// 				trans.Status = "failed"
// 				sendTrans(conn, trans)
// 				return
// 			}

// 			for _, v := range dir {
// 				if v.IsDir() || !strings.HasSuffix(v.Name(), ".chunk") {
// 					trans.Status = "failed"
// 					sendTrans(conn, trans)
// 					return
// 				}

// 				nm := v.Name()
// 				num, err := strconv.ParseUint(nm[:len(nm)-len(".chunk")], 10, 64)
// 				if err != nil {
// 					trans.Status = "failed"
// 					sendTrans(conn, trans)
// 					return
// 				}

// 				id := uuid.New()
// 				fmt.Println("Rename", path.Join(p, nm), "to", path.Join(mnt, id+".chunk"))
// 				if err := os.Rename(path.Join(p, nm), path.Join(mnt, id+".chunk")); err != nil {
// 					trans.Status = "failed"
// 					sendTrans(conn, trans)
// 					return
// 				}

// 				fsdata.Chunks[id] = &helper.ChunkData{
// 					Num:  num,
// 					Tags: []string{tbl},
// 				}
// 			}

// 			if err := os.RemoveAll(path.Join(mnt, cfg.Dir)); err != nil {
// 				trans.Status = "failed"
// 				sendTrans(conn, trans)
// 				return
// 			}
// 		}

// 		if err := fsdata.Write("1.fsdat"); err != nil {
// 			trans.Status = "failed"
// 			sendTrans(conn, trans)
// 			return
// 		}

// 		trans.Status = "finished"
// 		trans.Payload = string(string(stderr.Bytes()))
// 		sendTrans(conn, trans)
// 	}
// }

// func sendTrans(conn net.Conn, trans helper.Transaction) error {
// 	trans.Params.Params = nil
// 	bytes, err := json.Marshal(trans)
// 	if err != nil {
// 		return err
// 	}
// 	conn.Write(bytes)
// 	return nil
// }

// func sendTransOrPrint(conn net.Conn, trans helper.Transaction) {
// 	if err := sendTrans(conn, trans); err != nil {
// 		fmt.Println("Error:", err)
// 	}
// }

// func sendTransOrFail(conn net.Conn, trans helper.Transaction) error {
// 	if err := sendTrans(conn, trans); err != nil {
// 		trans.Status = "failed"
// 		sendTrans(conn, trans)
// 		return err
// 	}
// 	return nil
// }