package hipstmr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

type JobOutput struct {
	tables       []string
	buffers      []*bytes.Buffer
	counters     []uint
	current      int
	mnt          string
	dir          string
	maxChunkSize int
}

func (self *JobOutput) close() error {
	var res error = nil
	for i, _ := range self.buffers {
		err := self.writeChunk(i)
		if err != nil && res == nil {
			res = err
		}
	}
	return res
}

func (self *JobOutput) writeChunk(cur int) error {
	buf := self.buffers[cur]
	if buf.Len() == 0 {
		return nil
	}

	name := path.Join(self.mnt, self.dir, self.tables[cur], fmt.Sprintf("%d.chunk", self.counters[cur]))
	base := path.Dir(name)
	if err := os.MkdirAll(base, os.ModeDir|os.ModeTemporary|os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(buf.Bytes())
	if err != nil {
		return err
	}
	if n != buf.Len() {
		return errors.New(fmt.Sprintf("Wrote only %d bytes from %d.", n, buf.Len()))
	}

	self.counters[cur]++
	buf.Reset()
	// cmdPrefix := "!hipstmrjob: "
	// fmt.Println(cmdPrefix + self.tables[cur] + " -> " + name)
	return nil
}

func (self *JobOutput) SetCurrent(v int) error {
	if v < 0 || v >= len(self.tables) {
		return errors.New(fmt.Sprintf("Wrong table #%d", v))
	}

	self.current = v
	return nil
}

func (self *JobOutput) Add(key, subKey, value []byte) error {
	cur := self.current
	buf := self.buffers[cur]
	size := buf.Len()
	newSize := size + 2*3 + len(key) + len(subKey) + len(value)
	if newSize > self.maxChunkSize {
		if err := self.writeChunk(cur); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}

	fmt.Println(string(key), string(subKey), string(value))

	err := writeValue(buf, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	err = writeValue(buf, subKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	err = writeValue(buf, value)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return nil
}

func (self *JobOutput) AddStr(key, subKey, value string) error {
	return self.Add([]byte(key), []byte(subKey), []byte(value))
}

func readValue(reader io.Reader) ([]byte, error) {
	bs := []byte{0, 0}
	n, err := reader.Read(bs)
	if err != nil {
		return nil, err
	}
	if n != 2 {
		return nil, errors.New("!!! 11")
	}

	l := binary.LittleEndian.Uint16(bs)
	buf := make([]byte, l, l)
	n, err = reader.Read(buf)
	if n != int(l) {
		fmt.Println(n)
		return nil, errors.New("!!! 12")
	}
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func writeValue(writer io.Writer, value []byte) error {
	arr := []byte{0, 0}
	binary.LittleEndian.PutUint16(arr, uint16(len(value)))
	n, err := writer.Write(arr)
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("!!! 21")
	}

	n, err = writer.Write(value)
	if err != nil {
		return err
	}
	if n != len(value) {
		return errors.New("!!! 22")
	}
	return nil
}

func newOutput(cfg jobConfig, mnt string) (*JobOutput, error) {
	// writers := make([]io.WriteCloser, len(tables))
	buffers := make([]*bytes.Buffer, len(cfg.OutputTables))
	for i, _ := range cfg.OutputTables {
		buffers[i] = &bytes.Buffer{}
	}

	return &JobOutput{
		mnt:          mnt,
		dir:          cfg.Dir,
		tables:       cfg.OutputTables,
		current:      0,
		maxChunkSize: 40,
		buffers:      buffers,
		counters:     make([]uint, len(cfg.OutputTables)), // assume default zero
	}, nil
}
