package hipstmr

import (
	"os"
	"fmt"
	"strings"
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"encoding/json"
	"encoding/binary"
)


type JobConfig struct {
	Jtype string `json:"type"`
	Name string `json:"name"`
	Chunks []string `json:"chunks"`
	OutputTables []string `json:"output_tables"`
	Object []byte `json:"object"`
}

func parseConfig() (JobConfig, error) {
	buf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return JobConfig{}, err
	}

	var cfg JobConfig
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return JobConfig{}, err
	}

	return cfg, nil
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


type JobOutput struct {
	tables []string
	baseWriters []io.WriteCloser
	writers []*bufio.Writer
	current int
}

func newOutput(tables []string) (*JobOutput, error) {
	baseWriters := make([]io.WriteCloser, len(tables))
	writers := make([]*bufio.Writer, len(tables))
	for i, v := range tables {
		f, err := os.Create(fmt.Sprintf("data/%s.chunk.%d", v, i))
		if err != nil {
			return nil, err
		}
		writers[i] = bufio.NewWriter(f)
	}

	return &JobOutput{
		tables: tables,
		baseWriters: baseWriters,
		writers: writers,
		current: 0,
	}, nil
}


func (self *JobOutput) Close() error {
	var res error = nil
	for i, v := range self.writers {
		err := v.Flush()
		if err != nil && res == nil {
			res = err
		}

		err = self.baseWriters[i].Close()
		if err != nil && res == nil {
			res = err
		}
	}
	return res
}


func (self *JobOutput) SetCurrent(v int) error {
	if v < 0 || v >= len(self.tables) {
		return errors.New(fmt.Sprintf("Wrong table #%d", v))
	}

	self.current = v
	return nil
}

func (self *JobOutput) Add(key, subKey, value []byte) error {
	fmt.Println(string(key), string(subKey), string(value))
	err := writeValue(self.writers[self.current], key)
	if err != nil {
		return err
	}

	err = writeValue(self.writers[self.current], subKey)
	if err != nil {
		return err
	}

	err = writeValue(self.writers[self.current], value)
	if err != nil {
		return err
	}
	return nil
}

func (self *JobOutput) AddStr(key, subKey, value string) error {
	return self.Add([]byte(key), []byte(subKey), []byte(value))
}

func runMap(cfg JobConfig) {
	job, err := CreateMap(cfg)
	if err != nil {
		fmt.Println(err)
	}

	var baseReaders []io.ReadCloser
	var readers []io.Reader
	for _, c := range cfg.Chunks {
		f, err := os.Open("data/" + c)
		if err != nil {
			fmt.Println(err)
		}

		baseReaders = append(baseReaders, f)
		readers = append(readers, bufio.NewReader(f))
	}
	reader := io.MultiReader(readers...)
	defer func() {
		for _, v := range baseReaders {
			v.Close()
		}
	}()

	output, err := newOutput(cfg.OutputTables)
	if err != nil {
		fmt.Println(err)
	}
	defer output.Close()

	job.Start()

	for ;; {
		key, err := readValue(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}

		subKey, err := readValue(reader)
		if err != nil {
			fmt.Println(err)
		}

		value, err := readValue(reader)
		if err != nil {
			fmt.Println(err)
		}

		job.Do(key, subKey, value, output)
	}

	job.Finish()
}

func Init() {
	if os.Args[1] != "-hipstmrjob" {
		return
	}
	defer os.Exit(0)

	cfg, err := parseConfig()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Run job " + cfg.Jtype + ", " + cfg.Name + " on chunks {" + strings.Join(cfg.Chunks, ", ") + "}")

	if cfg.Jtype == "map" {
		runMap(cfg)
	}
}
