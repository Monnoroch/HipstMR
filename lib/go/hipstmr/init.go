package hipstmr

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func Init() {
	if os.Args[1] != "-hipstmrjob" {
		return
	}
	defer os.Exit(0)

	fs := flag.NewFlagSet("hipstmrjob", flag.PanicOnError)
	hipstmrjob := fs.Bool("hipstmrjob", false, "run as a MR job")
	mnt := fs.String("mnt", "", "mount point")
	fs.Parse(os.Args[1:])

	if !*hipstmrjob || *mnt == "" {
		fs.PrintDefaults()
		return
	}

	cfg, err := parseConfig()
	if err != nil {
		panic(err)
	}

	fmt.Fprintln(os.Stderr, "Run job " + cfg.Jtype + ", " + cfg.Name + " on chunks {" + strings.Join(cfg.Chunks, ", ") + "}")

	if cfg.Jtype == "map" {
		runMap(cfg, *mnt)
	}
}

type jobConfig struct {
	Jtype        string   `json:"type"`
	Name         string   `json:"name"`
	Chunks       []string `json:"chunks"`
	OutputTables []string `json:"output_tables"`
	Object       []byte   `json:"object"`
}

func parseConfig() (jobConfig, error) {
	buf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return jobConfig{}, err
	}

	var cfg jobConfig
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return jobConfig{}, err
	}

	return cfg, nil
}

func runMap(cfg jobConfig, mnt string) {
	job, err := createMap(cfg)
	if err != nil {
		panic(err)
	}

	var baseReaders []io.ReadCloser
	var readers []io.Reader
	for _, c := range cfg.Chunks {
		f, err := os.Open(mnt + c)
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

	output, err := newOutput(cfg.OutputTables, mnt)
	if err != nil {
		fmt.Println(err)
	}
	defer output.close()

	job.Start()

	for {
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
