package hipstmr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func Init() {
	if !(len(os.Args) == 2 && os.Args[1] == "-hipstmrjob") {
		return
	}
	defer os.Exit(0)

	cfg, err := parseConfig()
	if err != nil {
		panic(err)
	}

	fmt.Fprintln(os.Stderr, "Run job "+cfg.Jtype+", "+cfg.Name+" on chunks {"+strings.Join(cfg.Chunks, ", ")+"}")

	if cfg.Jtype == "map" {
		runMap(cfg)
	}
}

type jobConfig struct {
	Mnt  string `json:"mnt"`
	Jtype        string   `json:"type"`
	Name         string   `json:"name"`
	Dir          string   `json:"dir"`
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

func runMap(cfg jobConfig) {
	fmt.Println("runMap", cfg)

	job, err := createMap(cfg)
	if err != nil {
		panic(err)
	}

	var baseReaders []io.ReadCloser
	var readers []io.Reader
	for _, c := range cfg.Chunks {
		f, err := os.Open(path.Join(cfg.Mnt, c+".chunk"))
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

	output, err := newOutput(cfg, cfg.Mnt)
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
