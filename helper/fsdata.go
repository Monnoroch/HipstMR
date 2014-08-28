package helper

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type ChunkData struct {
	Num  uint64   `json:"num"`
	Size uint64 `json:"size"`
	Tags []string `json:"tags"`
}

type FsData struct {
	Chunks map[string]*ChunkData `json:"chunks"`
}

func (self *FsData) Read(name string) error {
	self.Chunks = nil

	p := path.Clean(name)
	dir, err := ioutil.ReadDir(p)
	if err != nil {
		return err
	}

	allChunks := make(map[string]*ChunkData)
	for _, v := range dir {
		if v.IsDir() {
			continue
		}

		nm := v.Name()
		if !strings.HasSuffix(nm, ".fsdat") {
			continue
		}

		bs, err := ioutil.ReadFile(path.Join(p, nm))
		if err != nil {
			return err
		}

		var chunks map[string]*ChunkData
		if err := json.Unmarshal(bs, &chunks); err != nil {
			return err
		}

		for k, v := range chunks {
			data, ok := allChunks[k]
			if !ok {
				allChunks[k] = v
			} else {
				data.Tags = append(data.Tags, v.Tags...)
			}
		}
	}
	self.Chunks = allChunks
	return nil
}

func (self *FsData) Write(file string) error {
	bs, err := json.MarshalIndent(self.Chunks, "", "\t")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(file, bs, os.ModePerm)
}

func (self *FsData) ClearFs(name string) {
	p := path.Clean(name)
	dir, _ := ioutil.ReadDir(p)
	for _, v := range dir {
		if v.IsDir() {
			os.RemoveAll(path.Join(p, v.Name()))
			continue
		}

		nm := v.Name()
		if !strings.HasSuffix(nm, ".chunk") {
			os.Remove(path.Join(p, nm))
			continue
		}

		id := nm[:len(nm)-len(".chunk")]
		_, ok := self.Chunks[id]
		if !ok {
			os.Remove(path.Join(p, nm))
		}
	}
}
