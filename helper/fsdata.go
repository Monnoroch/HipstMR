package helper

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type TagNumPair struct {
	Tag string `json:"tag"`
	Num uint64  `json:"num"`
}

type TagsSet map[string][]uint64

func (self TagsSet) MarshalJSON() ([]byte, error) {
	arr := make([]TagNumPair, 0)
	for k, lst := range self {
		for _, n := range lst {
			arr = append(arr, TagNumPair{
				Tag: k,
				Num: n,
			})
		}
	}
	return json.Marshal(arr)
}

func (self *TagsSet) UnmarshalJSON(data []byte) error {
	*self = make(TagsSet)
	arr := make([]TagNumPair, 0)
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}

	for _, v := range arr {
		(*self)[v.Tag] = append((*self)[v.Tag], v.Num)
	}
	return nil
}

type ChunkData struct {
	Size uint64  `json:"size"`
	Tags TagsSet `json:"tags"`
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

	self.Chunks = make(map[string]*ChunkData)
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
			_, ok := self.Chunks[k]
			if !ok {
				self.Chunks[k] = v
			} else {
				data := self.Chunks[k]
				if data.Size != v.Size {
					return errors.New("Sizes don't match.")
				}

				for tag, pair := range v.Tags {
					data.Tags[tag] = pair
				}
				self.Chunks[k] = data
			}
		}
	}
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
