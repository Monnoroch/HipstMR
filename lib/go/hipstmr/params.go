package hipstmr

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Params struct {
	InputTables  []string          `json:"input_tables"`
	OutputTables []string          `json:"output_tables"`
	Files        map[string][]byte `json:"files"`
	Type         string            `json:"type"`
	Name         string            `json:"name"`
	Object       []byte            `json:"job"`
}

func (self *Params) MarshalJSON() ([]byte, error) {
	obj := make(map[string]interface{})
	obj["name"] = self.Name
	obj["type"] = self.Type
	obj["input_tables"] = self.InputTables
	obj["output_tables"] = self.OutputTables
	obj["job"] = self.Object
	files := make(map[string][]byte)
	for k, v := range self.Files {
		isSelf := false
		if k[0] == '!' {
			k = k[1:]
			isSelf = true
		}
		path := filepath.Clean(k)
		key := filepath.Base(path)
		if isSelf {
			key = "!" + key
		}

		if v != nil {
			files[key] = v
			continue
		}

		f, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		files[key] = f
	}
	obj["files"] = files
	return json.Marshal(obj)
}

func (self *Params) SetBinary() *Params {
	self.Files["!"+os.Args[0]] = nil
	return self
}

func (self *Params) AddFile(name string) *Params {
	self.Files[name] = nil
	return self
}

func (self *Params) AddInput(name string) *Params {
	self.InputTables = append(self.InputTables, name)
	return self
}

func (self *Params) AddOutput(name string) *Params {
	self.OutputTables = append(self.OutputTables, name)
	return self
}

func NewParams() *Params {
	return (&Params{
		InputTables:  []string{},
		OutputTables: []string{},
		Files:        map[string][]byte{},
	}).SetBinary()
}

func NewParamsIO(in, out string) *Params {
	return (&Params{
		InputTables:  []string{in},
		OutputTables: []string{out},
		Files:        map[string][]byte{},
	}).SetBinary()
}
