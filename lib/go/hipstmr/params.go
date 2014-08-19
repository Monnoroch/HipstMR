package hipstmr

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"path/filepath"
)


type Params struct {
	InputTables []string  `json:"input_tables"`
	OutputTables []string `json:"output_tables"`
	Files map[string]string `json:"files"`
	Type string `json:"type"`
	Name string `json:"name"`
}

func (self *Params) MarshalJSON() ([]byte, error) {
	obj := make(map[string]interface{})
	obj["name"] = self.Name
	obj["type"] = self.Type
	obj["input_tables"] = self.InputTables
	obj["output_tables"] = self.OutputTables
	files := make(map[string]string)
	for k, _ := range(self.Files) {
		path := filepath.Clean(k)
		f, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		files[filepath.Base(path)] = string(f)
	}
	path := filepath.Clean(os.Args[0])
	f, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	files["!" + filepath.Base(path)] = string(f)

	obj["files"] = files
	return json.Marshal(obj)
}

func NewParams() *Params {
	return &Params{
		InputTables: []string{},
		OutputTables: []string{},
		Files: map[string]string{},
	}
}

func NewParamsIO(in, out string) *Params {
	return &Params{
		InputTables: []string{in},
		OutputTables: []string{out},
		Files: map[string]string{},
	}
}
