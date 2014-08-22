package hipstmr

import (
	"errors"
	"reflect"
	"encoding/json"
)


type register struct {
	maps map[string]Map
	reducews map[string]Reduce
}

func (self *register) Add(job Job) error {
	v, ok := job.(Map)
	if ok {
		self.maps[job.Name()] = v
		return nil
	}

	return errors.New("Unknown type of job!")
}



func (self *register) CreateMap(cfg JobConfig) (Map, error) {
	val, ok := self.maps[cfg.Name]
	if !ok {
		return nil, errors.New("Unknown type of map!")
	}

	vi := reflect.New(reflect.TypeOf(val).Elem()).Interface()
	if err := json.Unmarshal(cfg.Object, vi); err != nil {
		return nil, err
	}
	return vi.(Map), nil
}

var defaultRegister register

func initDefaultRegister() {
	if defaultRegister.maps == nil {
		defaultRegister = register{
			maps: map[string]Map{},
			reducews: map[string]Reduce{},
		}
	}
}


func Register(job Job) error {
	initDefaultRegister()
	return defaultRegister.Add(job)
}

func CreateMap(cfg JobConfig) (Map, error) {
	initDefaultRegister()
	return defaultRegister.CreateMap(cfg)
}
