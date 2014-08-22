package hipstmr

import (
	"encoding/json"
	"errors"
	"reflect"
)

func Register(job Job) error {
	initDefaultRegister()
	return defaultRegister.add(job)
}

type register struct {
	maps     map[string]Map
	reducews map[string]Reduce
}

func (self *register) add(job Job) error {
	v, ok := job.(Map)
	if ok {
		self.maps[job.Name()] = v
		return nil
	}

	return errors.New("Unknown type of job!")
}

func (self *register) createMap(cfg jobConfig) (Map, error) {
	val, ok := self.maps[cfg.Name]
	if !ok {
		return nil, errors.New("Unknown type of map: " + cfg.Name + "!")
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
			maps:     map[string]Map{},
			reducews: map[string]Reduce{},
		}
	}
}

func createMap(cfg jobConfig) (Map, error) {
	initDefaultRegister()
	return defaultRegister.createMap(cfg)
}
