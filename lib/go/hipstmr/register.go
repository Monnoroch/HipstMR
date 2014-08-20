package hipstmr

import (
	"os"
	"errors"
	"reflect"
	"io/ioutil"
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

	v, ok = job.(Reduce)
	if ok {
		self.reducews[job.Name()] = v
		return nil
	}

	return errors.New("Unknown type of job!")
}

func (self *register) CreateJob(name string) (Job, error) {
	buf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return nil, err
	}

	valM, okM := self.maps[name]
	valR, okR := self.reducews[name]

	if !okM && !okR {
		return nil, errors.New("Unknown type of job!")
	}

	val := valM
	if !okM {
		val = valR
	}

	t := reflect.TypeOf(val).Elem()
	v := reflect.New(t)
	vi := v.Interface()

	if err := json.Unmarshal(buf, vi); err != nil {
		return nil, err
	}
	return vi.(Job), nil
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

func CreateJob(name string) (Job, error) {
	initDefaultRegister()
	return defaultRegister.CreateJob(name)
}
