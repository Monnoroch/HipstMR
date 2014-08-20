package hipstmr

import "fmt"

type Register struct {}

func (self *Register) RunJob(jtype, name string) {
	fmt.Println("RunJob(" + jtype + ", " + name + ")")
}

var defaultRegister Register

func RunJob(jtype, name string) {
	defaultRegister.RunJob(jtype, name)
}

