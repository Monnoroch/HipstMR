package hipstmr

import (
	"os"
	"flag"
)

func Init() {
	if os.Args[1] != "-hipstmrjob" {
		return
	}
	defer os.Exit(0)

	fs := flag.NewFlagSet("job", flag.PanicOnError)
	name := fs.String("name", "", "handler name")
	jtype := fs.String("type", "", "job type")
	_ = fs.Bool("hipstmrjob", true, "")
	fs.Parse(os.Args[1:])

	if *jtype == "" || *name == "" {
		return
	}

	RunJob(*jtype, *name)
}
