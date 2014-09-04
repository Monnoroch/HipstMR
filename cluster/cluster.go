package main

import(
	"flag"
	"fmt"
	"path"
	"io/ioutil"
	"encoding/json"
	"HipstMR/fileserver"
	"bitbucket.org/kardianos/osext"
)

type machine struct {
	addr string
	fileservers []fileserver.Server
}

type Cluster struct {
	machines []machine
	cfg config
}

func runFsRec(fs *fileserver.Server) {
	if err := fs.Run(); err != nil {
		fmt.Println("Error:", err)
	}
	go runFsRec(fs)
}

func (self *Cluster) Run() {
	for _, m := range self.machines {
		for _, fs := range m.fileservers {
			v := fs
			v.GoForever()
		}
	}
	select{}
}

func (self *Cluster) RunMultiProc(binaryPath string) {
	binaryPath = path.Clean(binaryPath)
	for _, m := range self.machines {
		for _, fs := range m.fileservers {
			v := fs
			v.GoProcessDebugForever(binaryPath)
		}
	}
	select{}
}

func NewCluster(file string) (Cluster, error) {
	cfg, err := newConfig(file)
	if err != nil {
		return Cluster{}, err
	}

	res := Cluster{
		cfg: cfg,
		machines: make([]machine, len(cfg)),
	}
	for i, m := range cfg {
		mn := machine{
			addr: m.Addr,
			fileservers: make([]fileserver.Server, len(m.Fileservers)),
		}

		for j, f := range m.Fileservers {
			mn.fileservers[j] = fileserver.NewServer(":" + f.Port, f.Mnt)
		}

		res.machines[i] = mn
	}

	return res, nil
}



type fileserverCfg struct {
	Port string `json:"port"`
	Mnt string `json:"mnt"`
}


type machineCfg struct {
	Addr string `json:"address"`
	Fileservers []fileserverCfg `json:"fileservers"`
}

type config []machineCfg

func newConfig(file string) (config, error) {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		return config{}, err
	}

	var cfg config
	if err := json.Unmarshal(bs, &cfg); err != nil {
		return config{}, err
	}

	return cfg, nil
}


func main() {
	help := flag.Bool("help", false, "print this help")
	cfgFile := flag.String("config", "", "config file path")
	multiprocess := flag.Bool("multiprocess", false, "run in multiple processes (need to specify fsbinary)")
	fsbinary := flag.String("fsbinary", "", "fileserver binary for ")
	flag.Parse()
	if *help || *cfgFile == "" || (*multiprocess && *fsbinary == "") {
		flag.PrintDefaults()
		return
	}

	cluster, err := NewCluster(*cfgFile)
	if err != nil {
		panic(err)
	}

	fmt.Println(cluster)

	if *multiprocess {
		p, err := osext.ExecutableFolder()
		if err != nil {
			panic(err)
		}

		cluster.RunMultiProc(path.Join(p, *fsbinary))
	} else {
		cluster.Run()
	}
}

