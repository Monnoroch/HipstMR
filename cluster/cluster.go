package main

import(
	"flag"
	"fmt"
	"path"
	"io/ioutil"
	"encoding/json"
	"HipstMR/fileserver"
	"HipstMR/master"
	"bitbucket.org/kardianos/osext"
)

type machine struct {
	addr string
	fileservers []fileserver.Server
	masters []master.Master
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
	sig := make(chan struct{})
	count := 0
	for _, m := range self.machines {
		for _, fs := range m.fileservers {
			v := fs
			v.Go(sig)
			count++
		}
		for _, mas := range m.masters {
			v := mas
			v.Go(sig)
			count++
		}
	}
	for _ = range sig {
		count--
		if count == 0 {
			break
		}
	}
}

func (self *Cluster) RunForever() {
	for _, m := range self.machines {
		for _, fs := range m.fileservers {
			v := fs
			v.GoForever()
		}
		for _, mas := range m.masters {
			v := mas
			v.GoForever()
		}
	}
	select{}
}

func (self *Cluster) RunMultiProc(binaryPath string) {
	binaryPath = path.Clean(binaryPath)
	sig := make(chan struct{})
	count := 0
	for _, m := range self.machines {
		for _, fs := range m.fileservers {
			v := fs
			v.GoProcessDebug(binaryPath, sig)
			count++
		}
	}
	for _ = range sig {
		count--
		if count == 0 {
			break
		}
	}
}

func (self *Cluster) RunMultiProcForever(binaryPath string) {
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
			masters: make([]master.Master, len(m.Masters)),
		}

		for j, f := range m.Fileservers {
			mn.fileservers[j] = fileserver.NewServer(":" + f.Port, f.Mnt)
		}

		for j, f := range m.Masters {
			mn.masters[j] = master.NewMaster(":" + f.Port)
		}

		res.machines[i] = mn
	}

	return res, nil
}



type fileserverCfg struct {
	Port string `json:"port"`
	Mnt string `json:"mnt"`
}

type masterCfg struct {
	Port string `json:"port"`
}

type machineCfg struct {
	Addr string `json:"address"`
	Fileservers []fileserverCfg `json:"fileservers"`
	Masters []masterCfg `json:"masters"`
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
	forever := flag.Bool("forever", false, "restart failed jobs")
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

		if *forever {
			cluster.RunMultiProcForever(path.Join(p, *fsbinary))
		} else {
			cluster.RunMultiProc(path.Join(p, *fsbinary))
		}
	} else {
		if *forever {
			cluster.RunForever()
		} else {
			cluster.Run()
		}
	}
}

