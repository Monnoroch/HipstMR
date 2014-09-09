package main

import(
	"flag"
	"fmt"
	"path"
	"errors"
	"HipstMR/fileserver"
	"HipstMR/filesystem"
	"HipstMR/master"
	"HipstMR/utils"
	"bitbucket.org/kardianos/osext"
)

type ClusterNode struct {
	addr string
	path string
	fileservers []fileserver.Server
	fsSlaves []filesystem.Slave
	masters []master.Master
	cfg utils.Config
	nodeCfg *utils.MachineCfg
}

func (self *ClusterNode) Run() {
	sig := make(chan struct{})
	count := 0
	for _, fs := range self.fileservers {
		v := fs
		utils.Go(&v, sig)
		count++
	}
	for _, fs := range self.fsSlaves {
		v := fs
		utils.Go(&v, sig)
		count++
	}
	for _, mas := range self.masters {
		v := mas
		utils.Go(&v, sig)
		count++
	}
	for _ = range sig {
		count--
		if count == 0 {
			break
		}
	}
}

func (self *ClusterNode) RunForever() {
	for _, fs := range self.fileservers {
		v := fs
		utils.GoForever(&v)
	}
	for _, fs := range self.fsSlaves {
		v := fs
		utils.GoForever(&v)
	}
	for _, mas := range self.masters {
		v := mas
		utils.GoForever(&v)
	}
}

func (self *ClusterNode) RunMultiProc() {
	binaryFsPath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.Fileserver))
	binaryFsSlavePath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.FilesystemSlave))
	binaryMasterPath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.Master))
	sig := make(chan struct{})
	count := 0
	for _, fs := range self.fileservers {
		v := fs
		utils.GoProcessDebug(&v, binaryFsPath, sig)
		count++
	}
	for _, fs := range self.fsSlaves {
		v := fs
		utils.GoProcessDebug(&v, binaryFsSlavePath, sig)
		count++
	}
	for _, mas := range self.masters {
		v := mas
		utils.GoProcessDebug(&v, binaryMasterPath, sig)
		count++
	}
	for _ = range sig {
		count--
		if count == 0 {
			break
		}
	}
}

func (self *ClusterNode) RunMultiProcForever() {
	binaryFsPath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.Fileserver))
	binaryFsSlavePath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.FilesystemSlave))
	binaryMasterPath := path.Clean(path.Join(self.path, self.nodeCfg.Binaries.Master))
	for _, fs := range self.fileservers {
		v := fs
		utils.GoProcessDebugForever(&v, binaryFsPath)
	}
	for _, fs := range self.fsSlaves {
		v := fs
		utils.GoProcessDebugForever(&v, binaryFsSlavePath)
	}
	for _, mas := range self.masters {
		v := mas
		utils.GoProcessDebugForever(&v, binaryMasterPath)
	}
}

func NewClusterNode(file, name string) (ClusterNode, error) {
	binPath, err := osext.ExecutableFolder()
	if err != nil {
		return ClusterNode{}, err
	}

	cfg, err := utils.NewConfig(file)
	if err != nil {
		return ClusterNode{}, err
	}

	nodeCfg := cfg.GetMachineCfg(name)
	if nodeCfg == nil {
		return ClusterNode{}, errors.New("No machine with name \"" + name + "\".")
	}

	cfgFullPath := path.Clean(path.Join(binPath, file))

	res := ClusterNode{
		addr: nodeCfg.Addr,
		path: binPath,
		cfg: cfg,
		nodeCfg: nodeCfg,
		fileservers: make([]fileserver.Server, len(nodeCfg.Fileservers)),
		masters: make([]master.Master, len(nodeCfg.Masters)),
		fsSlaves: make([]filesystem.Slave, len(nodeCfg.FilesystemSlaves)),
	}

	for j, f := range nodeCfg.Fileservers {
		res.fileservers[j] = fileserver.NewServer(":" + f.Port, f.Mnt)
	}

	for j, f := range nodeCfg.Masters {
		res.masters[j] = master.NewMaster(":" + f.Port, cfgFullPath, res.cfg)
	}

	for j, f := range nodeCfg.FilesystemSlaves {
		res.fsSlaves[j] = filesystem.NewSlave(":" + f.Port, f.Mnt, nodeCfg.Addr, cfgFullPath, *nodeCfg)
	}

	return res, nil
}

func main() {
	help := flag.Bool("help", false, "print this help")
	cfgFile := flag.String("config", "", "config file path")
	machine := flag.String("machine", "", "current machine name")
	multiprocess := flag.Bool("multiprocess", false, "run in multiple processes (need to specify fsbinary)")
	forever := flag.Bool("forever", false, "restart failed jobs")
	flag.Parse()
	if *help || *cfgFile == "" || *machine == "" {
		flag.PrintDefaults()
		return
	}

	clusterNode, err := NewClusterNode(*cfgFile, *machine)
	if err != nil {
		panic(err)
	}

	fmt.Println(clusterNode.cfg)

	if *multiprocess {
		if *forever {
			clusterNode.RunMultiProcForever()
		} else {
			clusterNode.RunMultiProc()
		}
	} else {
		if *forever {
			clusterNode.RunForever()
		} else {
			clusterNode.Run()
		}
	}

	select{}
}

