package utils

import (
	"io/ioutil"
	"encoding/json"
)

type FileserverCfg struct {
	Port string `json:"port"`
	Mnt string `json:"mnt"`
}

type FilesystemSlaveCfg struct {
	Port string `json:"port"`
	Mnt string `json:"mnt"`
}

type MasterCfg struct {
	Port string `json:"port"`
}

type BinariesCfg struct {
	Fileserver string `json:"fileserver"`
	FilesystemSlave string `json:"filesystem_slave"`
	Master string `json:"master"`
}

type MachineCfg struct {
	Addr string `json:"address"`
	Binaries BinariesCfg `json:"binaries"`
	Fileservers []FileserverCfg `json:"fileservers"`
	FilesystemSlaves []FilesystemSlaveCfg `json:"filesystem_slaves"`
	Masters []MasterCfg `json:"masters"`
}

type Config struct {
	Name string
	Data []MachineCfg
}

func (self *Config) GetMachineCfg(name string) *MachineCfg {
	var nodeCfg *MachineCfg = nil
	for _, v := range self.Data {
		if v.Addr == name {
			nodeCfg = &v
			break
		}
	}
	return nodeCfg
}

func NewConfig(file string) (Config, error) {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		return Config{}, err
	}

	var cfg []MachineCfg
	if err := json.Unmarshal(bs, &cfg); err != nil {
		return Config{}, err
	}

	return Config{
		Name: file,
		Data: cfg,
	}, nil
}
