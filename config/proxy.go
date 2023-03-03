package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
)

type TrackerProxyConfig struct {
	Upstream []string `yaml:"-"`
	Listen   string   `yaml:"listen"`
}

type StorageProxyConfig struct {
	Upstream   string `yaml:"upstream"`
	Downstream string `yaml:"downstream"`
}

type ProxyConfig struct {
	Tracker *TrackerProxyConfig   `yaml:"tracker"`
	Storage []*StorageProxyConfig `yaml:"storage"`
}

func NewProxyConfig(fn string) (pc *ProxyConfig, err error) {
	var f *os.File
	f, err = os.Open(fn)
	if err != nil {
		return
	}
	defer f.Close()
	var b []byte
	b, err = ioutil.ReadAll(f)
	if err != nil {
		return
	}
	pc = &ProxyConfig{}
	err = yaml.Unmarshal(b, pc)
	return
}

func (p *ProxyConfig) Write(fn string) (err error) {
	var b []byte
	b, err = yaml.Marshal(p)
	var f *os.File
	f, err = os.OpenFile(fn+".save", os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	_, err = f.Write(b)
	if err != nil {
		f.Close()
		return
	}
	err = f.Sync()
	if err != nil {
		log.Print("sync file failed, ", err)
	}
	f.Close()
	err = os.Rename(fn+".save", fn)
	return
}
