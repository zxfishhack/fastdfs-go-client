package client

import "time"

type Config struct {
	ConnTimeout time.Duration
}

var clientConfig *Config

const (
	DefaultRWTimeout = 10 * time.Second
)

func init() {
	clientConfig = &Config{
		ConnTimeout: 60 * time.Second,
	}
}

func SetConfig(cfg *Config) {
	clientConfig = cfg
}
