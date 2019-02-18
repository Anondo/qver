package qver

import (
	"time"
)

type Config struct {
	Host    string
	Port    int
	TimeOut time.Duration
	QName   string
}

func Configure(host string, port int, timeOut time.Duration) Config {
	cnfg := Config{}
	cnfg.Host = host
	cnfg.Port = port
	cnfg.TimeOut = timeOut
	return cnfg
}
