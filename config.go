package qver

import (
	"time"
)

// Config holds the configuration of the server
type Config struct {
	Host           string
	Port           int
	TimeOut        time.Duration
	QName          string
	ResultsBackend *BackendResult
}

// Configure returns a new Config struct populated with the values provided
func Configure(host string, port int, timeOut time.Duration, bckend *BackendResult) Config {
	cnfg := Config{}
	cnfg.Host = host
	cnfg.Port = port
	cnfg.TimeOut = timeOut
	cnfg.ResultsBackend = bckend
	return cnfg
}
