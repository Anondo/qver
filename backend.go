package qver

import (
	"fmt"
	"time"
)

// Available backend systems
const (
	Redis = "redis"
)

// BackendResult holds all the informations related to the system to store the back end results
type BackendResult struct {
	Backend         string
	Host            string
	Port            int
	Path            string
	Username        string
	Password        string
	DB              string
	ResultsExpireIn time.Duration
}

func (b *BackendResult) connect() error {
	if b.Backend == Redis {
		if err := connectRedis(b); err != nil {
			return err
		}
	}

	if b.Backend != Redis {
		return fmt.Errorf("qver doesn't support the backend result %s", b.Backend)
	}

	return nil
}

func (b *BackendResult) store(jr jobResponse, r []interface{}) error {
	if b.Backend == Redis {
		if err := setKV(jr, r, b.ResultsExpireIn); err != nil {
			return err
		}
	}

	return nil
}
