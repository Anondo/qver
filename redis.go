package qver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Anondo/redis"
)

var (
	redisClient *redis.Client
)

func connectRedis(b *BackendResult) error {
	db, err := strconv.Atoi(b.DB)
	if err != nil {
		return err
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", b.Host, b.Port),
		Password: b.Password,
		DB:       db,
	})

	_, erR := redisClient.Ping().Result()

	if erR != nil {
		return erR
	}

	return nil
}

func setKV(jr jobResponse, r []interface{}, rexp time.Duration) error {
	tn := fmt.Sprintf("%s_%d", jr.JobName, jr.ID)

	result := struct {
		Args   []Arguments
		Result []interface{}
	}{
		Args:   jr.Args,
		Result: r,
	}

	rbyte, err := json.Marshal(result)

	if err != nil {
		return err
	}

	res := string(rbyte)

	if err := redisClient.Set(tn, res, rexp).Err(); err != nil {
		return err
	}

	return nil
}
