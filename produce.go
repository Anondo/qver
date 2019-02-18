package qver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type JobReq struct {
	Task string        `json:"task"`
	Args []interface{} `json:"args"`
}

func (s *Server) Publish(sgntr Signature) error {
	jr := JobReq{Task: sgntr.Name}

	for _, arg := range sgntr.Args {
		jr.Args = append(jr.Args, arg.Value)
	}

	byteRep, err := json.Marshal(jr)

	if err != nil {
		return err
	}

	uri := "http://" + s.Host + ":" + strconv.Itoa(s.Port) + "/api/v1/goqueue/"
	req, erR := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(byteRep))
	if erR != nil {
		return erR
	}

	client := http.Client{}
	ctx, cancel := context.WithTimeout(context.Background(), s.TimeOut*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	_, err = client.Do(req)
	if err != nil {
		return err
	}

	return nil

}
