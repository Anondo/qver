package qver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

type QueueReq struct {
	Name    string `json:"name"`
	Cap     int    `json:"cap"`
	Durable bool   `json:"durable"`
}

func (s *Server) DeclareQueue(n string, c int, d bool) error {
	qr := QueueReq{
		Name:    n,
		Cap:     c,
		Durable: d,
	}

	byteRep, err := json.Marshal(qr)

	if err != nil {
		return err
	}

	s.QName = qr.Name

	uri := "http://" + s.Host + ":" + strconv.Itoa(s.Port) + "/api/v1/goqueue/queue"
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
		return errors.New("\033[31m" + "GoQueue server not working" + "\033[0m")
	}

	return nil
}
