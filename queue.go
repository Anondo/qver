package qver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type QueueReq struct {
	Name string `json:"name"`
	Cap  int    `json:"cap"`
}

func (s *Server) DeclareQueue(n string, c int) error {
	qr := QueueReq{
		Name: n,
		Cap:  c,
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
		return err
	}

	return nil
}
