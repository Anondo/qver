package qver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

// jobReq is the request to the goqueue server for publishing a job
type jobReq struct {
	Task  string      `json:"task"`
	Args  []Arguments `json:"args"`
	QName string      `json:"qname"`
}

// Publish publishes a job signature to the goqueue server
func (s *Server) Publish(sgntr Signature) error {
	jr := jobReq{
		Task:  sgntr.Name,
		Args:  sgntr.Args,
		QName: s.QName,
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
	ctx, cancel := context.WithTimeout(context.Background(), s.TimeOut)
	defer cancel()
	req = req.WithContext(ctx)
	_, err = client.Do(req)
	if err != nil {
		return err
	}

	return nil

}
