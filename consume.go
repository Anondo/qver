package qver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Worker consumes tasks
type Worker struct {
	id          string
	Name        string
	Concurrency int
	Srvr        *Server
}

// jobResponse is the response sent by the goqueue server
type jobResponse struct {
	ID      int         `json:"id"`
	JobName string      `json:"job_name"`
	Args    []Arguments `json:"args"`
}

// Fetch prepares the workers for fetching tasks. It starts the worker server, spawns them, and makes them
//request for tasks to the goqueue server
func (w *Worker) Fetch() error {

	w.generateUUID() // populate the id field of the worker by generating a uuid

	if w.Srvr.ResultsBackend != nil {
		if err := w.Srvr.ResultsBackend.connect(); err != nil {
			return err
		}
	}

	if err := w.subscribe(); err != nil {
		return err
	}

	fmt.Println("Workers spawning...")

	time.Sleep(1 * time.Second) // TODO: not sure if this is needed

	uri := "http://" + w.Srvr.Host + ":" + strconv.Itoa(w.Srvr.Port) + "/api/v1/goqueue/" + "queue/" + w.Srvr.QName +
		"?sname=" + w.Name
	req, err := http.NewRequest(http.MethodGet, uri, nil)

	if err != nil {
		return err
	}

	c := http.Client{}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGKILL, syscall.SIGINT, syscall.SIGQUIT)

	for i := 0; i < w.Concurrency; i++ {
		go func(worker int) {
			for {
				resp, err := c.Do(req)

				if err != nil {
					log.Fatal("\033[31m", "GOQueue server has stopped", "\033[0m")
				}

				jr := jobResponse{}

				json.NewDecoder(resp.Body).Decode(&jr)

				if jr.JobName == "" {
					log.Fatalln("\033[31m", "GoQueue server failed to connect with:"+w.Name, "\033[0m")
				}

				task := w.Srvr.getTaskByName(jr.JobName)
				if task != nil {
					args := []interface{}{}
					for _, a := range jr.Args {
						switch a.Type {
						case Int, Int8, Int16, Int32, Int64: //this is done because interface if unmarshaled becomes float64,
							args = append(args, int(a.Value.(float64)))
						case UInt, UInt8, UInt16, UInt32, UInt64:
							args = append(args, int(a.Value.(float64)))
						default:
							args = append(args, a.Value)
						}
					}
					wn := w.Name + ":" + strconv.Itoa(worker)
					if err := w.triggerJob(jr, wn, task, args...); err != nil {
						log.Println("\033[31m", err, "\033[0m")
					}
				} else {
					log.Println("\033[31m", "Unregistererd Task", "\033[0m")
				}

			}
		}(i + 1)

	}

	fmt.Println("\033[35m")
	log.Printf("%d worker(s) are waiting for the tasks\n", w.Concurrency)
	log.Println("Broker: GoQueue")
	log.Printf("Broker URI: http://%s:%d\n", w.Srvr.Host, w.Srvr.Port)
	log.Println("Queue:", w.Srvr.QName)
	log.Println("Press Ctrl+C to exit")
	fmt.Println("\033[0m")

	<-stop

	log.Println("\033[31m", "Worker(s) died gracefully!!", "\033[0m")

	return nil
}

// NewWorker returns a new Worker pointer
func (s *Server) NewWorker(name string, concurrency int) *Worker {
	return &Worker{
		Name:        name,
		Concurrency: concurrency,
		Srvr:        s,
	}
}

// regTaskReq is the request for task registration to the goqueue server
type regTaskReq struct {
	TaskNames []string `json:"task_names"`
	QName     string   `json:"qname"`
}

// RegisterTasks register tasks for the current consumer. Registers it locally and makes
// a request to the goqueue server for registering as well.
func (s *Server) RegisterTasks(namedTasks map[string]interface{}) error {

	s.RegisteredTasks = namedTasks

	tns := []string{}
	for k := range namedTasks {
		tns = append(tns, k)
	}

	rtr := regTaskReq{
		TaskNames: tns,
		QName:     s.QName,
	}

	b, err := json.Marshal(rtr)

	if err != nil {
		return err
	}

	uri := "http://" + s.Host + ":" + strconv.Itoa(s.Port) + "/api/v1/goqueue/task/register"
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(b))

	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.TimeOut)
	defer cancel()
	req = req.WithContext(ctx)

	c := http.Client{}
	_, err = c.Do(req)

	if err != nil {
		return err
	}

	return nil
}

// triggerJob validates a job & triggers it
func (w *Worker) triggerJob(jr jobResponse, wn string, f interface{}, args ...interface{}) error {

	rv := reflect.ValueOf(f)
	rt := reflect.TypeOf(f)
	arguments := []reflect.Value{}

	if rt.NumIn() != len(args) {
		return errors.New("Invalid Number Of Arguments For Job")
	}

	for i, arg := range args {
		if reflect.TypeOf(arg) != rt.In(i) {
			return fmt.Errorf("Argument Type Mismatch: Expected %v, Got %v", rt.In(i), reflect.ValueOf(arg).Kind())
		}
		arguments = append(arguments, reflect.ValueOf(arg))
	}

	fmt.Println("\033[35m")
	log.Printf("Job Triggered: {by:%s , job:%s , arguments:%v}\n", wn, jr.JobName, args)
	log.Print("Processing results...")
	fmt.Println("\033[0m")

	resVal := rv.Call(arguments)
	results := []interface{}{} //just for keeping the results in a slice to display

	for _, v := range resVal {
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			results = append(results, int(v.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			results = append(results, int(v.Int()))
		case reflect.Float32, reflect.Float64:
			results = append(results, v.Float())
		case reflect.Bool:
			results = append(results, v.Bool())
		default:
			results = append(results, v)

		}
	}

	log.Printf("Results:%v\n", results)

	if err := w.sendAck(); err != nil {
		return err
	}

	if w.Srvr.ResultsBackend != nil {
		if err := w.Srvr.ResultsBackend.store(jr, results); err != nil {
			return err
		}
	}

	return nil
}

// susubsReq is the subscribe request for consumer to the goqueue server
type subsReq struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	QName string `json:"qname"`
}

// subscribe subscribes a consumer with the goqueue server
func (w *Worker) subscribe() error {

	s := subsReq{
		ID:    w.id,
		Name:  w.Name,
		QName: w.Srvr.QName,
	}

	b, err := json.Marshal(s)

	if err != nil {
		return err
	}

	uri := "http://" + w.Srvr.Host + ":" + strconv.Itoa(w.Srvr.Port) + "/api/v1/goqueue/subscribe"
	req, erR := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(b))

	if err != nil {
		return erR
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.Srvr.TimeOut)

	defer cancel()

	req = req.WithContext(ctx)

	c := http.Client{}
	_, err = c.Do(req)

	return err
}

// acacknowledgementRequest is the request payload for acknowledgement
type acknowledgementRequest struct {
	Ack        bool   `json:"ack"`
	Qname      string `json:"qname"`
	Subscriber string `json:"subscriber"`
}

// sendAck is the method to send acknowledgement to the goqueue server
func (w *Worker) sendAck() error {
	a := acknowledgementRequest{
		Ack:        true,
		Qname:      w.Srvr.QName,
		Subscriber: w.Name,
	}
	b, err := json.Marshal(a)

	if err != nil {
		return err
	}

	uri := "http://" + w.Srvr.Host + ":" + strconv.Itoa(w.Srvr.Port) + "/api/v1/goqueue/task/acknowledge"
	req, erR := http.NewRequest(http.MethodPost, uri, bytes.NewBuffer(b))

	if err != nil {
		return erR
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.Srvr.TimeOut)

	defer cancel()

	req = req.WithContext(ctx)

	c := http.Client{}
	_, err = c.Do(req)

	return err

}

func (w *Worker) generateUUID() {
	w.id = uuid.Must(uuid.NewV4()).String()
}
