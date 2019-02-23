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
)

type Worker struct {
	Name        string
	Concurrency int
	Port        int
	Srvr        *Server
}

type JobResponse struct {
	ID      int         `json:"id"`
	JobName string      `json:"job_name"`
	Args    []Arguments `json:"args"`
}

func (w *Worker) Fetch() error {

	go w.StartWorkerServer()

	time.Sleep(1 * time.Second)

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

				jr := JobResponse{}

				json.NewDecoder(resp.Body).Decode(&jr)

				task := w.Srvr.GetTaskByName(jr.JobName)
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
					if err := w.TriggerJob(jr.JobName, wn, task, args...); err != nil {
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

func (s *Server) NewWorker(name string, concurrency, port int) *Worker {
	return &Worker{
		Name:        name,
		Concurrency: concurrency,
		Port:        port,
		Srvr:        s,
	}
}

type RegTaskReq struct {
	TaskNames []string `json:"task_names"`
	QName     string   `json:"qname"`
}

func (s *Server) RegisterTasks(namedTasks map[string]interface{}) error {

	s.RegisteredTasks = namedTasks

	tns := []string{}
	for k, _ := range namedTasks {
		tns = append(tns, k)
	}

	rtr := RegTaskReq{
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	c := http.Client{}
	_, err = c.Do(req)

	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) TriggerJob(jn, wn string, f interface{}, args ...interface{}) error {

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
	log.Printf("Job Triggered: {by:%s , job:%s , arguments:%v}\n", wn, jn, args)
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

	return nil
}

type SubsReq struct {
	Name  string `json:"name"`
	Port  int    `json:"port"`
	QName string `json:"qname"`
}

func (w *Worker) Subscribe() error {

	s := SubsReq{
		Name:  w.Name,
		Port:  w.Port,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	req = req.WithContext(ctx)

	c := http.Client{}
	_, err = c.Do(req)

	return err
}

type Acknowledgement struct {
	Ack bool `json:"ack"`
}

func (w *Worker) StartWorkerServer() error {
	w.Subscribe()

	fmt.Println("Workers spawning...")
	http.HandleFunc("/worker/acknowledge", w.AcknowledgeHandler)
	if err := http.ListenAndServe(":"+strconv.Itoa(w.Port), nil); err != nil {
		return err
	}

	return nil
}

func (w *Worker) AcknowledgeHandler(rw http.ResponseWriter, r *http.Request) {

	if r.Method == http.MethodGet {
		a := Acknowledgement{true}
		b, err := json.Marshal(a)

		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(rw, "%s", string(b))
	}

}
