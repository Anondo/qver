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
	ID      int           `json:"id"`
	JobName string        `json:"job_name"`
	Args    []interface{} `json:"args"`
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

	// wg := sync.WaitGroup{}
	// wg.Add(w.Concurrency)

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
					wn := w.Name + ":" + strconv.Itoa(worker)
					if err := w.TriggerJob(jr.JobName, wn, task, jr.Args...); err != nil {
						log.Fatal("\033[31m", err, "\033[0m")
					}
				} else {
					log.Println("\033[31m", "Unregistererd Task", "\033[0m")
				}

			}
			// wg.Done()
		}(i + 1)

	}

	fmt.Println("\033[35m")
	log.Printf("%d worker(s) are waiting for the tasks\n", w.Concurrency)
	log.Println("Broker: GoQueue")
	log.Printf("Broker URI: http://%s:%d\n", w.Srvr.Host, w.Srvr.Port)
	log.Println("Queue:", w.Srvr.QName)
	log.Println("Press Ctrl+C to exit")
	fmt.Println("\033[0m")

	// wg.Wait()
	<-stop

	log.Println("\033[0m", "Worker(s) died gracefully!!", "\033[0m")

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

func (s *Server) RegisterTasks(namedTasks map[string]interface{}) error {

	s.RegisteredTasks = namedTasks

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
			return fmt.Errorf("Argument Type Mismatch: Expected %v, Got %v", rt.In(i), reflect.ValueOf(arg))
		}
		arguments = append(arguments, reflect.ValueOf(arg))
	}

	fmt.Println("\033[35m")
	log.Printf("Job Triggered: {by:%s , job:%s , arguments:%v}", wn, jn, args)
	fmt.Println("\033[0m")

	results := rv.Call(arguments)

	log.Printf("Results:%v", results)

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
