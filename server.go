package qver

// Server is the goqueue-driver that does all the work(publishing task , preparing workers etc)
type Server struct {
	Config
	RegisteredTasks map[string]interface{}
}

// NewServer returns a Server struct with the configuration provided
func NewServer(c Config) Server {
	s := Server{c, make(map[string]interface{})}
	return s
}

// getTaskByName returns a task matched by the name from the registered tasks
func (s *Server) getTaskByName(tn string) interface{} {
	for n, t := range s.RegisteredTasks {
		if n == tn {
			return t
		}
	}
	return nil
}
