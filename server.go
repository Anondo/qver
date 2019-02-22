package qver

type Server struct {
	Config
	RegisteredTasks map[string]interface{}
}

func NewServer(c Config) Server {
	s := Server{c, make(map[string]interface{})}
	return s
}

func (s *Server) GetTaskByName(tn string) interface{} {
	for n, t := range s.RegisteredTasks {
		if n == tn {
			return t
		}
	}
	return nil
}
