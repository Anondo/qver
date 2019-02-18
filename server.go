package qver

type Server struct {
	Config
}

func NewServer(c Config) Server {
	s := Server{c}
	return s
}
