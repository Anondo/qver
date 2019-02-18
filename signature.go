package qver

type Arguments struct {
	Value interface{}
}

type Signature struct {
	Name string
	Args []Arguments
}
