package qver

const (
	Int    = "int"
	Int8   = "int8"
	Int16  = "int16"
	Int32  = "int32"
	Int64  = "int64"
	UInt   = "uint"
	UInt8  = "uint8"
	UInt16 = "uint16"
	UInt32 = "uint32"
	UInt64 = "uint64"
	Float  = "float64"
	String = "string"
)

// Arugments is the argument for the task to be published
type Arguments struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// Signature holds the task details like name & arguments for that task
type Signature struct {
	Name string
	Args []Arguments
}
