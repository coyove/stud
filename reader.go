package stud

import (
	"io"
	"io/ioutil"
	"time"
)

type Reader struct {
	Key        string
	CreateTime time.Time
	r          io.Reader
}

func (r *Reader) ReadAll() ([]byte, error) {
	return ioutil.ReadAll(r)
}

func (r *Reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}
