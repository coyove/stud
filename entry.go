package stud

import (
	"encoding/binary"
	"fmt"
	"io"
)

type entry struct {
	dib  byte
	hash uint32
	k    uint64
	v    uint64
}

func (e entry) _marshal(w io.Writer) {
	panicerr(binary.Write(w, binary.BigEndian, e.dib))
	panicerr(binary.Write(w, binary.BigEndian, e.hash))
	panicerr(binary.Write(w, binary.BigEndian, e.k))
	panicerr(binary.Write(w, binary.BigEndian, e.v))
}

func _unmarshalEntry(r io.Reader) entry {
	e := entry{}
	panicerr(binary.Read(r, binary.BigEndian, &e.dib))
	panicerr(binary.Read(r, binary.BigEndian, &e.hash))
	panicerr(binary.Read(r, binary.BigEndian, &e.k))
	panicerr(binary.Read(r, binary.BigEndian, &e.v))
	return e
}

func (e entry) String() string {
	if e.k == 0 {
		return "null"
	}
	return fmt.Sprintf("%x->%x(%d)", e.k, e.v, e.dib)
}
