package stud

import (
	"encoding/binary"
	"fmt"
	"io"
)

type entry struct {
	dib    byte
	pad    [3]byte
	hash   uint32
	k      uint64
	offset uint64
	end    uint64
}

func (e entry) _marshal(w File) {
	panicerr2(w.Write(emptyEntry[:]))
	panicerr2(w.Seek(-entrySize, 1))
	panicerr(binary.Write(w, binary.BigEndian, e.dib))
	panicerr(binary.Write(w, binary.BigEndian, e.pad))
	panicerr(binary.Write(w, binary.BigEndian, e.hash))
	panicerr(binary.Write(w, binary.BigEndian, e.k))
	panicerr(binary.Write(w, binary.BigEndian, e.offset))
	panicerr(binary.Write(w, binary.BigEndian, e.end))
}

func _unmarshalEntry(r io.Reader) entry {
	e := entry{}
	panicerr(binary.Read(r, binary.BigEndian, &e.dib))
	panicerr(binary.Read(r, binary.BigEndian, &e.pad))
	panicerr(binary.Read(r, binary.BigEndian, &e.hash))
	panicerr(binary.Read(r, binary.BigEndian, &e.k))
	panicerr(binary.Read(r, binary.BigEndian, &e.offset))
	panicerr(binary.Read(r, binary.BigEndian, &e.end))
	return e
}

func (e entry) String() string {
	if e.k == 0 {
		return "null"
	}
	return fmt.Sprintf("%x->%x(%d)", e.k, e.offset, e.dib)
}
