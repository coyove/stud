package stud

import (
	"bytes"
	"fmt"
	"os"
	"time"

	_debug "runtime/debug"

	"github.com/cespare/xxhash"
)

var (
	debug           = os.Getenv("RBKVDBG") == "1"
	MaxSearch       = 32
	errDirty        = fmt.Errorf("dirty")
	ErrFull         = fmt.Errorf("full")
	ErrNotFound     = fmt.Errorf("not found")
	ErrInvalidEntry = fmt.Errorf("invalid entry")
)

const (
	Magic     = 0x0731
	entrySize = 21
	hdrSize   = 1 + 2 + 4 + 4 + 4 + 16
	// dirty flag (1b)
	// magic number (2b)
	// total buckets (4b)
	// bucket count (4b)
	// create time (4b)
	// uuid (16b)
)

type File interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Close() error
	Sync() error
	Seek(int64, int) (int64, error)
}

func hash(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func hash64(key string) uint64 {
	return xxhash.Sum64String(key[1:])
}

func dbg(f string, a ...interface{}) {
	fmt.Println(time.Now().Format(time.ANSIC), "]", fmt.Sprintf(f, a...))
}

func panicerr(err error) {
	if err != nil {
		panic(err)
	}
}

func panicerr2(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

func run(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err, _ = r.(error)
			if err == nil {
				err = fmt.Errorf("%v", r)
			}

			if debug {
				lines := bytes.Split(_debug.Stack(), []byte("\n"))
				buf := bytes.Buffer{}
				for i := 0; i < len(lines); i++ {
					const pkg = "robinkv"
					if bytes.Index(lines[i], []byte(pkg)) > -1 {
						l := lines[i+1]
						l = l[bytes.Index(l, []byte(pkg))+len(pkg):]
						l = bytes.SplitN(l, []byte(" "), 2)[0]
						buf.Write(l)
						i++
					}
				}
				dbg("catch error: %v%v", err, buf.String())
			}
		}
	}()
	f()
	return nil
}
