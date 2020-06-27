package stud

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sync"
	"time"
)

type DB struct {
	f            File
	wmu          sync.Mutex        // Put() locking
	bmu          [256]sync.RWMutex // bucket locks
	count        uint32
	createTime   uint32
	totalBuckets uint32
	uuid         [16]byte
}

func (m *DB) Count() int {
	return int(m.count)
}

func Open(path string, length uint32) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	m := &DB{f: f}
	if fi, _ := f.Stat(); fi.Size() == 0 {
		m.totalBuckets = length
		m.createTime = uint32(time.Now().Unix())
		rand.Read(m.uuid[:])
		return m, m.marshalHeader()
	}

	if err := m.unmarshalHeader(); err != nil {
		if err == errDirty {
			if debug {
				dbg("data is dirty, try rebuilding")
			}
			return m, m.Rebuild()
		}
		return nil, err
	}
	return m, nil
}

func (m *DB) Rebuild() error {
	return run(func() {
		tmp := *m
		tmp.f = &memFile{buf: make([]byte, hdrSize+entrySize*m.totalBuckets)}
		tmp.count = 0

		for i := uint32(0); i < m.totalBuckets; i++ {
			e := m._readBucket(i)
			if e.dib == 0 {
				continue
			}

			uuid := m._seek(int64(e.offset), 0)._read(new([16]byte)).([16]byte)
			if uuid == m.uuid {
				e.dib = 1
				tmp._putEntry(e, false)
			} else {
				if debug {
					dbg("rebuild: omit invalid block %d", i)
				}
			}
		}

		if debug {
			dbg("total rebuild: %d", tmp.count)
		}

		panicerr2(m.f.WriteAt(tmp.f.(*memFile).buf[hdrSize:], hdrSize))

		m.count = tmp.count
		m.writeCount()
		m._writeDirty(false)
	})
}

func (m *DB) marshalHeader() error {
	return run(func() {
		m._write(byte(0))
		m._write(byte(0))
		m._write(uint16(Magic))
		m._write(m.totalBuckets)
		m._write(m.count)
		m._write(m.createTime)
		m._write(m.uuid)
		for i := uint32(0); i < m.totalBuckets; i++ {
			(entry{})._marshal(m.f)
		}
	})
}

func (m *DB) unmarshalHeader() error {
	return run(func() {
		var xerr error
		if m._read(new(byte)).(byte) != 0 {
			xerr = errDirty
		}
		m._read(new(byte))
		mn := m._read(new(uint16)).(uint16)
		if mn != Magic {
			panic(fmt.Errorf("invalid header (magic)"))
		}
		m.totalBuckets = m._read(new(uint32)).(uint32)
		m.count = m._read(new(uint32)).(uint32)
		m.createTime = m._read(new(uint32)).(uint32)
		m.uuid = m._read(new([16]byte)).([16]byte)
		panicerr(xerr)
	})
}

func (m *DB) _readBucket(i uint32) entry {
	m.bmu[byte(i)].RLock()
	defer m.bmu[byte(i)].RUnlock()
	m._seek(hdrSize+int64(i)*entrySize, 0)
	return _unmarshalEntry(m.f)
}

func (m *DB) _writeBucket(i uint32, e entry) {
	m.bmu[byte(i)].Lock()
	defer m.bmu[byte(i)].Unlock()
	m._seek(hdrSize+int64(i)*entrySize, 0)
	e._marshal(m.f)
}

func (m *DB) _writeDirty(dirty bool) {
	var v [1]byte
	if dirty {
		v[0] = 1
	}
	panicerr2(m.f.WriteAt(v[:], 0))
}

func (m *DB) writeCount() { // may fail, but we don't care
	if _, err := m.f.Seek(7, 0); err != nil {
		return
	}
	binary.Write(m.f, binary.BigEndian, m.count)
}

func (m *DB) Put(key string, r io.Reader) error {
	if len(key) < 1 {
		return fmt.Errorf("empty key")
	}

	m.wmu.Lock()
	defer m.wmu.Unlock()
	return run(func() {
		oldEOF := panicerr2(m.f.Seek(0, 2)).(int64)
		m._write(m.uuid)
		m._write(uint32(len(key)))
		m._write([]byte(key))
		m._write(uint32(time.Now().Unix()))
		lengthMark := m._currentOffset()
		m._write(uint64(0))
		nw := panicerr2(io.Copy(m.f, r)).(int64)
		endMark := m._currentOffset()
		m._seek(lengthMark, 0)
		m._write(uint64(nw))
		m._writeDirty(true)
		// fmt.Println(hash(key), hash64(key))
		e := entry{dib: 1, hash: hash(key), k: hash64(key), offset: uint64(oldEOF), end: uint64(endMark)}
		if err := run(func() { m._putEntry(e, true) }); err != nil {
			if err == ErrFull {
				m._writeDirty(false)
			}
			panic(err)
		}
		m._writeDirty(false)
	})
}

func (m *DB) _putEntry(e entry, limitedRealloc bool) {
	i := e.hash % m.totalBuckets

	ms := MaxSearch
	if !limitedRealloc {
		ms = math.MaxInt32 // big number
	}

	type plan struct {
		index uint32
		e     entry
	}
	var plans []plan

	for retry := 0; retry < ms; retry++ {
		bk := m._readBucket(i)
		// if bk.k != 0 && bk.dib == 0 {
		// 	panic(i)
		// }
		if bk.dib == 0 {
			m._writeBucket(i, e)
			m.count++
			m.writeCount()
			goto EXECUTE_PLANS
		}
		if e.hash == bk.hash && e.k == bk.k {
			bk.offset = e.offset
			m._writeBucket(i, bk)
			goto EXECUTE_PLANS
		}
		if bk.dib < e.dib {
			plans = append(plans, plan{i, e})
			e = bk
		}
		i = (i + 1) % m.totalBuckets
		e.dib++
	}
	panic(ErrFull)

EXECUTE_PLANS:
	for i := len(plans) - 1; i >= 0; i-- {
		p := plans[i]
		m._writeBucket(p.index, p.e)
	}
}

func (m *DB) _makeReader() *Reader {
	strlen := m._read(new(uint32)).(uint32)
	str := make([]byte, strlen)
	panicerr2(m.f.Read(str))
	ts := m._read(new(uint32)).(uint32)
	length := m._read(new(uint64)).(uint64)
	return &Reader{
		Key:        string(str),
		CreateTime: time.Unix(int64(ts), 0),
		r:          io.LimitReader(m.f, int64(length)),
	}
}

func (m *DB) Get(key string) (*Reader, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("empty key")
	}

	var r *Reader
	err := run(func() {
		hash, hash64 := hash(key), hash64(key)
		i := hash % m.totalBuckets
		for dib := 1; ; dib++ {
			bk := m._readBucket(i)
			if int(bk.dib) < dib {
				break
			}
			if bk.hash == hash && bk.k == hash64 {
				if uuid := m._seek(int64(bk.offset), 0)._read(new([16]byte)).([16]byte); uuid != m.uuid {
					panic(ErrInvalidEntry)
				}

				r = m._makeReader()
				return
			}
			i = (i + 1) % m.totalBuckets
		}
		panic(ErrNotFound)
	})
	return r, err
}

func (m *DB) Range(cb func(*Reader) bool) error {
	return run(func() {
		for i := uint32(0); i < m.totalBuckets; i++ {
			e := m._readBucket(i)
			if e.dib == 0 {
				continue
			}

			if uuid := m._seek(int64(e.offset), 0)._read(new([16]byte)).([16]byte); uuid != m.uuid {
				panic(ErrInvalidEntry)
			}

			if !cb(m._makeReader()) {
				break
			}
		}
	})
}

func (m *DB) Close() error {
	return m.f.Close()
}

func (m *DB) _read(x interface{}) interface{} {
	panicerr(binary.Read(m.f, binary.BigEndian, x))
	return reflect.ValueOf(x).Elem().Interface()
}

func (m *DB) _write(x interface{}) {
	panicerr(binary.Write(m.f, binary.BigEndian, x))
}

func (m *DB) _seek(pos int64, w int) *DB {
	panicerr2(m.f.Seek(pos, w))
	return m
}

func (m *DB) _currentOffset() int64 {
	return panicerr2(m.f.Seek(0, 2)).(int64)
}
