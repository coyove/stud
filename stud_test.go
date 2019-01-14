package stud

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/coyove/common/rand"
)

const COUNT = 1 << 10

func genReader(r interface{}) io.Reader {
	buf := &bytes.Buffer{}
	switch x := r.(type) {
	case *rand.Rand:
		buf.Write(x.Fetch(8))
	case []byte:
		buf.Write(x)
	case int64:
		p := [8]byte{}
		binary.BigEndian.PutUint64(p[:], uint64(x))
		buf.Write(p[:])
	case int:
		p := [8]byte{}
		binary.BigEndian.PutUint64(p[:], uint64(x))
		buf.Write(p[:])
	case string:
		buf.WriteString(x)
	}
	return buf
}

var marker = []byte{1, 2, 3, 4, 5, 6, 7, 8}

func TestOpenStud(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i), genReader(r))
		fmt.Print("\r", i)
	}
	fmt.Print("\r")

	f.Create("137393731", genReader(marker))
	f.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	v, _ := f.Open("137393731")
	buf := v.ReadAllAndClose()
	if !bytes.Equal(buf, marker) {
		t.Error(buf)
	}

	if v, err := f.Open(strconv.Itoa(COUNT / 2)); err != nil {
		t.Error(err)
	} else {
		v.Close()
	}

	f.Close()
	os.Remove("map")
}

func TestOpenStudSmallMMap(t *testing.T) {
	// 40K mmap, 32K node buffer, 4K per node, which means only one node can reside in it
	f, err := Open("map", &Options{MMapSize: 1024 * 40})
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i), genReader(i))
		fmt.Print("\r", i)
	}
	fmt.Print("\r")

	f.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.Walk(nil, func(k string, v *Stream) error {
		num := int(binary.BigEndian.Uint64(v.ReadAllAndClose()))
		if k != strconv.Itoa(num) {
			t.Error(k, num)
		}
		return nil
	})

	f.Close()
	os.Remove("map")
}

func TestOpenStudLongKey(t *testing.T) {
	// 8K mmap, 4K per node, which means only one node can reside in it
	f, err := Open("map", &Options{MMapSize: 1024 * 8})
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i)+".12345678", genReader(i))
		fmt.Print("\r", i)
	}
	fmt.Print("\r")

	f.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.Walk(nil, func(k string, v *Stream) error {
		if k[:strings.Index(k, ".")] != strconv.Itoa(int(binary.BigEndian.Uint64(v.ReadAllAndClose()))) {
			t.Error(k)
		}
		return nil
	})

	f.Close()
	os.Remove("map")
}

func TestOpenStudFlag(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	m := map[string]bool{}
	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i), genReader(i))
		m[strconv.Itoa(i)] = true
		f.Flag(strconv.Itoa(i), func(uint32) uint32 { return uint32(i) })
	}
	f.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.Walk(func(md Metadata) bool {
		delete(m, md.ShortName())
		return true
	}, func(k string, v *Stream) error {
		x := binary.BigEndian.Uint64(v.ReadAllAndClose())
		if k != strconv.Itoa(int(x)) {
			t.Error(k)
		}
		if v.Flag() != uint32(x) {
			t.Error(k)
		}
		return nil
	})

	if len(m) > 0 {
		t.Error("Metadata filter failed")
	}

	f.Close()
	os.Remove("map")
}

func TestOpenStud2(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		f.Create(strconv.Itoa(i), genReader(int64(i)))
		if f.Count() != i+1 {
			t.Error("Count() failed")
		}
		if f.Size() != int64(i+1)*8 {
			t.Error("Size() failed")
		}
		for j := 0; j < i; j++ {
			v, _ := f.Open(strconv.Itoa(j))
			buf := v.ReadAllAndClose()
			vj := int64(binary.BigEndian.Uint64(buf))

			if vj != int64(j) {
				t.Error(vj, j)
			}
		}
	}

	f.Close()
	os.Remove("map")
}

func TestOpenStud2Random(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	m := map[string]int{}

	r := rand.New()
	for i := 0; i < COUNT*2; i++ {
		ir := int(r.Uint64())
		si := strconv.Itoa(ir)
		f.Create(si, genReader(int64(ir)))
		m[si] = ir

		if r.Intn(5) == 1 {
			f.Close()
			f, err = Open("map", nil)
			if f == nil {
				t.Fatal(err)
			}
		}

		fmt.Print("\r", i)
	}

	fmt.Print("\r")

	f.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for k, vi := range m {
		v2, _ := f.Open(k)
		v2i := int(binary.BigEndian.Uint64(v2.ReadAllAndClose()))
		if v2i != vi {
			t.Error(v2i, vi)
		}
	}

	f.Close()

	os.Remove("map")
}

func BenchmarkStud(b *testing.B) {
	f, err := Open("test", nil)
	if f == nil {
		b.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < b.N; i++ {
		v, _ := f.Open(strconv.Itoa(r.Intn(COUNT)) + "12345678")
		if v != nil {
			v.ReadAllAndClose()
		}
	}

	f.Close()
}

func BenchmarkStudCached(b *testing.B) {
	f, err := Open("test", &Options{CacheSize: 1024 * 1024 * 16})
	if f == nil {
		b.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < b.N; i++ {
		f.Get(strconv.Itoa(r.Intn(COUNT)) + "12345678")
	}

	f.Close()
}

func BenchmarkBolt(b *testing.B) {
	db, _ := bolt.Open("bolt", 0666, nil)
	r := rand.New()
	db.View(func(tx *bolt.Tx) error {
		for i := 0; i < b.N; i++ {
			b := tx.Bucket([]byte("main"))
			b.Get([]byte(strconv.Itoa(r.Intn(COUNT)) + "12345678"))
		}
		return nil
	})

	db.Close()
}

func BenchmarkNative(b *testing.B) {
	r := rand.New()
	for i := 0; i < b.N; i++ {
		f, _ := os.Open("test2/" + strconv.Itoa(r.Intn(COUNT)))
		fi, _ := f.Stat()
		buf := make([]byte, fi.Size())
		io.ReadAtLeast(f, buf, int(fi.Size()))
		f.Close()
	}
}

func TestMain(m *testing.M) {
	if testSetMem != nil {
		testSetMem()
	}

	os.RemoveAll("test2")
	os.Remove("test")
	os.Remove("test.lock")
	os.Remove("map.lock")
	os.Remove("map")
	os.Remove("bolt")

	start := time.Now()
	os.Mkdir("test2", 0777)
	rbuf := make([]byte, 8192)
	for i := 0; i < COUNT; i++ {
		ioutil.WriteFile("test2/"+strconv.Itoa(i), rbuf, 0666)
		fmt.Print("\rNative:", i)
	}
	fmt.Print("\r")
	fmt.Println("Native in", time.Now().Sub(start).Seconds(), "s")

	f, err := Open("test", &Options{ForceCreate: true})
	if err != nil {
		panic(err)
	}

	start = time.Now()
	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i)+"12345678", genReader(rbuf))
		fmt.Print("\rStud:", i)
	}
	fmt.Print("\r")
	fmt.Println("Stud in", time.Now().Sub(start).Seconds(), "s")

	start = time.Now()
	db, _ := bolt.Open("bolt", 0666, nil)
	for i := 0; i < COUNT; i++ {
		db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte("main"))
			b.Put([]byte(strconv.Itoa(i)+"12345678"), rbuf)
			fmt.Print("\rBolt:", i)
			return nil
		})
	}
	db.Close()
	fmt.Print("\r")
	fmt.Println("Bolt in", time.Now().Sub(start).Seconds(), "s")

	f.Close()

	code := m.Run()

	os.RemoveAll("test2")
	os.Remove("test")
	os.Remove("test.lock")
	os.Remove("map.lock")
	os.Remove("map")
	os.Remove("bolt")
	os.Exit(code)
}
