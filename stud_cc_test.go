package stud

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/coyove/common/rand"
)

func TestStudConcurrentRead(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < COUNT; i++ {
		f.Create(strconv.Itoa(i), genReader(int64(i)))
	}

	wg := sync.WaitGroup{}
	for i := 0; i < COUNT; i++ {
		wg.Add(1)
		go func(i int) {
			v, _ := f.Open(strconv.Itoa(i))
			buf := v.ReadAllAndClose()
			vj := int64(binary.BigEndian.Uint64(buf))

			if vj != int64(i) {
				t.Error(vj, i)
			}
			wg.Done()

		}(i)
	}

	wg.Wait()

	f.Close()
	os.Remove("map")
}

func BenchmarkStudConcurrentRead(b *testing.B) {
	f, err := Open("test", &Options{MaxFds: 4, CacheSize: 1024 * 1024 * 16})
	if f == nil {
		b.Fatal(err)
	}

	r := rand.New()
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			f.Get(strconv.Itoa(r.Intn(COUNT)) + "12345678")
		}
	})

	f.Close()
}

func BenchmarkNativeConcurrentRead(b *testing.B) {
	r := rand.New()
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			f, _ := os.Open("test2/" + strconv.Itoa(r.Intn(COUNT)))
			buf := make([]byte, 8)
			f.Seek(0, 0)
			io.ReadAtLeast(f, buf, 8)
			f.Close()
		}
	})

}
