package stud

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDB(t *testing.T) {
	os.Remove("test.db")
	rand.Seed(time.Now().Unix())

	dbg("stage 1")
	m, err := Open("test.db", 128)
	panicerr(err)

	x := map[int]int{}

	start := time.Now()
	for i := 0; ; i++ {
		v := rand.Int()
		if err := m.Put(strconv.Itoa(v), strings.NewReader(strconv.Itoa(v))); err != nil {
			dbg("put #%d: %v", i, err)
			break
		}
		x[v] = v
	}
	dbg("put cost: %0.6fs", time.Since(start).Seconds()/float64(len(x)))

	start = time.Now()
	for k, v := range x {
		v2, _ := m.Get(strconv.Itoa(k))
		buf, _ := v2.ReadAll()
		if string(buf) != strconv.Itoa(v) {
			fmt.Println(k, v2, v)
		}
	}
	dbg("get cost: %0.6fs", time.Since(start).Seconds()/float64(len(x)))

	// fmt.Println(m.Rebuild())
	m.Range(func(r *Reader) bool {
		buf, _ := r.ReadAll()
		if string(buf) != r.Key {
			fmt.Println(buf, r.Key)
		}
		return true
	})

	m._writeDirty(true)

	m.Close()

	dbg("stage 2")
	m, err = Open("test.db", 128)
	panicerr(err)

	m.Close()
	os.Remove("test.db")
}
