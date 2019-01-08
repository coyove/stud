package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

func main() {
	os.RemoveAll("stud")
	os.MkdirAll("stud", 0777)

	files, _ := ioutil.ReadDir("../../")
	gp := 0
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".go") {
			continue
		}
		if strings.HasSuffix(file.Name(), "_test.go") {
			continue
		}

		path := "../../" + file.Name()
		fset := token.NewFileSet()
		node, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			panic(err)
		}
		pos := []int{}
		for _, decl := range node.Decls {
			if f, _ := decl.(*ast.FuncDecl); f != nil {
				if (f.Name.Name) != "testPanic" {
					pos = append(pos, int(f.Body.Pos()))
					for _, stmt := range f.Body.List {
						if cal, _ := stmt.(*ast.ExprStmt); cal != nil {
							if strings.Contains(fmt.Sprintf("%v", cal.X.(*ast.CallExpr).Fun), "Lock") {
								continue
							}
						}
						if dcal, _ := stmt.(*ast.DeferStmt); dcal != nil {
							if strings.Contains(fmt.Sprintf("%v", dcal.Call.Fun), "Unlock") {
								continue
							}
						}
						if ret, _ := stmt.(*ast.ReturnStmt); ret == nil {
							pos = append(pos, int(stmt.End()))
						}
					}
				}
			}
		}

		sort.Ints(pos)
		buf, _ := ioutil.ReadFile(path)
		pbuf := bytes.Buffer{}
		plast := 0
		for _, pos := range pos {
			pbuf.Write(buf[plast:pos])
			pbuf.WriteString(fmt.Sprintf("\ntestPanic(%d)\n", gp))
			gp++
			plast = pos
		}

		pbuf.Write(buf[plast:])
		ioutil.WriteFile("stud/"+file.Name(), pbuf.Bytes(), 0777)
	}

	testCode := fmt.Sprintf(`
package stud

import (
	"testing"
	"os"
	"fmt"
	"strings"
	"strconv"
)

var gpEnd = %d

func TestM(t *testing.T) {
	test := func() {
		name := fmt.Sprintf("map%%d", testPtr) 
		ceil := 0

		defer func() {
			recover()

			if ceil == 0 {
				t.Log("panic", testPtr, "omitted")
				return
			} 
			t.Log("panic", testPtr, "at", ceil)

			if _, err := os.Stat(name); err != nil {
				return
			}

			testPtr = -1
			f, err := Open(name, nil)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < ceil; i++ {
				key := strconv.Itoa(i)
				buf, err := f.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				if key != string(buf) {
					t.Fatal(key, string(buf))
				}
			}

			if f.Count() != ceil {
				t.Fatal(f.Count(), ceil)
			}

			f.Close()
		}()

		f, err := Open(name, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		for i := 0; i < 256; i++ {
			key := strconv.Itoa(i)
			err := f.Create(key, strings.NewReader(key))
			if err != nil {
				t.Fatal(key, err)
			}
			ceil = i + 1
		}
	}

	defaultMMapSize = 1024 * 1024 * 1024
	if testSetMem != nil {
		testSetMem()
	}
	defaultMMapSize = 1024 * 1024 * 4

	testNoLock = true
	for i := 0 ; i < gpEnd; i++ {
		testPtr = i
		test()
		fmt.Printf("\rTest pointer: %%d / %%d", i, gpEnd)
	}

	fmt.Printf("\r\n")
	os.Remove("map")
}`, gp)

	ioutil.WriteFile("stud/main_test.go", []byte(testCode), 0777)
}
