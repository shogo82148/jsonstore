package jsonstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"strings"

	"time"
)

func TestOpen(t *testing.T) {
	f, err := ioutil.TempFile("", "jsonstore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	ioutil.WriteFile(f.Name(), []byte(`{"hello":"world"}`), 0644)
	ks, err := Open(f.Name())
	if err != nil {
		t.Error(err)
	}
	if len(ks.data) != 1 {
		t.Errorf("expected %d got %d", 1, len(ks.data))
	}
	if world, ok := ks.data["hello"]; !ok || string(*world) != `"world"` {
		t.Errorf("expected %s got %s", "world", world)
	}
}

func TestGeneral(t *testing.T) {
	dir, err := ioutil.TempDir("", "jsonstore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	name := filepath.Join(dir, "foo.jsonstore")
	ks := new(JSONStore)
	err = ks.Set("hello", "world")
	if err != nil {
		t.Error(err)
	}
	if err := Save(ks, name); err != nil {
		t.Error(err)
	}

	ks2, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	var a string
	var b string
	ks.Get("hello", &a)
	ks2.Get("hello", &b)
	if a != b {
		t.Errorf("expected '%s' got '%s'", a, b)
	}

	// Set a object, using a Gzipped JSON
	type Human struct {
		Name   string
		Height float64
	}
	ks.Set("human:1", Human{"Dante", 5.4})
	name2 := filepath.Join(dir, "foo2.jsonstore.gz")
	Save(ks, name2)
	ks2, _ = Open(name2)
	var human Human
	ks2.Get("human:1", &human)
	if human.Height != 5.4 {
		t.Errorf("expected '%v', got '%v'", Human{"Dante", 5.4}, human)
	}
}

func TestRegex(t *testing.T) {
	ks := new(JSONStore)
	ks.Set("hello:1", "world1")
	ks.Set("hello:2", "world2")
	ks.Set("hello:3", "world3")
	ks.Set("world:1", "hello1")
	reg := regexp.MustCompile(`hello`)

	if len(ks.GetAll(reg.MatchString).data) != len(ks.Keys())-1 {
		t.Errorf("Problem getting all")
	}
}

func TestSaveAndRename(t *testing.T) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	js, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	sizeBefore := fi.Size()

	js.Set("human:1", Human{"Dante", 5.4})

	done := make(chan struct{}, 1)
	go func() {
		SaveAndRename(js, name)
		close(done)
	}()

LOOP:
	for {
		fi, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		sizeAfter := fi.Size()
		if sizeAfter < sizeBefore {
			t.Errorf("the file is broken: %d -> %d", sizeBefore, sizeAfter)
			break LOOP
		}

		select {
		case <-done:
			break LOOP
		default:
			time.Sleep(time.Millisecond)
		}
	}

	<-done
	js, err = Open(name)
	if err != nil {
		t.Fatal(err)
	}
	var human Human
	err = js.Get("human:1", &human)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRegex(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	matcher := func(key string) bool {
		return strings.HasPrefix(key, "key-")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ks.GetAll(matcher)
	}
}

func key(n int) string {
	return "key-" + strconv.Itoa(n)
}

func setupJsonstore(n int) (string, func(), error) {
	dir, err := ioutil.TempDir("", "jsonstore")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }

	filename := filepath.Join(dir, "foo.json.gz")
	js := new(JSONStore)
	for i := 0; i < n; i++ {
		err := js.Set(key(i), Human{"Dante", 5.4})
		if err != nil {
			cleanup()
			return "", nil, err
		}
	}
	err = Save(js, filename)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return filename, cleanup, nil
}

type Human struct {
	Name   string
	Height float64
}

func BenchmarkGet(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	err = ks.Set("human:1", Human{"Dante", 5.4})
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	var human Human
	for i := 0; i < b.N; i++ {
		ks.Get("human:1", &human)
	}
}

func BenchmarkParaGet(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	err = ks.Set("human:1", Human{"Dante", 5.4})
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var human Human
		for pb.Next() {
			ks.Get("human:1", &human)
		}
	})
}

func BenchmarkSet(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	// set a key to any object you want
	for i := 0; i < b.N; i++ {
		err := ks.Set("human:1", Human{"Dante", 5.4})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParaSet(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// set a key to any object you want
		for pb.Next() {
			err := ks.Set("human:1", Human{"Dante", 5.4})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOpen(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Open(name)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSave(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	js, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Save(js, name)
	}
}

func BenchmarkSaveSet(b *testing.B) {
	name, cleanup, err := setupJsonstore(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ks, err := Open(name)
	if err != nil {
		b.Fatal(err)
	}

	// run save in background
	done := make(chan struct{}, 1)
	go func() {
		for {
			Save(ks, name)
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	b.ResetTimer()
	// set a key to any object you want
	for i := 0; i < b.N; i++ {
		err := ks.Set("human:1", Human{"Dante", 5.4})
		if err != nil {
			b.Fatal(err)
		}
	}
	close(done)
}

func benchmarkJSONStore(b *testing.B, size int) {
	name, cleanup, err := setupJsonstore(size)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		js, err := Open(name)
		if err != nil {
			b.Fatal(err)
		}
		err = js.Set("human:1", Human{"Dante", 5.4})
		if err != nil {
			b.Fatal(err)
		}
		err = Save(js, name)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONStore(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}
	for _, size := range sizes {
		size := size
		b.Run(
			fmt.Sprintf("size%d", size),
			func(b *testing.B) { benchmarkJSONStore(b, size) },
		)
	}
}
