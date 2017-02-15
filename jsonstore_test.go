package jsonstore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"

	"strings"

	redistest "github.com/soh335/go-test-redisserver"
	redis "gopkg.in/redis.v5"
)

func testFile() *os.File {
	f, err := ioutil.TempFile(".", "jsonstore")
	if err != nil {
		panic(err)
	}
	return f
}

func TestOpen(t *testing.T) {
	f := testFile()
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

func setupRedis(conf redistest.Config, n int) (*redis.Client, func(), error) {
	s, err := redistest.NewServer(true, conf)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { s.Stop() }

	c := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	})
	for i := 0; i < n; i++ {
		b, err := json.Marshal(Human{"Dante", 5.4})
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		err = c.Set(key(i), b, 0).Err()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
	}
	return c, cleanup, nil
}

func setupBolt(n int) (string, func(), error) {
	dir, err := ioutil.TempDir("", "bolt")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }

	filename := filepath.Join(dir, "bolt.db")
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		return err
	})
	if err != nil {
		cleanup()
		return "", nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		for i := 0; i < n; i++ {
			d, err := json.Marshal(Human{"Dante", 5.4})
			if err != nil {
				return err
			}
			return b.Put([]byte(key(i)), d)
		}
		return nil
	})
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

func TestRedis(t *testing.T) {
	client, cleanup, err := setupRedis(nil, 0)
	if err != nil {
		t.Skip("redis is not installed")
	}
	defer cleanup()

	err = client.Set("data", 1234, 0).Err()
	if err != nil {
		t.Errorf(err.Error())
	}
	val, err := client.Get("data").Result()
	if err != nil {
		t.Errorf(err.Error())
	}
	if val != "1234" {
		t.Errorf("Got %v instead of 1234", val)
	}
}

func BenchmarkRedisSet(b *testing.B) {
	client, cleanup, err := setupRedis(nil, 1000)
	if err != nil {
		b.Skip("redis is not installed")
	}
	defer cleanup()

	b.ResetTimer()
	// set a key to any object you want
	for i := 0; i < b.N; i++ {
		bJSON, _ := json.Marshal(Human{"Dante", 5.4})
		err := client.Set("human:1", bJSON, 0).Err()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisParaSet(b *testing.B) {
	client, cleanup, err := setupRedis(nil, 1000)
	if err != nil {
		b.Skip("redis is not installed")
	}
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// set a key to any object you want
		for pb.Next() {
			bJSON, _ := json.Marshal(Human{"Dante", 5.4})
			err := client.Set("human:1", bJSON, 0).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisGet(b *testing.B) {
	client, cleanup, err := setupRedis(nil, 1000)
	if err != nil {
		b.Skip("redis is not installed")
	}
	defer cleanup()

	bJSON, _ := json.Marshal(Human{"Dante", 5.4})
	err = client.Set("human:1", bJSON, 0).Err()
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := client.Get("human:1").Result()
		var human Human
		json.Unmarshal([]byte(v), &human)
	}
}

func BenchmarkRedisParaGet(b *testing.B) {
	client, cleanup, err := setupRedis(nil, 1000)
	if err != nil {
		b.Skip("redis is not installed")
	}
	defer cleanup()

	bJSON, _ := json.Marshal(Human{"Dante", 5.4})
	err = client.Set("human:1", bJSON, 0).Err()
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, _ := client.Get("human:1").Result()
			var human Human
			json.Unmarshal([]byte(v), &human)
		}
	})
}

func benchmarkRedis(b *testing.B, size int) {
	client, cleanup, err := setupRedis(redistest.Config{
		"appendonly": "yes",

		// this option is very very slow.
		// enable this in fairness.
		"appendfsync": "always",
	}, size)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bJSON, _ := json.Marshal(Human{"Dante", 5.4})
		err := client.Set("human:1", bJSON, 0).Err()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedis(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}
	for _, size := range sizes {
		size := size
		b.Run(
			fmt.Sprintf("size%d", size),
			func(b *testing.B) { benchmarkRedis(b, size) },
		)
	}
}

func TestBolt(t *testing.T) {
	defer os.Remove("my.db")
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		t.Errorf(err.Error())
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		err := b.Put([]byte("data"), []byte("1234"))
		return err
	})
	if err != nil {
		t.Errorf(err.Error())
	}

	var result string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		result = string(b.Get([]byte("data")))
		return nil
	})

	if result != "1234" {
		t.Errorf("Problem reading/writing with BoltDB")
	}
}

func BenchmarkBoltSet(b *testing.B) {
	name, cleanup, err := setupBolt(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	db, err := bolt.Open(name, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			bJSON, _ := json.Marshal(Human{"Dante", 5.4})
			err := b.Put([]byte("data"), bJSON)
			return err
		})
	}
}

func BenchmarkBoltParaSet(b *testing.B) {
	name, cleanup, err := setupBolt(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	db, err := bolt.Open(name, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("MyBucket"))
				bJSON, _ := json.Marshal(Human{"Dante", 5.4})
				err := b.Put([]byte("data"), bJSON)
				return err
			})
		}
	})
}

func BenchmarkBoltGet(b *testing.B) {
	name, cleanup, err := setupBolt(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	db, err := bolt.Open(name, 0600, nil)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		bJSON, _ := json.Marshal(Human{"Dante", 5.4})
		err := b.Put([]byte("data"), bJSON)
		return err
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			dat := b.Get([]byte("data"))
			var human Human
			json.Unmarshal([]byte(dat), &human)
			return nil
		})
	}
}

func BenchmarkBoltParaGet(b *testing.B) {
	name, cleanup, err := setupBolt(1000)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	db, err := bolt.Open(name, 0600, nil)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		bJSON, _ := json.Marshal(Human{"Dante", 5.4})
		err := b.Put([]byte("data"), bJSON)
		return err
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("MyBucket"))
				dat := b.Get([]byte("data"))
				var human Human
				json.Unmarshal([]byte(dat), &human)
				return nil
			})
		}
	})
}

func benchmarkBolt(b *testing.B, size int) {
	name, cleanup, err := setupBolt(size)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	// Open is out of the loop, because another process can read changes by Sync.
	db, err := bolt.Open(name, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			bJSON, err := json.Marshal(Human{"Dante", 5.4})
			if err != nil {
				return err
			}
			return b.Put([]byte("data"), bJSON)
		})
		if err != nil {
			b.Fatal(err)
		}
		err = db.Sync()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}
	for _, size := range sizes {
		size := size
		b.Run(
			fmt.Sprintf("size%d", size),
			func(b *testing.B) { benchmarkBolt(b, size) },
		)
	}
}
