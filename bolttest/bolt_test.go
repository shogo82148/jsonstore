package bolt_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"
)

func key(n int) string {
	return "key-" + strconv.Itoa(n)
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
