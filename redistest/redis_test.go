package redis_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	redistest "github.com/soh335/go-test-redisserver"
	redis "gopkg.in/redis.v5"
)

func key(n int) string {
	return "key-" + strconv.Itoa(n)
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

type Human struct {
	Name   string
	Height float64
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
