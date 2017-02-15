package jsonstore

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// NoSuchKeyError is thrown when calling Get with invalid key
type NoSuchKeyError struct {
	key string
}

func (err NoSuchKeyError) Error() string {
	return "jsonstore: no such key \"" + err.key + "\""
}

// JSONStore is the basic store object.
type JSONStore struct {
	data       map[string]*json.RawMessage
	diffCount  int64
	setCount   int64
	savedCount int64
	save       chan struct{}
	stop       chan struct{}
	done       chan struct{}
	sync.RWMutex
}

// Open will load a jsonstore from a file.
func Open(filename string) (*JSONStore, error) {
	// load from file
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// decode gzip if filename has ".gz" suffix
	var r io.ReadCloser = f
	if strings.HasSuffix(filename, ".gz") {
		r, err = gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
	}

	// decode json
	dec := json.NewDecoder(r)
	var data map[string]*json.RawMessage
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	return &JSONStore{data: data}, nil
}

// Save writes the jsonstore to disk.
func Save(ks *JSONStore, filename string) error {
	return save(ks, filename, true)
}

func save(ks *JSONStore, filename string, takeSnapshot bool) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var w io.WriteCloser = f
	if strings.HasSuffix(filename, ".gz") {
		w = gzip.NewWriter(f)
		defer w.Close()
	}
	return ks.saveToWriter(w, takeSnapshot)
}

// SaveAndRename writes the jsonstore to disk more safely.
// First, SaveAndRename writes the jsonstore to temporary file,
// and then rename it to filename.
// NOTE: os.Rename renames atomic on POSIX systems, but no guarantee on other systems.
func SaveAndRename(ks *JSONStore, filename string) error {
	return saveAndRename(ks, filename, true)
}

func saveAndRename(ks *JSONStore, filename string, takeSnapshot bool) error {
	tmpfile := fmt.Sprintf("%s.tmp-%d", filename, time.Now().Unix())
	if strings.HasSuffix(filename, ".gz") {
		tmpfile += ".gz"
	}
	defer os.Remove(tmpfile)
	err := save(ks, tmpfile, takeSnapshot)
	if err != nil {
		return err
	}
	return os.Rename(tmpfile, filename)
}

// SaveToWriter writes the jsonstore to io.Writer
func (s *JSONStore) saveToWriter(w io.Writer, takeSnapshot bool) error {
	snapshot := s
	if takeSnapshot {
		snapshot = s.snapshot(false)
	}
	enc := json.NewEncoder(w)
	return enc.Encode(snapshot.data)
}

// StartAutoSave starts auto saving.
func (s *JSONStore) StartAutoSave(filename string, d time.Duration, count int64) {
	s.Lock()
	s.diffCount = count
	s.save = make(chan struct{}, 1)
	s.stop = make(chan struct{}, 1)
	s.done = make(chan struct{}, 1)
	s.Unlock()

	go func() {
		var c <-chan time.Time
		if d != 0 {
			ticker := time.NewTicker(d)
			defer ticker.Stop()
			c = ticker.C
		}

		loop := true
		for loop {
			select {
			case <-s.stop:
				// when StopAutoSave() called
				loop = false
			case <-s.save:
				// when `count` changes occur
			case <-c:
				// the ticks are delivered
			}
			snapshot := s.snapshot(true)
			if snapshot == nil {
				continue
			}
			save(snapshot, filename, false)
			s.Lock()
			s.savedCount = snapshot.setCount
			s.Unlock()
		}
		close(s.done)
	}()
}

// StopAutoSave stops auto saving.
func (s *JSONStore) StopAutoSave() {
	close(s.stop)
	<-s.done // wait for saving goroutine
}

// Set saves a value at the given key.
func (s *JSONStore) Set(key string, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	if s.data == nil {
		s.data = make(map[string]*json.RawMessage)
	}
	s.data[key] = (*json.RawMessage)(&b)
	s.setCount++
	if s.diffCount != 0 && s.setCount-s.savedCount >= s.diffCount {
		select {
		case s.save <- struct{}{}:
		default:
		}
	}
	return nil
}

// Get will return the value associated with a key.
func (s *JSONStore) Get(key string, v interface{}) error {
	s.RLock()
	b, ok := s.data[key]
	s.RUnlock()
	if !ok {
		return NoSuchKeyError{key}
	}
	return json.Unmarshal(*b, v)
}

// GetAll is like a filter with a regexp.
func (s *JSONStore) GetAll(matcher func(key string) bool) *JSONStore {
	s.RLock()
	defer s.RUnlock()
	results := make(map[string]*json.RawMessage)
	for k, v := range s.data {
		if matcher == nil || matcher(k) {
			results[k] = v
		}
	}
	return &JSONStore{
		data:     results,
		setCount: s.setCount,
	}
}

func (s *JSONStore) snapshot(skipIfSaved bool) *JSONStore {
	s.RLock()
	defer s.RUnlock()
	if skipIfSaved && s.setCount == s.savedCount {
		return nil
	}
	results := make(map[string]*json.RawMessage)
	for k, v := range s.data {
		results[k] = v
	}
	return &JSONStore{
		data:     results,
		setCount: s.setCount,
	}
}

// Keys returns all the keys currently in map
func (s *JSONStore) Keys() []string {
	s.RLock()
	defer s.RUnlock()
	keys := make([]string, len(s.data))
	i := 0
	for k := range s.data {
		keys[i] = k
		i++
	}
	return keys
}

// Delete removes a key from the store.
func (s *JSONStore) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.data, key)
}

// Size returns the count element in the store.
func (s *JSONStore) Size() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.data)
}
