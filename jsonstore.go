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
	data map[string]*json.RawMessage
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
	return ks.SaveToWriter(w)
}

// SaveAndRename writes the jsonstore to disk safely.
// First, SaveAndRename writes the jsonstore to temporary file,
// and then rename it to filename.
// NOTE: os.Rename renames atomic on POSIX systems, but no guarantee on other systems.
func SaveAndRename(ks *JSONStore, filename string) error {
	tmpfile := fmt.Sprintf("%s.tmp-%d", filename, time.Now().Unix())
	if strings.HasSuffix(filename, ".gz") {
		tmpfile += ".gz"
	}
	defer os.Remove(tmpfile)
	err := Save(ks, tmpfile)
	if err != nil {
		return err
	}
	return os.Rename(tmpfile, filename)
}

// SaveToWriter writes the jsonstore to io.Writer
func (s *JSONStore) SaveToWriter(w io.Writer) error {
	snapshot := s.GetAll(nil)
	enc := json.NewEncoder(w)
	return enc.Encode(snapshot.data)
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
	return &JSONStore{data: results}
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
