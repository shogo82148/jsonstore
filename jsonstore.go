package jsonstore

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"regexp"
	"strings"
	"sync"
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
	data map[string]json.RawMessage
	sync.RWMutex
}

// Open will load a jsonstore from a file.
func Open(filename string) (*JSONStore, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(filename, ".gz") {
		r, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		b, err = ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
	}
	ks := new(JSONStore)

	// First Unmarshal the strings
	toOpen := make(map[string]string)
	err = json.Unmarshal(b, &toOpen)
	if err != nil {
		return nil, err
	}
	// Save to the raw message
	ks.data = make(map[string]json.RawMessage)
	for key := range toOpen {
		ks.data[key] = json.RawMessage(toOpen[key])
	}
	return ks, nil
}

// Save writes the jsonstore to disk.
func Save(ks *JSONStore, filename string) (err error) {
	ks.RLock()
	defer ks.RUnlock()

	toSave := make(map[string]string)
	for key := range ks.data {
		toSave[key] = string(ks.data[key])
	}
	b, err := json.MarshalIndent(toSave, "", " ")
	if err != nil {
		return
	}
	if strings.HasSuffix(filename, ".gz") {
		var b2 bytes.Buffer
		w := gzip.NewWriter(&b2)
		w.Write(b)
		w.Close()
		b = b2.Bytes()
	}
	return ioutil.WriteFile(filename, b, 0644)
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
		s.data = make(map[string]json.RawMessage)
	}
	s.data[key] = json.RawMessage(b)
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
	return json.Unmarshal(b, v)
}

// GetAll is like a filter with a regexp.
func (s *JSONStore) GetAll(re *regexp.Regexp) map[string]json.RawMessage {
	s.RLock()
	defer s.RUnlock()
	results := make(map[string]json.RawMessage)
	for k, v := range s.data {
		if re.MatchString(k) {
			results[k] = v
		}
	}
	return results
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
