package simplejsondb

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	Ext            string = ".json"
	GZipExt        string = ".json.gz"
	ErrNoDirectory error  = errors.New("not a directory")
)

const (
	// NoMode represents no locking.
	NoMode LockMode = iota
	// ModeRead acquires a shared read lock.
	ModeRead
	// ModeWrite acquires an exclusive write lock.
	ModeWrite
	// ModeReadWrite is an alias for write (exclusive) lock.
	ModeReadWrite
)

type (
	// Options - extra configuration
	Options struct {
		UseGzip bool
	}

	db struct {
		useGzip bool
		path    string
	}

	collection struct {
		useGzip   bool
		mu        sync.RWMutex
		name      string
		path      string
		recMu     sync.Mutex
		recModes  map[string]LockMode
		recLocks  map[string]*sync.RWMutex
		recStates map[string]*LockState
	}
	// LockMode is an enum for lock modes used by manual locking APIs.
	LockMode int
)

// internal lock state tracking per ID to support safe unlock semantics
type LockState struct {
	R int // number of outstanding read locks acquired via LockID
	W int // number of outstanding write locks acquired via LockID (0 or 1)
}

// RecordLock combines the RWMutex and its LockState for a specific record ID.
type RecordLock struct {
	Lock  *sync.RWMutex
	State *LockState
	Mode  *LockMode
}

type (
	// Collection - it's like a table name
	Collection interface {
		Get(string) ([]byte, error)
		GetAll() [][]byte
		GetAllByName() map[string][]byte
		Create(string, []byte, ...Options) error
		Delete(string) error
		Len() uint64
		LockID(id string, mode LockMode) (LockMode, error)
		UnlockID(id string) error
		GetLock(id string) *RecordLock
		IsLock(id string) bool
	}
	// DB - a database
	DB interface {
		Collection(string) (Collection, error)
	}
)

// New - a database instance
func New(dbname string, options *Options) (DB, error) {
	opts := Options{}
	if options != nil {
		opts = *options
	}

	dbpath := filepath.Join(dbname)
	_, err := getOrCreateDir(dbpath)
	if err != nil {
		return nil, err
	}

	return &db{path: dbpath, useGzip: opts.UseGzip}, nil
}

// Collection returns the collection or table
func (db *db) Collection(name string) (Collection, error) {
	c := filepath.Join(db.path, name)
	dir, err := getOrCreateDir(c)
	if err != nil {
		return nil, err
	}
	if !dir.IsDir() {
		return nil, ErrNoDirectory
	}
	return &collection{name: name, path: c, useGzip: db.useGzip}, nil
}

// GetAll - returns all records
func (c *collection) GetAll() (data [][]byte) {
	records, err := os.ReadDir(c.path)
	if err != nil {
		return
	}
	for _, r := range records {
		if !r.IsDir() {
			fPath := filepath.Join(c.path, r.Name())
			record, err := os.ReadFile(fPath)
			if err != nil {
				continue // skipping a file which has issue
			}

			if strings.LastIndex(r.Name(), GZipExt) > 0 {
				record, _ = UnGzip(record) // skipping ungip error over mutli file fetch
			}

			data = append(data, record)
		}
	}
	return
}

// GetAllByName - returns all records
func (c *collection) GetAllByName() (data map[string][]byte) {
	data = make(map[string][]byte)

	records, err := os.ReadDir(c.path)
	if err != nil {
		return
	}
	for _, r := range records {
		if !r.IsDir() {
			fPath := filepath.Join(c.path, r.Name())
			record, err := os.ReadFile(fPath)
			if err != nil {
				continue // skipping a file which has issue
			}

			if strings.LastIndex(r.Name(), GZipExt) > 0 {
				record, _ = UnGzip(record) // skipping ungip error over mutli file fetch
			}

			name := strings.TrimSuffix(r.Name(), Ext)
			data[name] = record
		}
	}
	return
}

// Get help to retrive key based record
func (c *collection) Get(key string) (data []byte, err error) {
	filename, err, isGzip := c.getPathIfExist(key, err)
	data, err = os.ReadFile(filename)
	if err != nil {
		return
	}

	if isGzip {
		_data, _err := UnGzip(data)
		if _err != nil {
			return _data, _err
		}
		data, err = _data, _err
	}
	return
}

// Insert - helps to save data into model dir
func (c *collection) Create(key string, data []byte, options ...Options) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var useGzip bool = c.useGzip
	if !c.useGzip {
		if options != nil && options[0].UseGzip {
			useGzip = options[0].UseGzip
		}
	}
	filename := c.getFullPath(key, c.useGzip)
	if useGzip {
		data, err = Gzip(data)
		if err != nil {
			return err
		}
	}
	return os.WriteFile(filename, data, os.ModePerm)
}

// Delete - helps to delete model dir record
func (c *collection) Delete(key string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filename, err, _ := c.getPathIfExist(key, err)
	if err != nil {
		return err
	}

	return os.Remove(filename)
}

func (c *collection) Len() uint64 {
	records, _ := os.ReadDir(c.path)
	return uint64(len(records))
}

func getOrCreateDir(path string) (os.FileInfo, error) {
	f, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			cwd, err := os.Getwd()
			if err != nil {
				return nil, err
			}
			newDir := filepath.Join(cwd, path)
			err = os.Mkdir(filepath.Join(cwd, path), os.ModePerm)
			if err != nil {
				return nil, err
			}
			return os.Stat(newDir)
		}
		return f, err
	}
	return f, nil
}

func (c *collection) getFullPath(key string, isGzip bool) string {
	var record string
	if isGzip {
		record = key + GZipExt
	} else {
		record = key + Ext
	}
	filename := filepath.Join(c.path, record)

	return filename
}

func (c *collection) getPathIfExist(key string, err error) (string, error, bool) {
	record := key + Ext
	filename := filepath.Join(c.path, record)

	if success, err := c.isExist(filename, err); !success {
		record = key + GZipExt
		filename = filepath.Join(c.path, record)
		if success, err := c.isExist(filename, err); !success {
			return "", err, false
		}

		return filename, nil, true
	}

	return filename, nil, false
}

func (c *collection) isExist(filename string, err error) (bool, error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, err
	}
	if !info.IsDir() {
		return true, nil
	}
	return false, nil
}

func UnGzip(record []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	_, err = buffer.Write(record)
	if err != nil {
		return record, err
	}
	reader, err := gzip.NewReader(&buffer)

	result, err = io.ReadAll(reader)
	if err != nil {
		return record, err
	}

	err = reader.Close()
	if err != nil {
		return record, nil
	}

	return
}

func Gzip(data []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err = writer.Write(data)
	if err != nil {
		return data, err
	}
	err = writer.Close()
	result = buffer.Bytes()
	return result, err
}

// helper: returns the RWMutex for a specific record ID, creating it if needed
func (c *collection) getRecordLock(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		c.recLocks = make(map[string]*sync.RWMutex)
	}
	l, ok := c.recLocks[id]
	if !ok || l == nil {
		l = &sync.RWMutex{}
		c.recLocks[id] = l
	}
	return l
}

// helper: returns the RWMutex for a specific record ID if it exists; does not create it
func (c *collection) getRecordLockIfExists(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		return nil
	}
	return c.recLocks[id]
}

// helper: returns the LockState for a specific ID, creating it if needed
func (c *collection) getOrCreateState(id string) *LockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		c.recStates = make(map[string]*LockState)
	}
	st, ok := c.recStates[id]
	if !ok || st == nil {
		st = &LockState{}
		c.recStates[id] = st
	}
	return st
}

// helper: returns the LockState for a specific ID if it exists; does not create it
func (c *collection) getStateIfExists(id string) *LockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		return nil
	}
	return c.recStates[id]
}

// LockID allows manual locking for a specific record ID.
func (c *collection) LockID(id string, mode LockMode) (LockMode, error) {
	var err error = nil
	// detect possible deadlock due to double locking
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deadlock detected: possible double lock on ID '" + id + "' with mode " + strconv.Itoa(int(mode)))
			err = r.(error)
		}
	}()
	l := c.getRecordLock(id)
	st := c.getOrCreateState(id)
	if c.recModes == nil {
		c.recModes = make(map[string]LockMode)
	}
	c.recModes[id] = mode
	switch mode {
	case ModeRead:
		st.R++
		l.RLock()
		return ModeRead, err
	case ModeReadWrite:
		st.R++
		st.W++
		l.Lock()
		return ModeReadWrite, err
	// case ModeWrite
	default: // write/read_write/other (exclusive)
		st.W++
		l.Lock()
		return ModeWrite, err
	}
}

// UnlockID releases a previously acquired lock for a specific record ID.
// mode should match the mode used in LockID.
func (c *collection) UnlockID(id string) error {
	mode, ok := c.recModes[id]
	if !ok {
		mode = NoMode
	}
	var err error = nil
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deadlock detected: possible double unlock on ID '" + id + "' with mode " + strconv.Itoa(int(mode)))
			err = r.(error)
		}
	}()
	// Do not create a new lock when unlocking; if lock/state don't exist, it's a no-op
	l := c.getRecordLockIfExists(id)
	st := c.getStateIfExists(id)
	if l == nil || st == nil {
		return nil
	}

	switch mode {
	case ModeRead:
		if st.R <= 0 {
			err = errors.New("double unlock: read lock not held")
		} else {
			st.R--
			l.RUnlock()
		}
	case ModeReadWrite:
		if st.R <= 0 || st.W <= 0 {
			err = errors.New("double unlock: read lock not held")
		} else {
			st.R--
			st.W--
			l.Unlock()
		}
	default: // write/read_write/other (exclusive)
		if st.W <= 0 {
			err = errors.New("double unlock: read lock not held")
		} else {
			st.W--
			l.Unlock()
		}
	}

	return err
}

// GetLock returns the RecordLock (RWMutex + LockState) for a specific record ID.
func (c *collection) GetLock(id string) *RecordLock {
	lock := c.getRecordLock(id)
	if lock == nil {
		return &RecordLock{
			Lock:  nil,
			State: nil,
			Mode:  nil,
		}
	}
	exists := c.getStateIfExists(id)
	mode := c.recModes[id]
	if exists == nil {
		exists = &LockState{}
	}
	return &RecordLock{
		Lock:  lock,
		State: exists,
		Mode:  &mode,
	}
}

func (c *collection) IsLock(id string) bool {
	l := c.getRecordLockIfExists(id)
	st := c.getStateIfExists(id)
	mode := c.recModes[id]
	if l == nil || st == nil {
		return false
	}
	switch mode {
	case ModeRead:
		return st.R > 0
	case ModeReadWrite:
		return st.R > 0 && st.W > 0
	default: // write/read_write/other (exclusive)
		return st.W > 0
	}
}
