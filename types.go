package simplejsondb

import "errors"

// LockType represents the type of lock requested for a path.
type LockType int

const (
	// LOCK_READ acquires a shared/read lock for the file path.
	LOCK_READ LockType = iota
	// LOCK_WRITE acquires an exclusive/write lock for the file path.
	LOCK_WRITE
	// LOCK_READ_WRITE acquires an exclusive lock suitable for read-modify-write cycles.
	LOCK_READ_WRITE
)

var (
	// Ext is the standard uncompressed JSON file extension.
	Ext = ".json"
	// GZipExt is the gzip-compressed JSON file extension.
	GZipExt = ".json.gz"

	// ErrNoDirectory is returned when a required directory path is not a directory.
	ErrNoDirectory = errors.New("not a directory")
)

// Options represents optional behavior when creating/writing records.
type Options struct {
	// UseGzip determines whether records should be stored as gzip-compressed JSON.
	UseGzip bool
}

// DB represents a database (root directory containing collections).
type DB interface {
	// Collection returns a collection handle for the given name, creating the directory if needed.
	Collection(name string) (Collection, error)
}

// Collection represents a set of records stored as files in a directory.
type Collection interface {
	// Get reads the record with the given id as raw bytes.
	// It reads {id}.json, or if absent, {id}.json.gz (automatically decompressing).
	Get(id string) ([]byte, error)
	// Create creates or overwrites the record with the given id using atomic write semantics.
	// Optional Options may override compression per-call.
	Create(id string, data []byte, opts ...Options) error
	// Delete removes the record (handles either .json or .json.gz if present).
	Delete(id string) error
	// GetAll returns the raw contents of all records in the collection.
	GetAll() [][]byte
	// GetAllByName returns a map of id -> raw record contents.
	GetAllByName() map[string][]byte
	// Len returns the number of files (records) in the collection directory.
	Len() uint64
}
