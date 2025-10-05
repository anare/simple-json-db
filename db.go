package simplejsondb

import (
	"os"
	"path/filepath"
)

// db is a filesystem-backed database.
type db struct {
	useGzip bool
	path    string
}

// New creates a DB by ensuring the root directory exists.
func New(dbname string, options *Options) (DB, error) {
	opts := Options{}
	if options != nil {
		opts = *options
	}
	dbpath := filepath.Join(dbname)
	if err := os.MkdirAll(dbpath, 0o755); err != nil {
		return nil, err
	}
	return &db{path: dbpath, useGzip: opts.UseGzip}, nil
}

// Collection returns a collection handle for the given name, ensuring the directory exists.
func (db *db) Collection(name string) (Collection, error) {
	c := filepath.Join(db.path, name)
	if err := os.MkdirAll(c, 0o755); err != nil {
		return nil, err
	}
	fi, err := os.Stat(c)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, ErrNoDirectory
	}
	return &collection{name: name, path: c, useGzip: db.useGzip}, nil
}
