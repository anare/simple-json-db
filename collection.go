package simplejsondb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// collection represents a directory containing JSON records.
type collection struct {
	useGzip bool
	name    string
	path    string
}

// GetAll returns all record contents in the collection.
// Files are read one-by-one; gzip files are decompressed.
func (c *collection) GetAll() (data [][]byte) {
	entries, err := os.ReadDir(c.path)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, Ext) && !strings.HasSuffix(name, GZipExt) {
			// skip temp or unrelated files
			continue
		}
		fp := filepath.Join(c.path, name)
		b, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		if strings.HasSuffix(name, GZipExt) {
			if ub, err := UnGzip(b); err == nil {
				b = ub
			} else {
				continue
			}
		}
		data = append(data, b)
	}
	return data
}

// GetAllByName returns map of id -> raw content.
func (c *collection) GetAllByName() map[string][]byte {
	res := make(map[string][]byte)
	entries, err := os.ReadDir(c.path)
	if err != nil {
		return res
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, Ext) && !strings.HasSuffix(name, GZipExt) {
			continue
		}
		fp := filepath.Join(c.path, name)
		b, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		if strings.HasSuffix(name, GZipExt) {
			if ub, err := UnGzip(b); err == nil {
				b = ub
			} else {
				continue
			}
			name = strings.TrimSuffix(name, GZipExt)
		} else {
			name = strings.TrimSuffix(name, Ext)
		}
		res[name] = b
	}
	return res
}

// filePathCandidates returns potential file paths for the id in preferred order (.json then .json.gz)
func (c *collection) filePathCandidates(id string) (jsonPath, gzPath string) {
	return filepath.Join(c.path, id+Ext), filepath.Join(c.path, id+GZipExt)
}

// Get reads a record by id with a read lock for whichever file exists.
func (c *collection) Get(id string) ([]byte, error) {
	jsonPath, gzPath := c.filePathCandidates(id)

	// prefer json if exists
	if b, err := c.readFileWithLock(jsonPath, false); err == nil {
		return b, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// fallback to gzip
	b, err := c.readFileWithLock(gzPath, true)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (c *collection) readFileWithLock(path string, gz bool) ([]byte, error) {
	// Take a read lock on this specific path
	Lock(path, LOCK_READ)
	defer Unlock(path, LOCK_READ)

	// stat to provide nice os.ErrNotExist if absent
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if gz {
		ub, err := UnGzip(b)
		if err != nil {
			return nil, err
		}
		b = ub
	}
	return b, nil
}

// Create writes the record atomically. It uses gzip depending on collection default
// or per-call Options override. Writers are exclusive per-file.
func (c *collection) Create(id string, data []byte, opts ...Options) error {
	useGzip := c.useGzip
	if len(opts) > 0 {
		useGzip = opts[0].UseGzip
	}
	ext := Ext
	if useGzip {
		var err error
		data, err = Gzip(data)
		if err != nil {
			return err
		}
		ext = GZipExt
	}

	dir, final := buildPaths(c.path, id, ext)
	Lock(final, LOCK_WRITE)
	defer Unlock(final, LOCK_WRITE)

	// ensure the other format (if exists) is removed to avoid ambiguity
	other := filepath.Join(c.path, id+map[bool]string{true: Ext, false: GZipExt}[useGzip])
	// if we are writing gzip, other is .json; if not, other is .json.gz
	if useGzip {
		other = filepath.Join(c.path, id+Ext)
	} else {
		other = filepath.Join(c.path, id+GZipExt)
	}
	_ = os.Remove(other)

	return writeAtomic(dir, final, data)
}

// Delete removes the record file if present (either .json or .json.gz), with a write lock.
func (c *collection) Delete(id string) error {
	jsonPath, gzPath := c.filePathCandidates(id)

	// Try json first
	Lock(jsonPath, LOCK_WRITE)
	if _, err := os.Stat(jsonPath); err == nil {
		defer Unlock(jsonPath, LOCK_WRITE)
		return os.Remove(jsonPath)
	}
	Unlock(jsonPath, LOCK_WRITE)

	// Then gzip
	Lock(gzPath, LOCK_WRITE)
	defer Unlock(gzPath, LOCK_WRITE)
	if _, err := os.Stat(gzPath); err != nil {
		return err
	}
	return os.Remove(gzPath)
}

// Len returns the number of directory entries (files) in the collection.
func (c *collection) Len() uint64 {
	entries, err := os.ReadDir(c.path)
	if err != nil {
		return 0
	}
	var cnt uint64
	for _, e := range entries {
		if !e.IsDir() {
			cnt++
		}
	}
	return cnt
}
