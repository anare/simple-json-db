package simplejsondb

import (
	"os"
	"path/filepath"
)

// writeAtomic writes data to a temporary file in dir, fsyncs, closes, then atomically renames
// it to the final path. It also attempts to fsync the directory to persist the rename.
func writeAtomic(dir, final string, data []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// ensure cleanup if anything fails
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, final); err != nil {
		return err
	}

	// attempt to fsync the directory to persist rename on disk
	d, err := os.Open(dir)
	if err != nil {
		// best-effort; ignore error
		return nil
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		// ignore dir sync error as optional
		return nil
	}
	_ = d.Close()
	return nil
}

// buildPaths computes directory path and final file path for a given collection path, id, and extension.
func buildPaths(collectionDir, id, ext string) (dir string, final string) {
	dir = collectionDir
	final = filepath.Join(collectionDir, id+ext)
	return
}
