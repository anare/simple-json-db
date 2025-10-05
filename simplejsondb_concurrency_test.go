package simplejsondb_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbpkg "github.com/pnkj-kmr/simple-json-db"
)

type counter struct {
	Count int `json:"count"`
}

// writeAtomic writes data to path atomically via temp file + rename.
// For this concurrency test we intentionally skip fsyncs to avoid timing being dominated
// by storage flush costs on some filesystems under the race detector. The goal is to
// measure lock contention across different files, not persistence latency.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}

func TestConcurrentReadWriteSameFile(t *testing.T) {
	base := t.TempDir()
	db, err := dbpkg.New(filepath.Join(base, "db"), &dbpkg.Options{UseGzip: false})
	if err != nil {
		t.Fatal(err)
	}
	coll, err := db.Collection("users")
	if err != nil {
		t.Fatal(err)
	}

	id := "123"
	// initialize
	if err := coll.Create(id, []byte(`{"count":0}`)); err != nil {
		t.Fatal(err)
	}

	finalPath := filepath.Join(base, "db", "users", id+dbpkg.Ext)

	var wg sync.WaitGroup
	n := 200
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// Exclusive lock for the entire read-modify-write cycle
			dbpkg.Lock(finalPath, dbpkg.LOCK_READ_WRITE)
			defer dbpkg.Unlock(finalPath, dbpkg.LOCK_READ_WRITE)

			b, err := os.ReadFile(finalPath)
			if err != nil && !os.IsNotExist(err) {
				t.Errorf("read: %v", err)
				return
			}
			var c counter
			if len(b) > 0 && err == nil {
				if err := json.Unmarshal(b, &c); err != nil {
					t.Errorf("unmarshal: %v", err)
					return
				}
			}
			c.Count++
			out, _ := json.Marshal(&c)
			if err := writeAtomic(finalPath, out); err != nil {
				t.Errorf("writeAtomic: %v", err)
			}
		}()
	}
	wg.Wait()

	b, err := coll.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	var c counter
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Count != n {
		t.Fatalf("expected %d, got %d", n, c.Count)
	}
}

func TestConcurrentDifferentFiles(t *testing.T) {
	base := t.TempDir()
	db, _ := dbpkg.New(filepath.Join(base, "db"), &dbpkg.Options{UseGzip: false})
	coll, _ := db.Collection("users")

	files := 100
	for i := 0; i < files; i++ {
		id := "id" + strconvI(i)
		if err := coll.Create(id, []byte(`{"count":0}`)); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(files)
	for i := 0; i < files; i++ {
		id := "id" + strconvI(i)
		path := filepath.Join(base, "db", "users", id+dbpkg.Ext)
		go func(id, path string) {
			defer wg.Done()
			dbpkg.Lock(path, dbpkg.LOCK_READ_WRITE)
			defer dbpkg.Unlock(path, dbpkg.LOCK_READ_WRITE)
			b, _ := os.ReadFile(path)
			// artificial compute delay
			time.Sleep(20 * time.Millisecond)
			var c counter
			_ = json.Unmarshal(b, &c)
			c.Count++
			out, _ := json.Marshal(&c)
			_ = writeAtomic(path, out)
		}(id, path)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// If operations were serialized globally, we'd expect ~files*20ms.
	// Require it to be significantly less to ensure no unnecessary cross-file blocking.
	if elapsed > 400*time.Millisecond { // 20ms * 20 = 400ms threshold allows scheduling jitter
		t.Fatalf("operations took too long: %v", elapsed)
	}
}

func TestGzipMode(t *testing.T) {
	base := t.TempDir()
	db, _ := dbpkg.New(filepath.Join(base, "db"), &dbpkg.Options{UseGzip: true})
	coll, _ := db.Collection("users")
	id := "g1"

	// Initialize
	if err := coll.Create(id, []byte(`{"count":0}`)); err != nil {
		t.Fatal(err)
	}

	finalPath := filepath.Join(base, "db", "users", id+dbpkg.GZipExt)

	var wg sync.WaitGroup
	total := int32(200)
	wg.Add(int(total))
	for i := 0; i < int(total); i++ {
		go func() {
			defer wg.Done()
			dbpkg.Lock(finalPath, dbpkg.LOCK_READ_WRITE)
			defer dbpkg.Unlock(finalPath, dbpkg.LOCK_READ_WRITE)
			b, _ := os.ReadFile(finalPath)
			ub, _ := dbpkg.UnGzip(b)
			var c counter
			_ = json.Unmarshal(ub, &c)
			c.Count++
			out, _ := json.Marshal(&c)
			gz, _ := dbpkg.Gzip(out)
			_ = writeAtomic(finalPath, gz)
		}()
	}
	wg.Wait()

	b, _ := coll.Get(id)
	var c counter
	_ = json.Unmarshal(b, &c)
	if c.Count != int(total) {
		t.Fatalf("expected %d, got %d", total, c.Count)
	}
}

func TestGetAllAndByName(t *testing.T) {
	base := t.TempDir()
	db, _ := dbpkg.New(filepath.Join(base, "db"), &dbpkg.Options{UseGzip: false})
	coll, _ := db.Collection("users")

	// Seed some files
	files := 50
	for i := 0; i < files; i++ {
		id := "id" + strconvI(i)
		if err := coll.Create(id, []byte(`{"count":0}`)); err != nil {
			t.Fatal(err)
		}
	}

	// Concurrent writers updating different files
	var wg sync.WaitGroup
	wg.Add(files)
	var writes int32
	for i := 0; i < files; i++ {
		id := "id" + strconvI(i)
		path := filepath.Join(base, "db", "users", id+dbpkg.Ext)
		go func(id, path string) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				dbpkg.Lock(path, dbpkg.LOCK_READ_WRITE)
				b, _ := os.ReadFile(path)
				var c counter
				_ = json.Unmarshal(b, &c)
				c.Count++
				out, _ := json.Marshal(&c)
				_ = writeAtomic(path, out)
				dbpkg.Unlock(path, dbpkg.LOCK_READ_WRITE)
				atomic.AddInt32(&writes, 1)
			}
		}(id, path)
	}

	// While writes happen, repeatedly call GetAll and GetAllByName and ensure we can parse all entries
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		all := coll.GetAll()
		for _, b := range all {
			var c counter
			if err := json.Unmarshal(b, &c); err != nil {
				t.Fatalf("invalid json from GetAll: %v", err)
			}
		}
		byName := coll.GetAllByName()
		for _, b := range byName {
			var c counter
			if err := json.Unmarshal(b, &c); err != nil {
				t.Fatalf("invalid json from GetAllByName: %v", err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	// Final sanity check
	byName := coll.GetAllByName()
	if len(byName) != files {
		t.Fatalf("expected %d files, got %d", files, len(byName))
	}
}

// helpers
func strconvI(i int) string { return strconv.Itoa(i) }
