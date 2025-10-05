package simplejsondb_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	dbpkg "github.com/pnkj-kmr/simple-json-db"
)

type exCounter struct {
	Count int `json:"count"`
}

// testWriteAtomic mirrors the library's atomic write semantics but is scoped to this example.
func testWriteAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*.json")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func Example() {
	base := "./_exampledb"
	db, _ := dbpkg.New(filepath.Join(base), &dbpkg.Options{UseGzip: false})
	coll, _ := db.Collection("users")
	id := "123"

	// Initialize
	_ = coll.Create(id, []byte(`{"count":0}`))

	// Concurrent increment
	var wg sync.WaitGroup
	wg.Add(10)
	finalPath := filepath.Join(base, "users", id+dbpkg.Ext)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			dbpkg.Lock(finalPath, dbpkg.LOCK_READ_WRITE)
			defer dbpkg.Unlock(finalPath, dbpkg.LOCK_READ_WRITE)
			// Avoid nested locking: perform direct file IO under the external lock
			b, _ := os.ReadFile(finalPath)
			var c exCounter
			_ = json.Unmarshal(b, &c)
			c.Count++
			out, _ := json.Marshal(&c)
			_ = testWriteAtomic(finalPath, out)
		}()
	}
	wg.Wait()

	// Read final result
	b, _ := coll.Get(id)
	var c exCounter
	_ = json.Unmarshal(b, &c)
	fmt.Printf("final=%d\n", c.Count)

	// Delete
	_ = coll.Delete(id)

	// Output:
	// final=10
}
