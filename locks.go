package simplejsondb

import "sync"

// internal structure for managing per-path RWMutexes with reference counts.
type lockEntry struct {
	mu   *sync.RWMutex
	refs int
}

var (
	locksMu sync.Mutex
	locks   = make(map[string]*lockEntry)
)

// Lock acquires a lock for the given path according to the provided LockType.
// Multiple readers are allowed; writers are exclusive. This lock manager is
// process-local and does not use OS-level file locks.
func Lock(path string, t LockType) {
	le := getOrCreateLock(path)
	switch t {
	case LOCK_READ:
		le.mu.RLock()
	case LOCK_WRITE, LOCK_READ_WRITE:
		le.mu.Lock()
	default:
		le.mu.RLock()
	}
}

// Unlock releases a previously acquired lock for the given path and type.
// The lock's reference is decreased and the lock may be garbage-collected
// when no goroutines hold it anymore.
func Unlock(path string, t LockType) {
	locksMu.Lock()
	le, ok := locks[path]
	locksMu.Unlock()
	if !ok || le == nil {
		return
	}

	switch t {
	case LOCK_READ:
		le.mu.RUnlock()
	case LOCK_WRITE, LOCK_READ_WRITE:
		le.mu.Unlock()
	default:
		le.mu.RUnlock()
	}

	// decrease ref and cleanup if zero
	locksMu.Lock()
	le.refs--
	if le.refs == 0 {
		delete(locks, path)
	}
	locksMu.Unlock()
}

func getOrCreateLock(path string) *lockEntry {
	locksMu.Lock()
	defer locksMu.Unlock()
	if le, ok := locks[path]; ok {
		le.refs++
		return le
	}
	le := &lockEntry{mu: &sync.RWMutex{}, refs: 1}
	locks[path] = le
	return le
}
