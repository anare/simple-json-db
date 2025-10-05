// Package simplejsondb provides a simple JSON file-backed store.
//
// NOTE: Implementation is split across multiple files:
//   - types.go (public types)
//   - locks.go (global per-path lock manager)
//   - atomic_write.go (atomic write utilities)
//   - io_gzip.go (gzip helpers)
//   - db.go and collection.go (DB and Collection implementations)
package simplejsondb
