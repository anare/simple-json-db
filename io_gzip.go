package simplejsondb

import (
	"bytes"
	"compress/gzip"
	"io"
)

// UnGzip decompresses gzip-compressed data.
func UnGzip(record []byte) ([]byte, error) {
	var buffer bytes.Buffer
	if _, err := buffer.Write(record); err != nil {
		return record, err
	}
	reader, err := gzip.NewReader(&buffer)
	if err != nil {
		return record, err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		_ = reader.Close()
		return record, err
	}
	if err := reader.Close(); err != nil {
		return record, err
	}
	return data, nil
}

// Gzip compresses data using gzip.
func Gzip(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return data, err
	}
	if err := writer.Close(); err != nil {
		return data, err
	}
	return buffer.Bytes(), nil
}
