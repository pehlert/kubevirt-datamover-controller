/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uploader

import (
	"fmt"
	"io"

	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// RangeReader is implemented by object stores that can read a byte range of
// an object without downloading all of it. velero.ObjectStore has no such
// call, so it is an optional extension.
type RangeReader interface {
	// GetObjectRange returns length bytes of the object starting at offset.
	GetObjectRange(bucket, key string, offset, length int64) (io.ReadCloser, error)
}

// StreamUploader is implemented by object stores that can upload a stream of
// unknown length with a caller-chosen multipart part size. The part size
// bounds the largest object that fits into the store's part count limit, so
// a caller that knows an upper bound for the stream can size parts for it.
type StreamUploader interface {
	// PutObjectStream uploads body to key. partSize <= 0 selects the default.
	PutObjectStream(bucket, key string, body io.Reader, partSize int64) error
}

// PutObjectStream uploads a stream through StreamUploader when the store
// implements it and through plain PutObject otherwise.
func PutObjectStream(store velero.ObjectStore, bucket, key string, body io.Reader, partSize int64) error {
	if s, ok := store.(StreamUploader); ok {
		return s.PutObjectStream(bucket, key, body, partSize)
	}
	return store.PutObject(bucket, key, body)
}

// GetObjectRange reads length bytes at offset from an object. Stores without
// native range support are read from the start and the leading bytes are
// discarded, which is correct but slow.
func GetObjectRange(store velero.ObjectStore, bucket, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range offset=%d length=%d for object %s", offset, length, key)
	}
	if r, ok := store.(RangeReader); ok {
		return r.GetObjectRange(bucket, key, offset, length)
	}
	reader, err := store.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, reader, offset); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to seek to offset %d in object %s: %w", offset, key, err)
	}
	return &limitedReadCloser{Reader: io.LimitReader(reader, length), closer: reader}, nil
}

type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func (l *limitedReadCloser) Close() error { return l.closer.Close() }
