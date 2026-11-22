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
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// plainStore hides the optional extensions of the mock store.
type plainStore struct{ velero.ObjectStore }

func TestGetObjectRange(t *testing.T) {
	mock := NewMockObjectStore("b", "")
	if err := mock.PutObject("b", "k", strings.NewReader("0123456789")); err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]velero.ObjectStore{"native": mock, "fallback": plainStore{mock}} {
		t.Run(name, func(t *testing.T) {
			r, err := GetObjectRange(store, "b", "k", 3, 4)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(r)
			_ = r.Close()
			if string(got) != "3456" {
				t.Fatalf("got %q", got)
			}
			if _, err := GetObjectRange(store, "b", "k", -1, 4); err == nil {
				t.Fatal("negative offset accepted")
			}
			if _, err := GetObjectRange(store, "b", "missing", 0, 1); err == nil {
				t.Fatal("missing object accepted")
			}
		})
	}
	// Reading past the end through the fallback is an error, not silence.
	if _, err := GetObjectRange(plainStore{mock}, "b", "k", 20, 1); err == nil {
		t.Fatal("offset past the end accepted")
	}
}

func TestPutObjectStreamFallsBackToPutObject(t *testing.T) {
	mock := NewMockObjectStore("b", "")
	if err := PutObjectStream(plainStore{mock}, "b", "k", bytes.NewReader([]byte("abc")), 1<<20); err != nil {
		t.Fatal(err)
	}
	got, err := mock.GetObjectBytes("k")
	if err != nil || string(got) != "abc" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestLookupReportsFormatOfLatestCheckpoint(t *testing.T) {
	for _, format := range []string{CheckpointFormatQcow2, CheckpointFormatBlocks} {
		mock := NewMockObjectStore("b", "")
		file := CheckpointFile{Filename: "f", DiskName: "root", ObjectPath: "checkpoints/ns/vm/cp1/f"}
		if err := mock.PutObject("b", file.ObjectPath, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := PutVMIndex(mock, "ns", "vm", "b", VMIndex{Checkpoints: []CheckpointEntry{
			{ID: "cp1", Type: BackupTypeFull, Format: format, Files: []CheckpointFile{file}},
		}}); err != nil {
			t.Fatal(err)
		}
		result, err := LookupLatestCheckpoint(context.Background(), mock, "b", "ns", "vm")
		if err != nil || !result.Found || !result.IsChainValid {
			t.Fatalf("lookup failed: %+v, %v", result, err)
		}
		if result.Format != format {
			t.Fatalf("format %q, want %q", result.Format, format)
		}
	}
}
