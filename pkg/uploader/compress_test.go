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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

// fakeQemuImg replaces qemuImgRun for the duration of a test. info is the JSON
// returned for "info"; convert writes convertData to the target path.
func fakeQemuImg(t *testing.T, info string, infoErr error, convertData []byte, convertErr error) *[][]string {
	t.Helper()
	var calls [][]string
	orig := qemuImgRun
	qemuImgRun = func(args ...string) ([]byte, error) {
		calls = append(calls, args)
		switch args[0] {
		case "info":
			return []byte(info), infoErr
		case "convert":
			if convertErr != nil {
				return []byte("boom"), convertErr
			}
			return nil, os.WriteFile(args[len(args)-1], convertData, 0o600)
		}
		return nil, errors.New("unexpected qemu-img call")
	}
	t.Cleanup(func() { qemuImgRun = orig })
	return &calls
}

func writeTestImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vmb-root.qcow2")
	if err := os.WriteFile(path, []byte("original image data"), 0o600); err != nil {
		t.Fatalf("failed to write test image: %v", err)
	}
	return path
}

func TestCompressFullImage(t *testing.T) {
	path := writeTestImage(t)
	calls := fakeQemuImg(t, `{"format":"qcow2","virtual-size":1024}`, nil, []byte("small"), nil)

	target, size, err := compressFullImage(path, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != path+compressedSuffix {
		t.Errorf("target = %q, want %q", target, path+compressedSuffix)
	}
	if size != int64(len("small")) {
		t.Errorf("size = %d, want %d", size, len("small"))
	}
	if strings.HasSuffix(strings.ToLower(target), ".qcow2") {
		t.Errorf("temporary copy %q must not look like a qcow2 file to the upload walk", target)
	}
	if len(*calls) != 2 || (*calls)[1][0] != "convert" {
		t.Fatalf("expected info then convert, got %v", *calls)
	}
	convert := strings.Join((*calls)[1], " ")
	for _, want := range []string{"-c", "compression_type=zstd", "-O qcow2", path} {
		if !strings.Contains(convert, want) {
			t.Errorf("convert args %q missing %q", convert, want)
		}
	}
}

func TestCompressFullImageSkipsIncremental(t *testing.T) {
	path := writeTestImage(t)
	calls := fakeQemuImg(t, `{"format":"qcow2","backing-filename":"/var/run/overlay.qcow2"}`, nil, nil, nil)

	target, size, err := compressFullImage(path, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != "" || size != 0 {
		t.Errorf("got (%q, %d), want an image with a backing file to be left alone", target, size)
	}
	if len(*calls) != 1 {
		t.Errorf("convert must not run for an image with a backing file, calls: %v", *calls)
	}
}

func TestCompressFullImageConvertFailureLeavesNoCopy(t *testing.T) {
	path := writeTestImage(t)
	fakeQemuImg(t, `{"format":"qcow2"}`, nil, nil, errors.New("exit status 1"))

	target, _, err := compressFullImage(path, logr.Discard())
	if err == nil || !strings.Contains(err.Error(), "qemu-img convert failed") {
		t.Fatalf("error = %v, want a convert failure", err)
	}
	if target != "" {
		t.Errorf("target = %q, want empty on failure", target)
	}
	if _, statErr := os.Stat(path + compressedSuffix); !os.IsNotExist(statErr) {
		t.Errorf("temporary copy must be removed after a failed convert")
	}
}

func TestCompressFullImageInfoErrors(t *testing.T) {
	path := writeTestImage(t)

	fakeQemuImg(t, "", errors.New("exit status 1"), nil, nil)
	if _, _, err := compressFullImage(path, logr.Discard()); err == nil || !strings.Contains(err.Error(), "qemu-img info failed") {
		t.Fatalf("error = %v, want an info failure", err)
	}

	fakeQemuImg(t, "not json", nil, nil, nil)
	if _, _, err := compressFullImage(path, logr.Discard()); err == nil || !strings.Contains(err.Error(), "parse qemu-img info") {
		t.Fatalf("error = %v, want a parse failure", err)
	}
}
