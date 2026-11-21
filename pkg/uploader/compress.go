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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/go-logr/logr"
)

// compressedSuffix marks the temporary rewritten copy of a full backup image.
// It deliberately does not end in ".qcow2" so the upload walk never picks it up.
const compressedSuffix = ".compressed.tmp"

// qemuImgInfo is the subset of `qemu-img info --output=json` used here.
type qemuImgInfo struct {
	Format          string `json:"format"`
	BackingFilename string `json:"backing-filename"`
}

// qemuImgRun runs qemu-img and returns its combined output. It is a variable
// so tests can replace it.
var qemuImgRun = func(args ...string) ([]byte, error) {
	return exec.Command("qemu-img", args...).CombinedOutput() //nolint:gosec // fixed binary, arguments built by the uploader
}

// imageHasBackingFile reports whether a qcow2 image references a backing file.
// Incremental backups do (the VM's CBT overlay); full backups do not.
func imageHasBackingFile(path string) (bool, error) {
	out, err := qemuImgRun("info", "--output=json", path)
	if err != nil {
		return false, fmt.Errorf("qemu-img info failed: %w: %s", err, string(out))
	}
	var info qemuImgInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return false, fmt.Errorf("failed to parse qemu-img info output: %w", err)
	}
	return info.BackingFilename != "", nil
}

// compressFullImage rewrites a full backup image as a sparse, zstd-compressed
// qcow2 next to the original and returns the path and size of the copy. Images
// with a backing file (incrementals) are left alone: it returns "" and the
// caller uploads the original. The caller removes the returned file.
func compressFullImage(path string, logger logr.Logger) (string, int64, error) {
	hasBacking, err := imageHasBackingFile(path)
	if err != nil {
		return "", 0, err
	}
	if hasBacking {
		logger.Info("Image has a backing file, uploading as is", "path", path)
		return "", 0, nil
	}

	target := path + compressedSuffix
	_ = os.Remove(target)

	start := time.Now()
	out, err := qemuImgRun("convert", "-c", "-o", "compression_type=zstd", "-O", "qcow2", path, target)
	if err != nil {
		_ = os.Remove(target)
		return "", 0, fmt.Errorf("qemu-img convert failed: %w: %s", err, string(out))
	}

	info, err := os.Stat(target)
	if err != nil {
		_ = os.Remove(target)
		return "", 0, fmt.Errorf("failed to stat compressed image: %w", err)
	}

	logger.Info("Compressed full backup image", "path", path, "compressedSize", info.Size(),
		"duration", time.Since(start).Round(time.Second).String())
	return target, info.Size(), nil
}
