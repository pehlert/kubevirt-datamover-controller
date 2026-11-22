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

package puller

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/go-logr/logr"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// RestoreOptions tunes RestoreChain.
type RestoreOptions struct {
	// Workers is the number of parallel ranged downloads.
	Workers int
	// TargetIsBlockDevice selects block device handling for the target.
	TargetIsBlockDevice bool
}

// RestoreChain rebuilds one disk from a chain of block-format checkpoints.
// dataObjects are the data object paths of the disk along the chain, oldest
// first. Every needed block is read once, from the newest checkpoint that
// has it, verified against its checksum and written at its place in
// targetPath. Zero blocks are not written: a file target is created sparse,
// a block device target is zeroed by discarding the range.
func RestoreChain(
	ctx context.Context, store velero.ObjectStore, bucket string, dataObjects []string, targetPath string,
	opts RestoreOptions, logger logr.Logger,
) error {
	chain := make([]*BlockIndex, 0, len(dataObjects))
	for _, object := range dataObjects {
		ix, err := LoadIndex(store, bucket, IndexPathForData(object))
		if err != nil {
			return err
		}
		chain = append(chain, ix)
	}
	merged, err := MergeIndexes(chain)
	if err != nil {
		return fmt.Errorf("failed to merge block indexes: %w", err)
	}

	flags := os.O_WRONLY
	if !opts.TargetIsBlockDevice {
		flags |= os.O_CREATE
	}
	target, err := os.OpenFile(targetPath, flags, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open restore target %s: %w", targetPath, err)
	}
	defer func() { _ = target.Close() }()

	if opts.TargetIsBlockDevice {
		size, err := target.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to determine the size of %s: %w", targetPath, err)
		}
		if size < merged.DiskSize {
			return fmt.Errorf("restore target %s has %d bytes, the disk needs %d", targetPath, size, merged.DiskSize)
		}
	} else if err := target.Truncate(merged.DiskSize); err != nil {
		return fmt.Errorf("failed to size restore target %s: %w", targetPath, err)
	}

	var fetch []fetchBlock
	var zeroBlocks, dataBlocks int64
	// Zero blocks: coalesce runs so each run is one call.
	runStart, runLen := int64(-1), int64(0)
	flushZero := func() error {
		if runLen == 0 {
			return nil
		}
		err := zeroRange(target, runStart, runLen, opts.TargetIsBlockDevice)
		runStart, runLen = -1, 0
		return err
	}
	for n, b := range merged.Blocks {
		off := int64(n) * merged.BlockSize
		length := blockLen(merged.DiskSize, merged.BlockSize, int64(n))
		if b.Entry.Zero {
			zeroBlocks++
			if runLen > 0 && runStart+runLen == off {
				runLen += length
				continue
			}
			if err := flushZero(); err != nil {
				return err
			}
			runStart, runLen = off, length
			continue
		}
		dataBlocks++
		fetch = append(fetch, fetchBlock{object: chain[b.Source].DataObject, entry: b.Entry, rawLen: length})
	}
	if err := flushZero(); err != nil {
		return err
	}

	logger.Info("Restoring disk from block checkpoints",
		"checkpoints", len(chain), "diskSize", merged.DiskSize, "dataBlocks", dataBlocks, "zeroBlocks", zeroBlocks)

	var written atomic.Int64
	err = fetchBlocks(ctx, store, bucket, fetch, opts.Workers, func(b fetchBlock, data []byte) error {
		if _, err := target.WriteAt(data, b.entry.N*merged.BlockSize); err != nil {
			return fmt.Errorf("failed to write block %d to %s: %w", b.entry.N, targetPath, err)
		}
		written.Add(int64(len(data)))
		return nil
	})
	if err != nil {
		return err
	}
	if err := target.Sync(); err != nil {
		return fmt.Errorf("failed to sync restore target %s: %w", targetPath, err)
	}
	logger.Info("Disk restored", "bytesWritten", written.Load(), "target", targetPath)
	return nil
}

// zeroRange makes [offset, offset+length) of the target read as zeros. A
// freshly sized file already does. A block device may hold old data, so the
// range is deallocated, or overwritten with zeros where that is unsupported.
func zeroRange(target *os.File, offset, length int64, blockDevice bool) error {
	if !blockDevice {
		return nil
	}
	if err := punchHole(target, offset, length); err == nil {
		return nil
	}
	zeros := make([]byte, min(length, DefaultBlockSize))
	for done := int64(0); done < length; {
		n := min(int64(len(zeros)), length-done)
		if _, err := target.WriteAt(zeros[:n], offset+done); err != nil {
			return fmt.Errorf("failed to zero %d bytes at %d: %w", length, offset, err)
		}
		done += n
	}
	return nil
}
