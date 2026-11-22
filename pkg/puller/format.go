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

// Package puller implements the streaming (pull-mode) backup path: it reads a
// disk through the KubeVirt backup export endpoint and stores it as
// individually compressed blocks, without staging the backup on a volume.
//
// # Storage format
//
// Per disk and checkpoint two objects are written next to each other:
//
//	checkpoints/<ns>/<vm>/<checkpoint>/<volume>.blocks      data object
//	checkpoints/<ns>/<vm>/<checkpoint>/<volume>.blocks.idx  block index
//
// The disk is cut into fixed-size blocks (4 MiB by default). The data object
// is the concatenation of one independent zstd frame per stored block, in the
// order the blocks happened to finish. The block index (zstd-compressed JSON)
// records for every block of the checkpoint its number, where its frame is in
// the data object, the SHA-256 of its uncompressed content and whether it is
// all zero (zero blocks have no frame).
//
// A full backup has an index entry for every block of the disk. An
// incremental has entries only for the blocks that overlap a dirty extent.
// Restore merges the indexes from the newest full to the wanted checkpoint
// and reads every block once, from the newest checkpoint that has it.
package puller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

const (
	// IndexVersion is the version of the block index layout.
	IndexVersion = 1

	// DefaultBlockSize is the size of one block of a disk.
	DefaultBlockSize int64 = 4 << 20

	// DataObjectSuffix is appended to the volume name to form the data object name.
	DataObjectSuffix = ".blocks"

	// IndexObjectSuffix is appended to the data object name to form the index object name.
	IndexObjectSuffix = ".idx"

	compressionZstd = "zstd"
	checksumSHA256  = "sha256"

	// maxIndexBytes bounds the decompressed size of a block index.
	maxIndexBytes = 1 << 30
)

// BlockEntry describes one block of a checkpoint.
type BlockEntry struct {
	// N is the block number; the block covers bytes [N*BlockSize, (N+1)*BlockSize).
	N int64 `json:"n"`
	// Offset and Length locate the block's zstd frame in the data object.
	Offset int64 `json:"o,omitempty"`
	Length int64 `json:"l,omitempty"`
	// Checksum is the hex SHA-256 of the uncompressed block.
	Checksum string `json:"s,omitempty"`
	// Zero marks an all-zero block. It has no frame and no checksum.
	Zero bool `json:"z,omitempty"`
}

// BlockIndex is the index of one disk in one checkpoint.
type BlockIndex struct {
	Version int    `json:"version"`
	Format  string `json:"format"`

	VMNamespace string `json:"vmNamespace"`
	VMName      string `json:"vmName"`
	Volume      string `json:"volume"`

	// Checkpoint is the checkpoint this index belongs to; Parent is the
	// checkpoint it is based on (empty for a full backup).
	Checkpoint string `json:"checkpoint"`
	Parent     string `json:"parent,omitempty"`
	// BackupType is "full" or "incremental" for this disk.
	BackupType string `json:"backupType"`

	DiskSize  int64 `json:"diskSize"`
	BlockSize int64 `json:"blockSize"`

	Compression string `json:"compression"`
	Checksum    string `json:"checksum"`

	// DataObject is the object path of the data object, DataSize its length.
	DataObject string `json:"dataObject"`
	DataSize   int64  `json:"dataSize"`

	// Blocks is sorted by block number.
	Blocks []BlockEntry `json:"blocks"`
}

// DataObjectName returns the data object file name for a volume.
func DataObjectName(volume string) string { return volume + DataObjectSuffix }

// IndexObjectName returns the index object file name for a volume.
func IndexObjectName(volume string) string { return volume + DataObjectSuffix + IndexObjectSuffix }

// IndexPathForData returns the index object path belonging to a data object path.
func IndexPathForData(dataObjectPath string) string { return dataObjectPath + IndexObjectSuffix }

// IsFull reports whether the index is a full backup of its disk.
func (ix *BlockIndex) IsFull() bool { return strings.EqualFold(ix.BackupType, uploader.BackupTypeFull) }

// BlockCount returns the number of blocks the disk is divided into.
func (ix *BlockIndex) BlockCount() int64 { return blockCount(ix.DiskSize, ix.BlockSize) }

// BlockLen returns the length in bytes of block n; only the last block can be short.
func (ix *BlockIndex) BlockLen(n int64) int64 { return blockLen(ix.DiskSize, ix.BlockSize, n) }

func blockCount(diskSize, blockSize int64) int64 {
	return (diskSize + blockSize - 1) / blockSize
}

func blockLen(diskSize, blockSize, n int64) int64 {
	return min(blockSize, diskSize-n*blockSize)
}

// Validate checks the internal consistency of an index.
func (ix *BlockIndex) Validate() error {
	if ix.Version != IndexVersion {
		return fmt.Errorf("unsupported block index version %d", ix.Version)
	}
	if ix.Format != uploader.CheckpointFormatBlocks {
		return fmt.Errorf("unsupported block index format %q", ix.Format)
	}
	if ix.Compression != compressionZstd || ix.Checksum != checksumSHA256 {
		return fmt.Errorf("unsupported compression %q or checksum %q", ix.Compression, ix.Checksum)
	}
	if ix.BlockSize <= 0 || ix.DiskSize <= 0 {
		return fmt.Errorf("invalid disk size %d or block size %d", ix.DiskSize, ix.BlockSize)
	}
	if ix.DataSize < 0 {
		return fmt.Errorf("invalid data size %d", ix.DataSize)
	}
	count := ix.BlockCount()
	prev := int64(-1)
	var stored int64
	for i := range ix.Blocks {
		b := &ix.Blocks[i]
		if b.N <= prev || b.N >= count {
			return fmt.Errorf("block entry %d has number %d (previous %d, block count %d)", i, b.N, prev, count)
		}
		prev = b.N
		if b.Zero {
			if b.Length != 0 || b.Offset != 0 || b.Checksum != "" {
				return fmt.Errorf("zero block %d carries data fields", b.N)
			}
			continue
		}
		if b.Length <= 0 || b.Offset < 0 || b.Offset+b.Length > ix.DataSize {
			return fmt.Errorf("block %d frame %d+%d is outside the data object (%d bytes)", b.N, b.Offset, b.Length, ix.DataSize)
		}
		if len(b.Checksum) != 64 {
			return fmt.Errorf("block %d has no valid checksum", b.N)
		}
		stored += b.Length
	}
	if stored != ix.DataSize {
		return fmt.Errorf("block frames add up to %d bytes but the data object has %d", stored, ix.DataSize)
	}
	if ix.IsFull() && int64(len(ix.Blocks)) != count {
		return fmt.Errorf("full backup index has %d of %d blocks", len(ix.Blocks), count)
	}
	return nil
}

// EncodeIndex serializes an index as zstd-compressed JSON.
func EncodeIndex(ix *BlockIndex) ([]byte, error) {
	sort.Slice(ix.Blocks, func(i, j int) bool { return ix.Blocks[i].N < ix.Blocks[j].N })
	if err := ix.Validate(); err != nil {
		return nil, fmt.Errorf("refusing to write an inconsistent block index: %w", err)
	}
	raw, err := json.Marshal(ix)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block index: %w", err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = enc.Close() }()
	return enc.EncodeAll(raw, nil), nil
}

// DecodeIndex parses and validates an index written by EncodeIndex.
func DecodeIndex(data []byte) (*BlockIndex, error) {
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open block index: %w", err)
	}
	defer dec.Close()
	raw, err := io.ReadAll(io.LimitReader(dec, maxIndexBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to decompress block index: %w", err)
	}
	if len(raw) > maxIndexBytes {
		return nil, fmt.Errorf("block index is larger than %d bytes", maxIndexBytes)
	}
	ix := &BlockIndex{}
	if err := json.Unmarshal(raw, ix); err != nil {
		return nil, fmt.Errorf("failed to parse block index: %w", err)
	}
	if err := ix.Validate(); err != nil {
		return nil, err
	}
	return ix, nil
}

// LoadIndex reads and validates a block index object.
func LoadIndex(store velero.ObjectStore, bucket, indexPath string) (*BlockIndex, error) {
	data, err := uploader.GetObjectBytes(store, bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read block index %s: %w", indexPath, err)
	}
	ix, err := DecodeIndex(data)
	if err != nil {
		return nil, fmt.Errorf("block index %s: %w", indexPath, err)
	}
	if IndexPathForData(ix.DataObject) != indexPath {
		return nil, fmt.Errorf("block index %s points at data object %s outside its checkpoint", indexPath, ix.DataObject)
	}
	return ix, nil
}

// MergedBlock is one block of a restored disk and where to read it from.
type MergedBlock struct {
	// Source is the position in the chain of the index that holds the block.
	Source int
	Entry  BlockEntry
}

// MergedIndex maps every block of a disk to the newest checkpoint that has it.
type MergedIndex struct {
	DiskSize  int64
	BlockSize int64
	// Blocks has one element per block of the disk, in block order.
	Blocks []MergedBlock
}

// MergeIndexes merges the indexes of one disk along a checkpoint chain given
// oldest first. The newest index wins for every block it contains. The merge
// starts at the newest full backup in the chain, so older entries are ignored.
func MergeIndexes(chain []*BlockIndex) (*MergedIndex, error) {
	if len(chain) == 0 {
		return nil, fmt.Errorf("no block indexes to merge")
	}
	newest := chain[len(chain)-1]
	start := -1
	for i := len(chain) - 1; i >= 0; i-- {
		ix := chain[i]
		if ix.BlockSize != newest.BlockSize {
			return nil, fmt.Errorf("checkpoint %s uses block size %d, checkpoint %s uses %d",
				ix.Checkpoint, ix.BlockSize, newest.Checkpoint, newest.BlockSize)
		}
		if ix.DiskSize != newest.DiskSize {
			return nil, fmt.Errorf("checkpoint %s has disk size %d, checkpoint %s has %d",
				ix.Checkpoint, ix.DiskSize, newest.Checkpoint, newest.DiskSize)
		}
		if ix.Volume != newest.Volume {
			return nil, fmt.Errorf("checkpoint %s is for volume %q, checkpoint %s for %q",
				ix.Checkpoint, ix.Volume, newest.Checkpoint, newest.Volume)
		}
		if i < len(chain)-1 && chain[i+1].Parent != ix.Checkpoint {
			return nil, fmt.Errorf("checkpoint %s has parent %q, want %q",
				chain[i+1].Checkpoint, chain[i+1].Parent, ix.Checkpoint)
		}
		if ix.IsFull() {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("checkpoint chain for volume %q contains no full backup", newest.Volume)
	}

	merged := &MergedIndex{
		DiskSize:  newest.DiskSize,
		BlockSize: newest.BlockSize,
		Blocks:    make([]MergedBlock, newest.BlockCount()),
	}
	covered := make([]bool, len(merged.Blocks))
	for i := start; i < len(chain); i++ {
		for _, b := range chain[i].Blocks {
			merged.Blocks[b.N] = MergedBlock{Source: i, Entry: b}
			covered[b.N] = true
		}
	}
	for n, ok := range covered {
		if !ok {
			return nil, fmt.Errorf("block %d of volume %q is in no checkpoint of the chain", n, newest.Volume)
		}
	}
	return merged, nil
}
