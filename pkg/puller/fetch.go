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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"golang.org/x/sync/errgroup"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

const (
	// DefaultFetchWorkers is the number of parallel ranged downloads.
	DefaultFetchWorkers = 4

	// maxFetchBytes is the largest range of a data object fetched with one request.
	maxFetchBytes = 32 << 20
)

// fetchBlock is one stored block to download from a data object.
type fetchBlock struct {
	object string
	entry  BlockEntry
	// rawLen is the expected length of the block once decompressed.
	rawLen int64
}

// fetchRange is a contiguous part of one data object holding several frames.
type fetchRange struct {
	object string
	offset int64
	length int64
	blocks []fetchBlock
}

// planFetch groups blocks into ranged requests: frames that are adjacent in
// the same data object are fetched together, up to maxBytes per request.
func planFetch(blocks []fetchBlock, maxBytes int64) []fetchRange {
	sorted := append([]fetchBlock(nil), blocks...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].object != sorted[j].object {
			return sorted[i].object < sorted[j].object
		}
		return sorted[i].entry.Offset < sorted[j].entry.Offset
	})
	var out []fetchRange
	for _, b := range sorted {
		if l := len(out); l > 0 {
			cur := &out[l-1]
			if cur.object == b.object && cur.offset+cur.length == b.entry.Offset && cur.length+b.entry.Length <= maxBytes {
				cur.length += b.entry.Length
				cur.blocks = append(cur.blocks, b)
				continue
			}
		}
		out = append(out, fetchRange{object: b.object, offset: b.entry.Offset, length: b.entry.Length, blocks: []fetchBlock{b}})
	}
	return out
}

// fetchBlocks downloads the given blocks with ranged reads, decompresses
// each one, verifies its length and checksum and passes it to handle.
// handle is called from several goroutines and must not keep data.
func fetchBlocks(
	ctx context.Context, store velero.ObjectStore, bucket string, blocks []fetchBlock, workers int,
	handle func(b fetchBlock, data []byte) error,
) error {
	if workers <= 0 {
		workers = DefaultFetchWorkers
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	ranges := make(chan fetchRange)
	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(ranges)
		for _, r := range planFetch(blocks, maxFetchBytes) {
			select {
			case ranges <- r:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for range workers {
		group.Go(func() error {
			var compressed, raw []byte
			for r := range ranges {
				if err := gctx.Err(); err != nil {
					return err
				}
				if int64(cap(compressed)) < r.length {
					compressed = make([]byte, r.length)
				}
				compressed = compressed[:r.length]
				if err := readRange(store, bucket, r, compressed); err != nil {
					return err
				}
				var pos int64
				for _, b := range r.blocks {
					frame := compressed[pos : pos+b.entry.Length]
					pos += b.entry.Length
					raw, err = decoder.DecodeAll(frame, raw[:0])
					if err != nil {
						return fmt.Errorf("block %d in %s does not decompress: %w", b.entry.N, b.object, err)
					}
					if int64(len(raw)) != b.rawLen {
						return fmt.Errorf("block %d in %s has %d bytes, want %d", b.entry.N, b.object, len(raw), b.rawLen)
					}
					sum := sha256.Sum256(raw)
					if hex.EncodeToString(sum[:]) != b.entry.Checksum {
						return fmt.Errorf("block %d in %s fails its checksum", b.entry.N, b.object)
					}
					if err := handle(b, raw); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return group.Wait()
}

// readRange reads exactly one fetchRange into buf.
func readRange(store velero.ObjectStore, bucket string, r fetchRange, buf []byte) error {
	reader, err := uploader.GetObjectRange(store, bucket, r.object, r.offset, r.length)
	if err != nil {
		return fmt.Errorf("failed to read %d bytes at %d of %s: %w", r.length, r.offset, r.object, err)
	}
	_, readErr := io.ReadFull(reader, buf)
	closeErr := reader.Close()
	if readErr != nil {
		return fmt.Errorf("short read of %d bytes at %d of %s: %w", r.length, r.offset, r.object, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to finish reading %s: %w", r.object, closeErr)
	}
	return nil
}
