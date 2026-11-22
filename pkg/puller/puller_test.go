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
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

const (
	testBlock  = int64(4096)
	testBucket = "bucket"
)

// fakeDisk serves ranged reads from memory and can be told to misbehave.
type fakeDisk struct {
	mu       sync.Mutex
	data     []byte
	requests int
	// failAt maps a request number (1-based) to a failure mode.
	failAt map[int]string
	// always, when set, makes every request fail that way.
	always string
}

func (d *fakeDisk) Data(_ context.Context, offset, length int64) (io.ReadCloser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.requests++
	mode := d.always
	if m, ok := d.failAt[d.requests]; ok {
		mode = m
	}
	end := min(offset+length, int64(len(d.data)))
	chunk := d.data[offset:end]
	switch mode {
	case "error":
		return nil, errors.New("connection refused")
	case "short":
		// Connection cut in the middle of the second block of the request.
		cut := min(int64(len(chunk)), testBlock+testBlock/2)
		return io.NopCloser(io.MultiReader(bytes.NewReader(chunk[:cut]), errReader{})), nil
	case "truncated":
		// Clean EOF before the requested length: no error, just fewer bytes.
		return io.NopCloser(bytes.NewReader(chunk[:len(chunk)/2])), nil
	case "extra":
		return io.NopCloser(io.MultiReader(bytes.NewReader(chunk), bytes.NewReader([]byte{1}))), nil
	}
	return io.NopCloser(bytes.NewReader(chunk)), nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// makeDisk builds a disk of the given number of blocks. Blocks listed in
// zero stay zero, the others get random content. tail adds a short last block.
func makeDisk(t *testing.T, blocks int, zero map[int]bool, tail int) []byte {
	t.Helper()
	data := make([]byte, int64(blocks)*testBlock+int64(tail))
	for n := range blocks {
		if !zero[n] {
			_, err := rand.Read(data[int64(n)*testBlock : int64(n+1)*testBlock])
			require.NoError(t, err)
		}
	}
	if tail > 0 {
		_, err := rand.Read(data[int64(blocks)*testBlock:])
		require.NoError(t, err)
	}
	return data
}

func testOptions() TransferOptions {
	return TransferOptions{Readers: 3, RunBlocks: 4, Retries: 3, RetryDelay: time.Millisecond, IdleTimeout: time.Second}
}

// backup runs a transfer of disk into store and writes the block index, the
// way the pull subcommand does for one volume.
func backup(
	t *testing.T, store *uploader.MockObjectStore, disk DataSource, extents []Extent, incremental bool,
	checkpoint, parent string,
) *BlockIndex {
	t.Helper()
	plan, err := PlanBlocks(extents, testBlock, incremental)
	require.NoError(t, err)
	dataPath := uploader.GetQCOWPath("ns", "vm", checkpoint, DataObjectName("root"))
	entries, size, err := TransferVolume(context.Background(), disk, plan, func(r io.Reader) error {
		return store.PutObject(testBucket, dataPath, r)
	}, testOptions())
	require.NoError(t, err)
	ix := &BlockIndex{
		Version: IndexVersion, Format: uploader.CheckpointFormatBlocks, VMNamespace: "ns", VMName: "vm", Volume: "root",
		Checkpoint: checkpoint, Parent: parent, BackupType: uploader.BackupTypeFull,
		DiskSize: plan.DiskSize, BlockSize: plan.BlockSize, Compression: compressionZstd, Checksum: checksumSHA256,
		DataObject: dataPath, DataSize: size, Blocks: entries,
	}
	if incremental {
		ix.BackupType = uploader.BackupTypeIncremental
	}
	encoded, err := EncodeIndex(ix)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(testBucket, IndexPathForData(dataPath), bytes.NewReader(encoded)))
	return ix
}

func restore(t *testing.T, store *uploader.MockObjectStore, indexes ...*BlockIndex) []byte {
	t.Helper()
	objects := make([]string, 0, len(indexes))
	for _, ix := range indexes {
		objects = append(objects, ix.DataObject)
	}
	target := filepath.Join(t.TempDir(), "disk.img")
	require.NoError(t, RestoreChain(context.Background(), store, testBucket, objects, target, RestoreOptions{Workers: 3}, logr.Discard()))
	out, err := os.ReadFile(target)
	require.NoError(t, err)
	return out
}

func fullExtents(size int64) []Extent { return []Extent{{Offset: 0, Length: size, Type: 0}} }

func TestReadMapPaging(t *testing.T) {
	pages := map[int64]string{
		0:    `{"extents":[{"offset":0,"length":100,"type":0,"description":"data"},{"offset":100,"length":50,"type":3}],"next_offset":150}`,
		150:  `{"extents":[{"offset":150,"length":850,"type":2}],"next_offset":1000}`,
		1000: `{"extents":[{"offset":1000,"length":24,"type":0}]}`,
	}
	var asked []int64
	extents, err := readMap(context.Background(), func(_ context.Context, offset int64) ([]byte, error) {
		asked = append(asked, offset)
		return []byte(pages[offset]), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{0, 150, 1000}, asked)
	require.Len(t, extents, 4)
	assert.Equal(t, Extent{Offset: 100, Length: 50, Type: 3}, extents[1])
	size, err := diskSizeFromExtents(extents)
	require.NoError(t, err)
	assert.Equal(t, int64(1024), size)
}

func TestReadMapErrors(t *testing.T) {
	cases := map[string]string{
		"not json":            `<html>`,
		"empty page":          `{"extents":[]}`,
		"paging stands still": `{"extents":[{"offset":0,"length":10,"type":0}],"next_offset":0}`,
	}
	for name, page := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := readMap(context.Background(), func(context.Context, int64) ([]byte, error) { return []byte(page), nil })
			require.Error(t, err)
		})
	}
	_, err := readMap(context.Background(), func(context.Context, int64) ([]byte, error) { return nil, errors.New("HTTP 500") })
	require.ErrorContains(t, err, "HTTP 500")
}

func TestDiskSizeFromExtentsRejectsGapsAndOverlaps(t *testing.T) {
	_, err := diskSizeFromExtents([]Extent{{Offset: 0, Length: 10}, {Offset: 20, Length: 10}})
	require.ErrorContains(t, err, "gap or overlap")
	_, err = diskSizeFromExtents([]Extent{{Offset: 0, Length: 10}, {Offset: 5, Length: 10}})
	require.ErrorContains(t, err, "gap or overlap")
	_, err = diskSizeFromExtents([]Extent{{Offset: 4, Length: 10}})
	require.Error(t, err)
	_, err = diskSizeFromExtents(nil)
	require.Error(t, err)
	_, err = diskSizeFromExtents([]Extent{{Offset: 0, Length: 0}})
	require.Error(t, err)
}

func TestPlanBlocksFull(t *testing.T) {
	// 10 blocks and a short tail. Blocks 2-3 are zero in the map, block 5 is
	// only partly zero, block 7 is a hole without the zero bit.
	extents := []Extent{
		{Offset: 0, Length: 2 * testBlock, Type: 0},
		{Offset: 2 * testBlock, Length: 2 * testBlock, Type: 3},
		{Offset: 4 * testBlock, Length: testBlock + 100, Type: 0},
		{Offset: 5*testBlock + 100, Length: testBlock - 100, Type: 2},
		{Offset: 6 * testBlock, Length: testBlock, Type: 0},
		{Offset: 7 * testBlock, Length: testBlock, Type: 1},
		{Offset: 8 * testBlock, Length: 2*testBlock + 10, Type: 0},
	}
	plan, err := PlanBlocks(extents, testBlock, false)
	require.NoError(t, err)
	assert.Equal(t, 10*testBlock+10, plan.DiskSize)
	assert.Equal(t, []int64{2, 3}, plan.Zero)
	assert.Equal(t, []int64{0, 1, 4, 5, 6, 7, 8, 9, 10}, plan.Read)
	assert.Equal(t, int64(10), plan.BlockLen(10))
	assert.Equal(t, 8*testBlock+10, plan.ReadBytes())
}

func TestPlanBlocksIncremental(t *testing.T) {
	// Dirty: a few bytes at the end of block 1, a range crossing blocks 4-6,
	// and a dirty+zero extent in block 8. Zero-only extents are not dirty.
	extents := []Extent{
		{Offset: 0, Length: 2*testBlock - 8, Type: 0},
		{Offset: 2*testBlock - 8, Length: 8, Type: 1},
		{Offset: 2 * testBlock, Length: 2*testBlock + 5, Type: 2},
		{Offset: 4*testBlock + 5, Length: 2 * testBlock, Type: 1},
		{Offset: 6*testBlock + 5, Length: 2*testBlock - 5, Type: 0},
		{Offset: 8 * testBlock, Length: 16, Type: 3},
		{Offset: 8*testBlock + 16, Length: 2*testBlock - 16, Type: 0},
	}
	plan, err := PlanBlocks(extents, testBlock, true)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 4, 5, 6, 8}, plan.Read)
	assert.Empty(t, plan.Zero, "an incremental never records blocks it did not read")
}

func TestPlanBlocksNothingDirty(t *testing.T) {
	plan, err := PlanBlocks(fullExtents(8*testBlock), testBlock, true)
	require.NoError(t, err)
	assert.Empty(t, plan.Read)
	assert.Empty(t, plan.Zero)
}

func TestPlanRuns(t *testing.T) {
	plan := &BlockPlan{DiskSize: 100 * testBlock, BlockSize: testBlock, Read: []int64{0, 1, 2, 3, 4, 5, 9, 10, 20}}
	assert.Equal(t, []blockRun{{0, 4}, {4, 2}, {9, 2}, {20, 1}}, plan.runs(4))
}

func TestIsZero(t *testing.T) {
	zeros := make([]byte, testBlock)
	block := make([]byte, testBlock)
	assert.True(t, isZero(block, zeros))
	assert.True(t, isZero(block[:10], zeros), "a short last block")
	block[testBlock-1] = 1
	assert.False(t, isZero(block, zeros), "a single byte at the very end")
	block[testBlock-1], block[0] = 0, 0x80
	assert.False(t, isZero(block, zeros))
}

func TestTransferAndRestoreFull(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	zero := map[int]bool{3: true, 4: true, 9: true, 20: true}
	disk := makeDisk(t, 21, zero, 123)
	ix := backup(t, store, &fakeDisk{data: disk}, fullExtents(int64(len(disk))), false, "cp1", "")

	assert.Len(t, ix.Blocks, 22)
	var zeroBlocks int
	offsets := map[int64]bool{}
	for _, b := range ix.Blocks {
		if b.Zero {
			zeroBlocks++
			assert.True(t, zero[int(b.N)], "block %d", b.N)
			continue
		}
		assert.False(t, offsets[b.Offset], "two blocks share offset %d", b.Offset)
		offsets[b.Offset] = true
	}
	assert.Equal(t, len(zero), zeroBlocks, "zero blocks are detected although the map calls them data")
	data, err := store.GetObjectBytes(ix.DataObject)
	require.NoError(t, err)
	assert.Equal(t, ix.DataSize, int64(len(data)))

	assert.True(t, bytes.Equal(disk, restore(t, store, ix)))
}

func TestTransferUsesMapZeroBlocksWithoutReading(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 8, map[int]bool{2: true, 3: true, 4: true, 5: true}, 0)
	extents := []Extent{
		{Offset: 0, Length: 2 * testBlock, Type: 0},
		{Offset: 2 * testBlock, Length: 4 * testBlock, Type: 3},
		{Offset: 6 * testBlock, Length: 2 * testBlock, Type: 0},
	}
	src := &fakeDisk{data: disk}
	ix := backup(t, store, src, extents, false, "cp1", "")
	assert.Equal(t, 2, src.requests, "only the two data runs are read")
	assert.Len(t, ix.Blocks, 8)
	assert.True(t, bytes.Equal(disk, restore(t, store, ix)))
}

func TestIncrementalChainRestore(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 16, map[int]bool{5: true}, 77)
	full := backup(t, store, &fakeDisk{data: disk}, fullExtents(int64(len(disk))), false, "cp1", "")

	// First incremental: block 2 rewritten, block 7 zeroed, block 5 filled.
	disk1 := append([]byte(nil), disk...)
	_, _ = rand.Read(disk1[2*testBlock : 3*testBlock])
	clear(disk1[7*testBlock : 8*testBlock])
	_, _ = rand.Read(disk1[5*testBlock+10 : 5*testBlock+20])
	dirty1 := []Extent{
		{Offset: 0, Length: 2 * testBlock, Type: 0},
		{Offset: 2 * testBlock, Length: testBlock, Type: 1},
		{Offset: 3 * testBlock, Length: 2*testBlock + 10, Type: 0},
		{Offset: 5*testBlock + 10, Length: 10, Type: 1},
		{Offset: 5*testBlock + 20, Length: 2*testBlock - 20, Type: 0},
		{Offset: 7 * testBlock, Length: testBlock, Type: 3},
		{Offset: 8 * testBlock, Length: 8*testBlock + 77, Type: 0},
	}
	inc1 := backup(t, store, &fakeDisk{data: disk1}, dirty1, true, "cp2", "cp1")
	assert.Len(t, inc1.Blocks, 3)

	// Second incremental: block 2 again and the short tail block.
	disk2 := append([]byte(nil), disk1...)
	_, _ = rand.Read(disk2[2*testBlock : 2*testBlock+50])
	_, _ = rand.Read(disk2[16*testBlock:])
	dirty2 := []Extent{
		{Offset: 0, Length: 2 * testBlock, Type: 0},
		{Offset: 2 * testBlock, Length: 50, Type: 1},
		{Offset: 2*testBlock + 50, Length: 14*testBlock - 50, Type: 0},
		{Offset: 16 * testBlock, Length: 77, Type: 1},
	}
	inc2 := backup(t, store, &fakeDisk{data: disk2}, dirty2, true, "cp3", "cp2")
	assert.Len(t, inc2.Blocks, 2)

	assert.True(t, bytes.Equal(disk, restore(t, store, full)))
	assert.True(t, bytes.Equal(disk1, restore(t, store, full, inc1)))
	assert.True(t, bytes.Equal(disk2, restore(t, store, full, inc1, inc2)))

	merged, err := MergeIndexes([]*BlockIndex{full, inc1, inc2})
	require.NoError(t, err)
	assert.Equal(t, 2, merged.Blocks[2].Source, "newest checkpoint wins")
	assert.Equal(t, 1, merged.Blocks[5].Source)
	assert.Equal(t, 1, merged.Blocks[7].Source)
	assert.True(t, merged.Blocks[7].Entry.Zero, "a block zeroed later overrides its data in the full")
	assert.Equal(t, 0, merged.Blocks[0].Source)
	assert.Equal(t, 2, merged.Blocks[16].Source)
}

func TestMergeIndexesRejectsBrokenChains(t *testing.T) {
	base := func(cp, parent, typ string, blocks ...int64) *BlockIndex {
		ix := &BlockIndex{Volume: "root", Checkpoint: cp, Parent: parent, BackupType: typ, DiskSize: 4 * testBlock, BlockSize: testBlock}
		for _, n := range blocks {
			ix.Blocks = append(ix.Blocks, BlockEntry{N: n, Zero: true})
		}
		return ix
	}
	full := base("cp1", "", "full", 0, 1, 2, 3)

	_, err := MergeIndexes(nil)
	require.Error(t, err)

	_, err = MergeIndexes([]*BlockIndex{base("cp2", "cp1", "incremental", 1)})
	require.ErrorContains(t, err, "no full backup")

	_, err = MergeIndexes([]*BlockIndex{full, base("cp3", "cp2", "incremental", 1)})
	require.ErrorContains(t, err, "has parent")

	other := base("cp2", "cp1", "incremental", 1)
	other.Volume = "data"
	_, err = MergeIndexes([]*BlockIndex{full, other})
	require.ErrorContains(t, err, "volume")

	resized := base("cp2", "cp1", "incremental", 1)
	resized.DiskSize = 8 * testBlock
	_, err = MergeIndexes([]*BlockIndex{full, resized})
	require.ErrorContains(t, err, "disk size")

	_, err = MergeIndexes([]*BlockIndex{base("cp1", "", "full", 0, 1, 3)})
	require.ErrorContains(t, err, "block 2")

	// A newer full in the middle of the chain replaces everything before it.
	merged, err := MergeIndexes([]*BlockIndex{full, base("cp2", "cp1", "full", 0, 1, 2, 3), base("cp3", "cp2", "incremental", 2)})
	require.NoError(t, err)
	assert.Equal(t, []int{1, 1, 2, 1}, []int{merged.Blocks[0].Source, merged.Blocks[1].Source, merged.Blocks[2].Source, merged.Blocks[3].Source})
}

func TestTransferRetriesFailedAndShortReads(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 12, nil, 0)
	src := &fakeDisk{data: disk, failAt: map[int]string{1: "error", 2: "short", 4: "truncated"}}
	ix := backup(t, store, src, fullExtents(int64(len(disk))), false, "cp1", "")
	assert.Greater(t, src.requests, 3, "failed requests were repeated")
	assert.Len(t, ix.Blocks, 12)
	assert.True(t, bytes.Equal(disk, restore(t, store, ix)), "no block lost or duplicated by the retries")
}

func TestTransferFailsWhenReadsKeepFailing(t *testing.T) {
	for _, mode := range []string{"error", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			disk := makeDisk(t, 8, nil, 0)
			plan, err := PlanBlocks(fullExtents(int64(len(disk))), testBlock, false)
			require.NoError(t, err)
			var uploaded atomic.Bool
			_, _, err = TransferVolume(context.Background(), &fakeDisk{data: disk, always: mode}, plan, func(r io.Reader) error {
				_, copyErr := io.Copy(io.Discard, r)
				if copyErr == nil {
					uploaded.Store(true)
				}
				return copyErr
			}, testOptions())
			require.ErrorContains(t, err, "failed after")
			assert.False(t, uploaded.Load(), "a failed transfer must not complete the upload")
		})
	}
}

func TestTransferFailsOnExtraData(t *testing.T) {
	disk := makeDisk(t, 4, nil, 0)
	plan, err := PlanBlocks(fullExtents(int64(len(disk))), testBlock, false)
	require.NoError(t, err)
	_, _, err = TransferVolume(context.Background(), &fakeDisk{data: disk, always: "extra"}, plan, func(r io.Reader) error {
		_, copyErr := io.Copy(io.Discard, r)
		return copyErr
	}, testOptions())
	require.ErrorContains(t, err, "more than the requested")
}

func TestTransferFailsWhenUploadFails(t *testing.T) {
	disk := makeDisk(t, 64, nil, 0)
	plan, err := PlanBlocks(fullExtents(int64(len(disk))), testBlock, false)
	require.NoError(t, err)
	_, _, err = TransferVolume(context.Background(), &fakeDisk{data: disk}, plan, func(r io.Reader) error {
		_, _ = io.CopyN(io.Discard, r, 3*testBlock)
		return errors.New("bucket quota exceeded")
	}, testOptions())
	require.ErrorContains(t, err, "bucket quota exceeded")
}

func TestTransferStopsWhenCanceled(t *testing.T) {
	disk := makeDisk(t, 64, nil, 0)
	plan, err := PlanBlocks(fullExtents(int64(len(disk))), testBlock, false)
	require.NoError(t, err)
	ctx, cancel := context.WithCancelCause(context.Background())
	var uploaded atomic.Bool
	_, _, err = TransferVolume(ctx, &fakeDisk{data: disk}, plan, func(r io.Reader) error {
		// Simulates the pause watchdog firing while blocks are in flight.
		_, _ = io.CopyN(io.Discard, r, testBlock)
		cancel(ErrVMPaused)
		_, copyErr := io.Copy(io.Discard, r)
		if copyErr == nil {
			uploaded.Store(true)
		}
		return copyErr
	}, testOptions())
	require.ErrorIs(t, err, ErrVMPaused)
	assert.False(t, uploaded.Load())
}

func TestTransferIdleTimeout(t *testing.T) {
	plan, err := PlanBlocks(fullExtents(4*testBlock), testBlock, false)
	require.NoError(t, err)
	opts := testOptions()
	opts.IdleTimeout, opts.Retries = 20*time.Millisecond, 1
	_, _, err = TransferVolume(context.Background(), stalledSource{}, plan, func(r io.Reader) error {
		_, copyErr := io.Copy(io.Discard, r)
		return copyErr
	}, opts)
	require.ErrorContains(t, err, "no data from the export")
}

// stalledSource returns a body that blocks until the request is canceled.
type stalledSource struct{}

func (stalledSource) Data(ctx context.Context, _, _ int64) (io.ReadCloser, error) {
	return io.NopCloser(readerFunc(func([]byte) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})), nil
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestTransferProgressAndRateLimit(t *testing.T) {
	disk := makeDisk(t, 10, map[int]bool{1: true}, 0)
	plan, err := PlanBlocks(fullExtents(int64(len(disk))), testBlock, false)
	require.NoError(t, err)
	opts := testOptions()
	opts.Progress = &Progress{}
	opts.Progress.AddTotal(plan.ReadBytes())
	// 10 blocks at 20 blocks per second take about half a second.
	opts.Limiter = NewRateLimiter(20 * testBlock)
	started := time.Now()
	_, _, err = TransferVolume(context.Background(), &fakeDisk{data: disk}, plan, func(r io.Reader) error {
		_, copyErr := io.Copy(io.Discard, r)
		return copyErr
	}, opts)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(started), 350*time.Millisecond)
	done, total := opts.Progress.Snapshot()
	assert.Equal(t, int64(len(disk)), total)
	assert.Equal(t, total, done, "zero blocks count as processed too")
	assert.Nil(t, NewRateLimiter(0))
}

func TestParseProgress(t *testing.T) {
	logs := strings.Join([]string{
		`{"level":"info","ts":"2026-01-01T00:00:00Z","logger":"puller","msg":"Planned volume","bytesToRead":100}`,
		`{"level":"info","logger":"puller","msg":"transfer progress","bytesDone":10,"totalBytes":100}`,
		`not json transfer progress`,
		`{"level":"info","logger":"puller","msg":"transfer progress","bytesDone":40,"totalBytes":100}`,
		`{"level":"info","logger":"puller","msg":"Volume transferred","bytesRead":100}`,
		``,
	}, "\n")
	done, total, ok := ParseProgress(logs)
	require.True(t, ok)
	assert.Equal(t, int64(40), done)
	assert.Equal(t, int64(100), total)

	_, _, ok = ParseProgress(`{"msg":"Starting"}`)
	assert.False(t, ok)
	_, _, ok = ParseProgress(`{"msg":"transfer progress","bytesDone":200,"totalBytes":100}`)
	assert.False(t, ok, "done beyond total is not believed")
	_, _, ok = ParseProgress(`{"msg":"transfer progress","totalBytes":100}`)
	assert.False(t, ok)
}

func TestIndexRoundTripAndValidation(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 6, map[int]bool{1: true}, 0)
	ix := backup(t, store, &fakeDisk{data: disk}, fullExtents(int64(len(disk))), false, "cp1", "")

	loaded, err := LoadIndex(store, testBucket, IndexPathForData(ix.DataObject))
	require.NoError(t, err)
	assert.Equal(t, ix.Blocks, loaded.Blocks)
	assert.Equal(t, ix.DiskSize, loaded.DiskSize)

	mutate := func(f func(*BlockIndex)) error {
		c := *ix
		c.Blocks = append([]BlockEntry(nil), ix.Blocks...)
		f(&c)
		return c.Validate()
	}
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Version = 99 }), "version")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Format = "qcow2" }), "format")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Blocks = c.Blocks[:5] }), "add up")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Blocks[0].Checksum = "abc" }), "checksum")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Blocks[0].Length += 1 << 30 }), "outside the data object")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Blocks[2].N = 0 }), "number")
	require.ErrorContains(t, mutate(func(c *BlockIndex) { c.Blocks[1].Length = 5 }), "zero block")
	require.ErrorContains(t, mutate(func(c *BlockIndex) {
		// Drop the zero block: the frames still add up, the block count does not.
		c.Blocks = append(c.Blocks[:1:1], c.Blocks[2:]...)
	}), "full backup index has")

	_, err = DecodeIndex([]byte("garbage"))
	require.Error(t, err)

	// An index copied next to another data object is refused.
	raw, err := store.GetObjectBytes(IndexPathForData(ix.DataObject))
	require.NoError(t, err)
	require.NoError(t, store.PutObject(testBucket, "checkpoints/ns/other/cp1/root.blocks.idx", bytes.NewReader(raw)))
	_, err = LoadIndex(store, testBucket, "checkpoints/ns/other/cp1/root.blocks.idx")
	require.ErrorContains(t, err, "outside its checkpoint")
}

func TestVerifyAndRestoreDetectCorruption(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 12, map[int]bool{4: true}, 9)
	ix := backup(t, store, &fakeDisk{data: disk}, fullExtents(int64(len(disk))), false, "cp1", "")

	result, err := VerifyDataObject(context.Background(), store, testBucket, ix.DataObject, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(13), result.Blocks)
	assert.Equal(t, int64(1), result.ZeroBlocks)
	assert.Equal(t, int64(len(disk))-testBlock, result.RawBytes)

	good, err := store.GetObjectBytes(ix.DataObject)
	require.NoError(t, err)

	// Flip one byte in the middle of the data object.
	bad := append([]byte(nil), good...)
	bad[len(bad)/2] ^= 0xff
	require.NoError(t, store.PutObject(testBucket, ix.DataObject, bytes.NewReader(bad)))
	_, err = VerifyDataObject(context.Background(), store, testBucket, ix.DataObject, 2)
	require.Error(t, err)
	target := filepath.Join(t.TempDir(), "disk.img")
	require.Error(t, RestoreChain(context.Background(), store, testBucket, []string{ix.DataObject}, target, RestoreOptions{}, logr.Discard()))

	// A truncated data object is a short read.
	require.NoError(t, store.PutObject(testBucket, ix.DataObject, bytes.NewReader(good[:len(good)-10])))
	_, err = VerifyDataObject(context.Background(), store, testBucket, ix.DataObject, 2)
	require.ErrorContains(t, err, "short read")

	// Two blocks swapped in the index: both decompress, both fail the checksum.
	require.NoError(t, store.PutObject(testBucket, ix.DataObject, bytes.NewReader(good)))
	swapped := *ix
	swapped.Blocks = append([]BlockEntry(nil), ix.Blocks...)
	swapped.Blocks[0].Checksum, swapped.Blocks[1].Checksum = swapped.Blocks[1].Checksum, swapped.Blocks[0].Checksum
	encoded, err := EncodeIndex(&swapped)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(testBucket, IndexPathForData(ix.DataObject), bytes.NewReader(encoded)))
	_, err = VerifyDataObject(context.Background(), store, testBucket, ix.DataObject, 2)
	require.ErrorContains(t, err, "checksum")
}

func TestVerifyVM(t *testing.T) {
	store := uploader.NewMockObjectStore(testBucket, "")
	disk := makeDisk(t, 4, nil, 0)
	ix := backup(t, store, &fakeDisk{data: disk}, fullExtents(int64(len(disk))), false, "cp1", "")
	index := uploader.VMIndex{VMName: "vm", Namespace: "ns", Checkpoints: []uploader.CheckpointEntry{
		{ID: "old", Type: "full", Files: []uploader.CheckpointFile{{Filename: "vmb-root.qcow2", DiskName: "root", ObjectPath: "checkpoints/ns/vm/old/vmb-root.qcow2"}}},
		{ID: "cp1", Type: "full", Format: uploader.CheckpointFormatBlocks, Files: []uploader.CheckpointFile{
			{Filename: "root.blocks", DiskName: "root", Size: ix.DataSize, ObjectPath: ix.DataObject},
			{Filename: "root.blocks.idx", ObjectPath: IndexPathForData(ix.DataObject)},
		}},
	}}
	require.NoError(t, uploader.PutVMIndex(store, "ns", "vm", testBucket, index))

	verified, err := VerifyVM(context.Background(), store, testBucket, "ns", "vm", "", 2, logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, 1, verified, "the qcow2 checkpoint is skipped")

	_, err = VerifyVM(context.Background(), store, testBucket, "ns", "vm", "old", 2, logr.Discard())
	require.ErrorContains(t, err, "not stored in block format")
	_, err = VerifyVM(context.Background(), store, testBucket, "ns", "vm", "missing", 2, logr.Discard())
	require.ErrorContains(t, err, "not found")
	_, err = VerifyVM(context.Background(), store, testBucket, "ns", "nobody", "", 2, logr.Discard())
	require.Error(t, err)

	index.Checkpoints[1].Files[0].Size++
	require.NoError(t, uploader.PutVMIndex(store, "ns", "vm", testBucket, index))
	_, err = VerifyVM(context.Background(), store, testBucket, "ns", "vm", "cp1", 2, logr.Discard())
	require.ErrorContains(t, err, "stored bytes")
}

func TestPlanFetchCoalescesAdjacentFrames(t *testing.T) {
	blocks := []fetchBlock{
		{object: "b", entry: BlockEntry{N: 9, Offset: 0, Length: 10}},
		{object: "a", entry: BlockEntry{N: 2, Offset: 30, Length: 10}},
		{object: "a", entry: BlockEntry{N: 0, Offset: 0, Length: 10}},
		{object: "a", entry: BlockEntry{N: 1, Offset: 10, Length: 10}},
		{object: "a", entry: BlockEntry{N: 3, Offset: 40, Length: 100}},
	}
	ranges := planFetch(blocks, 60)
	require.Len(t, ranges, 4)
	assert.Equal(t, fetchRange{object: "a", offset: 0, length: 20, blocks: []fetchBlock{blocks[2], blocks[3]}}, ranges[0])
	assert.Equal(t, int64(30), ranges[1].offset, "a gap in the object starts a new request")
	assert.Equal(t, int64(40), ranges[2].offset, "the size cap starts a new request")
	assert.Equal(t, "b", ranges[3].object)
}

func TestPartSizeFor(t *testing.T) {
	assert.Equal(t, int64(minPartSize), partSizeFor(10<<30))
	big := partSizeFor(900 << 30)
	assert.Zero(t, big%(1<<20))
	assert.GreaterOrEqual(t, big*maxUploadParts, int64(900<<30), "a disk that does not compress still fits")
	assert.Less(t, big, int64(160<<20))
}

func TestParseExportInfoAndPausedCheck(t *testing.T) {
	// Covered in run_test.go; kept here so the file fails loudly if the
	// constants the controller relies on change.
	assert.Equal(t, "transfer progress", ProgressMessage)
	assert.Equal(t, "ExportReady", ExportReadyReason)
	assert.Equal(t, "root.blocks.idx", IndexObjectName("root"))
	assert.Equal(t, fmt.Sprintf("x/%s", IndexObjectName("root")), IndexPathForData("x/"+DataObjectName("root")))
}
