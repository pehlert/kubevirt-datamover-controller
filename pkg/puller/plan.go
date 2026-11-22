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
	"fmt"
)

// Extent is one entry of the block map served by the export endpoint.
type Extent struct {
	Offset      int64  `json:"offset"`
	Length      int64  `json:"length"`
	Type        int    `json:"type"`
	Description string `json:"description,omitempty"`
}

// Extent type bits. The meaning of bit 0 depends on the backup type: for a
// full backup it marks a hole, for an incremental a dirty range.
const (
	extentBitHoleOrDirty = 1
	extentBitZero        = 2
)

// BlockPlan lists what has to be done for every block of a disk.
type BlockPlan struct {
	DiskSize  int64
	BlockSize int64
	// Read holds the blocks to read from the export, ascending.
	Read []int64
	// Zero holds the blocks the map reports as zero in full; they are
	// recorded as zero blocks without being read. Ascending.
	Zero []int64
}

// BlockLen returns the length of block n.
func (p *BlockPlan) BlockLen(n int64) int64 { return blockLen(p.DiskSize, p.BlockSize, n) }

// ReadBytes returns the number of bytes the plan reads from the export.
func (p *BlockPlan) ReadBytes() int64 {
	var total int64
	for _, n := range p.Read {
		total += p.BlockLen(n)
	}
	return total
}

// diskSizeFromExtents checks that the extents cover the disk from offset 0
// without gaps or overlaps and returns the disk size.
func diskSizeFromExtents(extents []Extent) (int64, error) {
	var end int64
	for i, e := range extents {
		if e.Length <= 0 {
			return 0, fmt.Errorf("map extent %d has length %d", i, e.Length)
		}
		if e.Offset != end {
			return 0, fmt.Errorf("map extent %d starts at %d, want %d (gap or overlap)", i, e.Offset, end)
		}
		end += e.Length
	}
	if end == 0 {
		return 0, fmt.Errorf("block map is empty")
	}
	return end, nil
}

// PlanBlocks turns a block map into a plan.
//
// Full backup: every block is covered. A block is read unless the map
// reports all of it as zero. The map of a full backup reports most of a
// disk as data even where it is zero, so the reader still has to detect
// zero blocks itself.
//
// Incremental backup: only blocks that overlap a dirty extent are read;
// all other blocks are left to the parent checkpoints.
func PlanBlocks(extents []Extent, blockSize int64, incremental bool) (*BlockPlan, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size %d", blockSize)
	}
	diskSize, err := diskSizeFromExtents(extents)
	if err != nil {
		return nil, err
	}
	count := blockCount(diskSize, blockSize)
	read := make([]bool, count)
	for _, e := range extents {
		var wanted bool
		if incremental {
			wanted = e.Type&extentBitHoleOrDirty != 0
		} else {
			wanted = e.Type&extentBitZero == 0
		}
		if !wanted {
			continue
		}
		for n := e.Offset / blockSize; n <= (e.Offset+e.Length-1)/blockSize; n++ {
			read[n] = true
		}
	}
	plan := &BlockPlan{DiskSize: diskSize, BlockSize: blockSize}
	for n := range count {
		switch {
		case read[n]:
			plan.Read = append(plan.Read, n)
		case !incremental:
			plan.Zero = append(plan.Zero, n)
		}
	}
	return plan, nil
}

// blockRun is a range of consecutive blocks read with one request.
type blockRun struct {
	First int64
	Count int64
}

// runs groups the blocks to read into runs of at most maxBlocks consecutive blocks.
func (p *BlockPlan) runs(maxBlocks int64) []blockRun {
	var out []blockRun
	for _, n := range p.Read {
		if l := len(out); l > 0 && out[l-1].First+out[l-1].Count == n && out[l-1].Count < maxBlocks {
			out[l-1].Count++
			continue
		}
		out = append(out, blockRun{First: n, Count: 1})
	}
	return out
}
