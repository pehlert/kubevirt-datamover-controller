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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

// TransferOptions tunes the transfer of one volume.
type TransferOptions struct {
	// Readers is the number of parallel ranged reads.
	Readers int
	// RunBlocks is the largest number of consecutive blocks read with one request.
	RunBlocks int64
	// Retries is how often a failed read is repeated before the transfer fails.
	Retries int
	// RetryDelay is the pause before the first retry; it doubles per attempt.
	RetryDelay time.Duration
	// IdleTimeout aborts a read that delivers no complete block for this long.
	IdleTimeout time.Duration
	// Limiter, when set, limits the read rate across all readers.
	Limiter *RateLimiter
	// Progress, when set, is advanced by the bytes of every finished block.
	Progress *Progress
}

// DefaultTransferOptions returns the defaults used by the pull subcommand.
func DefaultTransferOptions() TransferOptions {
	return TransferOptions{
		Readers:     4,
		RunBlocks:   16,
		Retries:     5,
		RetryDelay:  2 * time.Second,
		IdleTimeout: 5 * time.Minute,
	}
}

// Progress counts the bytes of a transfer.
type Progress struct {
	total atomic.Int64
	done  atomic.Int64
}

// AddTotal raises the number of bytes the transfer will process.
func (p *Progress) AddTotal(n int64) { p.total.Add(n) }

// AddDone records n processed bytes.
func (p *Progress) AddDone(n int64) { p.done.Add(n) }

// Snapshot returns the processed and the total bytes.
func (p *Progress) Snapshot() (done, total int64) { return p.done.Load(), p.total.Load() }

// RateLimiter limits a byte rate across goroutines. Each Wait reserves its
// bytes at the configured rate and sleeps until the reservation is due.
type RateLimiter struct {
	mu          sync.Mutex
	bytesPerSec int64
	next        time.Time
}

// NewRateLimiter returns a limiter for bytesPerSec, or nil for no limit.
func NewRateLimiter(bytesPerSec int64) *RateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &RateLimiter{bytesPerSec: bytesPerSec}
}

// Wait blocks until n more bytes may be read.
func (l *RateLimiter) Wait(ctx context.Context, n int64) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	wait := l.next.Sub(now)
	l.next = l.next.Add(time.Duration(float64(n) / float64(l.bytesPerSec) * float64(time.Second)))
	l.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

// blockResult is a block that has been read and, unless it is zero, compressed.
type blockResult struct {
	n    int64
	zero bool
	sum  string
	comp *[]byte
}

// isZero reports whether b contains only zero bytes.
func isZero(b []byte, zeros []byte) bool {
	return bytes.Equal(b, zeros[:len(b)])
}

// TransferVolume reads the planned blocks from src, compresses them and
// streams the frames to upload as one object. It returns the index entries
// for all blocks of the plan and the length of the uploaded object.
//
// upload must consume the reader until EOF; its error fails the transfer.
// Nothing is written to disk: a block lives in memory from the read until
// its frame has been handed to the upload stream.
func TransferVolume(
	ctx context.Context, src DataSource, plan *BlockPlan, upload func(io.Reader) error, opts TransferOptions,
) ([]BlockEntry, int64, error) {
	if opts.Readers <= 0 || opts.RunBlocks <= 0 {
		return nil, 0, fmt.Errorf("invalid transfer options: readers=%d runBlocks=%d", opts.Readers, opts.RunBlocks)
	}
	// One encoder serves all readers; EncodeAll is safe for concurrent use
	// and each call produces an independent frame.
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(opts.Readers))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	defer func() { _ = encoder.Close() }()

	pipeReader, pipeWriter := io.Pipe()
	uploadDone := make(chan error, 1)
	go func() {
		err := upload(pipeReader)
		// Unblock the writer if the upload stopped before reading everything.
		_ = pipeReader.CloseWithError(errors.Join(errors.New("upload stopped"), err))
		uploadDone <- err
	}()

	runs := make(chan blockRun)
	results := make(chan blockResult, opts.Readers)
	zeros := make([]byte, plan.BlockSize)
	rawPool := sync.Pool{New: func() any { b := make([]byte, plan.BlockSize); return &b }}
	compPool := sync.Pool{New: func() any { b := make([]byte, 0, plan.BlockSize+plan.BlockSize/64); return &b }}

	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(runs)
		for _, run := range plan.runs(opts.RunBlocks) {
			select {
			case runs <- run:
			case <-gctx.Done():
				return context.Cause(gctx)
			}
		}
		return nil
	})

	var readers sync.WaitGroup
	for range opts.Readers {
		readers.Add(1)
		group.Go(func() error {
			defer readers.Done()
			raw := rawPool.Get().(*[]byte)
			defer rawPool.Put(raw)
			for run := range runs {
				emit := func(n int64, data []byte) error {
					res := blockResult{n: n}
					if isZero(data, zeros) {
						res.zero = true
					} else {
						sum := sha256.Sum256(data)
						res.sum = hex.EncodeToString(sum[:])
						comp := compPool.Get().(*[]byte)
						*comp = encoder.EncodeAll(data, (*comp)[:0])
						res.comp = comp
					}
					select {
					case results <- res:
						return nil
					case <-gctx.Done():
						return context.Cause(gctx)
					}
				}
				if err := readRun(gctx, src, plan, run, *raw, opts, emit); err != nil {
					return err
				}
			}
			return nil
		})
	}
	go func() {
		readers.Wait()
		close(results)
	}()

	entries := make([]BlockEntry, 0, len(plan.Read)+len(plan.Zero))
	for _, n := range plan.Zero {
		entries = append(entries, BlockEntry{N: n, Zero: true})
	}
	var dataSize int64
	group.Go(func() error {
		for res := range results {
			entry := BlockEntry{N: res.n, Zero: res.zero}
			if !res.zero {
				frame := *res.comp
				if _, err := pipeWriter.Write(frame); err != nil {
					return fmt.Errorf("failed to hand block %d to the upload: %w", res.n, err)
				}
				entry.Offset, entry.Length, entry.Checksum = dataSize, int64(len(frame)), res.sum
				dataSize += int64(len(frame))
				compPool.Put(res.comp)
			}
			entries = append(entries, entry)
			if opts.Progress != nil {
				opts.Progress.AddDone(plan.BlockLen(res.n))
			}
		}
		return nil
	})

	transferErr := group.Wait()
	if transferErr != nil {
		// Fail the upload instead of completing a truncated object.
		_ = pipeWriter.CloseWithError(transferErr)
	} else {
		_ = pipeWriter.Close()
	}
	uploadErr := <-uploadDone
	if transferErr != nil {
		return nil, 0, transferErr
	}
	if uploadErr != nil {
		return nil, 0, fmt.Errorf("upload failed: %w", uploadErr)
	}
	if want := len(plan.Read) + len(plan.Zero); len(entries) != want {
		return nil, 0, fmt.Errorf("transfer produced %d block entries, want %d", len(entries), want)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].N < entries[j].N })
	return entries, dataSize, nil
}

// readRun reads the blocks of one run and passes each complete block to
// emit. A failed or short read is retried from the first block that was not
// delivered, so a block is never emitted twice.
func readRun(
	ctx context.Context, src DataSource, plan *BlockPlan, run blockRun, buf []byte, opts TransferOptions,
	emit func(n int64, data []byte) error,
) error {
	next := run.First
	end := run.First + run.Count
	delay := opts.RetryDelay
	for attempt := 0; ; attempt++ {
		before := next
		err := readBlocks(ctx, src, plan, &next, end, buf, opts, emit)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		var emitErr *emitError
		if errors.As(err, &emitErr) {
			return emitErr.err
		}
		if next > before {
			// Progress was made: start counting attempts again.
			attempt, delay = 0, opts.RetryDelay
		}
		if attempt >= opts.Retries {
			return fmt.Errorf("reading blocks %d-%d failed after %d attempts: %w", next, end-1, attempt+1, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
		delay *= 2
	}
}

// emitError marks an error returned by the emit callback, which must not be retried.
type emitError struct{ err error }

func (e *emitError) Error() string { return e.err.Error() }

// readBlocks performs one request for blocks [*next, end) and advances *next
// past every block it delivered.
func readBlocks(
	ctx context.Context, src DataSource, plan *BlockPlan, next *int64, end int64, buf []byte, opts TransferOptions,
	emit func(n int64, data []byte) error,
) error {
	offset := *next * plan.BlockSize
	length := min(end*plan.BlockSize, plan.DiskSize) - offset

	reqCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var idle *time.Timer
	if opts.IdleTimeout > 0 {
		idle = time.AfterFunc(opts.IdleTimeout, func() {
			cancel(fmt.Errorf("no data from the export for %s", opts.IdleTimeout))
		})
		defer idle.Stop()
	}

	body, err := src.Data(reqCtx, offset, length)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	for *next < end {
		want := plan.BlockLen(*next)
		if err := opts.Limiter.Wait(reqCtx, want); err != nil {
			return err
		}
		got, err := io.ReadFull(body, buf[:want])
		if err != nil {
			if cause := context.Cause(reqCtx); cause != nil {
				return cause
			}
			// The export sends no length and cuts the connection on error,
			// so a short block is the only sign of a failed read.
			return fmt.Errorf("short read of block %d: got %d of %d bytes: %w", *next, got, want, err)
		}
		if idle != nil {
			idle.Reset(opts.IdleTimeout)
		}
		if err := emit(*next, buf[:want]); err != nil {
			return &emitError{err: err}
		}
		*next++
	}
	// The response must end exactly where the request did.
	var extra [1]byte
	if n, _ := body.Read(extra[:]); n > 0 {
		return &emitError{err: fmt.Errorf("export returned more than the requested %d bytes at offset %d", length, offset)}
	}
	return nil
}
