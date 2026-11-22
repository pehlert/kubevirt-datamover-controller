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
	"os"
	"strings"
	"sync/atomic"

	"github.com/go-logr/logr"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// Environment variables of the verify subcommand, in addition to the BSL
// settings shared with the other subcommands.
const (
	// EnvVerifyCheckpoint limits verification to one checkpoint ID. Without
	// it every block-format checkpoint of the VM is verified.
	EnvVerifyCheckpoint = "KUBEVIRT_DM_VERIFY_CHECKPOINT"
	// EnvVerifyWorkers sets the number of parallel ranged downloads.
	EnvVerifyWorkers = "KUBEVIRT_DM_VERIFY_WORKERS"
)

// VerifyResult summarizes the verification of one data object.
type VerifyResult struct {
	Checkpoint  string
	Volume      string
	Blocks      int64
	ZeroBlocks  int64
	StoredBytes int64
	RawBytes    int64
}

// VerifyDataObject re-reads the data object behind a block index and checks
// that every stored block decompresses to the recorded length and checksum.
func VerifyDataObject(
	ctx context.Context, store velero.ObjectStore, bucket, dataObject string, workers int,
) (*VerifyResult, error) {
	ix, err := LoadIndex(store, bucket, IndexPathForData(dataObject))
	if err != nil {
		return nil, err
	}
	if ix.DataObject != dataObject {
		return nil, fmt.Errorf("block index of %s points at %s", dataObject, ix.DataObject)
	}
	result := &VerifyResult{Checkpoint: ix.Checkpoint, Volume: ix.Volume, StoredBytes: ix.DataSize}
	fetch := make([]fetchBlock, 0, len(ix.Blocks))
	for _, b := range ix.Blocks {
		result.Blocks++
		if b.Zero {
			result.ZeroBlocks++
			continue
		}
		fetch = append(fetch, fetchBlock{object: ix.DataObject, entry: b, rawLen: ix.BlockLen(b.N)})
	}
	var raw atomic.Int64
	if err := fetchBlocks(ctx, store, bucket, fetch, workers, func(_ fetchBlock, data []byte) error {
		raw.Add(int64(len(data)))
		return nil
	}); err != nil {
		return nil, err
	}
	result.RawBytes = raw.Load()
	return result, nil
}

// VerifyVM verifies the block-format checkpoints of one VM. An empty
// checkpoint selects all of them. It returns the number of data objects
// verified and fails on the first block that does not match.
func VerifyVM(
	ctx context.Context, store velero.ObjectStore, bucket, vmNamespace, vmName, checkpoint string, workers int,
	logger logr.Logger,
) (int, error) {
	vmIndex, found, err := uploader.GetVMIndex(store, vmNamespace, vmName, bucket, logger)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("no checkpoint index found for VM %s/%s", vmNamespace, vmName)
	}
	verified, matched := 0, false
	for _, entry := range vmIndex.Checkpoints {
		if checkpoint != "" && entry.ID != checkpoint {
			continue
		}
		matched = true
		if entry.Format != uploader.CheckpointFormatBlocks {
			if checkpoint != "" {
				return 0, fmt.Errorf("checkpoint %s is not stored in block format and cannot be verified", entry.ID)
			}
			logger.Info("Skipping checkpoint that is not in block format", "checkpoint", entry.ID)
			continue
		}
		for _, file := range entry.Files {
			if file.DiskName == "" {
				continue // block index object, read through its data object
			}
			want := uploader.GetQCOWPath(vmNamespace, vmName, entry.ID, file.Filename)
			if file.ObjectPath != want {
				return verified, fmt.Errorf("checkpoint %s file %q has object path %q, want %q", entry.ID, file.Filename, file.ObjectPath, want)
			}
			result, err := VerifyDataObject(ctx, store, bucket, file.ObjectPath, workers)
			if err != nil {
				return verified, fmt.Errorf("checkpoint %s volume %s: %w", entry.ID, file.DiskName, err)
			}
			if result.StoredBytes != file.Size {
				return verified, fmt.Errorf("checkpoint %s volume %s: index records %d stored bytes, VM index %d",
					entry.ID, file.DiskName, result.StoredBytes, file.Size)
			}
			logger.Info("Data object verified", "checkpoint", entry.ID, "type", entry.Type, "volume", result.Volume,
				"blocks", result.Blocks, "zeroBlocks", result.ZeroBlocks,
				"storedBytes", result.StoredBytes, "rawBytes", result.RawBytes)
			verified++
		}
	}
	if checkpoint != "" && !matched {
		return 0, fmt.Errorf("checkpoint %s not found for VM %s/%s", checkpoint, vmNamespace, vmName)
	}
	return verified, nil
}

// RunVerify is the entrypoint of the verify subcommand.
func RunVerify(ctx context.Context, logger logr.Logger) error {
	storeConfig := common.ObjectStoreConfig{
		BSLProvider:                  os.Getenv(common.EnvBSLProvider),
		BSLBucket:                    os.Getenv(common.EnvBSLBucket),
		BSLPrefix:                    os.Getenv(common.EnvBSLPrefix),
		BSLRegion:                    os.Getenv(common.EnvBSLRegion),
		BSLS3URL:                     os.Getenv(common.EnvBSLS3URL),
		BSLS3ForcePathStyle:          common.ParseBool(os.Getenv(common.EnvBSLS3ForcePathStyle)),
		BSLInsecureSkipTLSVerify:     common.ParseBool(os.Getenv(common.EnvBSLInsecureSkipTLSVerify)),
		BSLCACert:                    os.Getenv(common.EnvBSLCACert),
		BSLCustomerKeyEncryptionFile: os.Getenv(common.EnvBSLCustomerKeyEncryptionFile),
		BSLProfile:                   os.Getenv(common.EnvBSLProfile),
		CredentialsFile:              os.Getenv(common.EnvCredentialsFile),
	}
	if storeConfig.CredentialsFile == "" {
		storeConfig.CredentialsFile = common.DefaultCredentialsPath
	}
	vmName := os.Getenv(uploader.EnvVMName)
	vmNamespace := os.Getenv(uploader.EnvVMNamespace)
	for name, value := range map[string]string{
		common.EnvBSLProvider:   storeConfig.BSLProvider,
		common.EnvBSLBucket:     storeConfig.BSLBucket,
		uploader.EnvVMName:      vmName,
		uploader.EnvVMNamespace: vmNamespace,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	workers := DefaultFetchWorkers
	if v := strings.TrimSpace(os.Getenv(EnvVerifyWorkers)); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &workers); err != nil || workers < 1 {
			return fmt.Errorf("%s must be a positive number, got %q", EnvVerifyWorkers, v)
		}
	}
	store, err := uploader.InitObjectStore(&storeConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}
	verified, err := VerifyVM(ctx, store, storeConfig.BSLBucket, vmNamespace, vmName,
		os.Getenv(EnvVerifyCheckpoint), workers, logger)
	if err != nil {
		return fmt.Errorf("verification failed after %d data objects: %w", verified, err)
	}
	logger.Info("Verification completed", "dataObjects", verified)
	return nil
}
