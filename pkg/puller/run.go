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
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// Environment variables of the pull subcommand, in addition to the ones the
// upload subcommand reads (BSL settings, VM, checkpoint, backup type).
const (
	EnvTokenFile     = "KUBEVIRT_DM_PULL_TOKEN_FILE"
	EnvReaders       = "KUBEVIRT_DM_PULL_READERS"
	EnvRateLimitMiB  = "KUBEVIRT_DM_PULL_RATE_LIMIT_MIB"
	EnvProgressEvery = "KUBEVIRT_DM_PULL_PROGRESS_INTERVAL"

	// DefaultTokenFile is where the export token secret is mounted.
	DefaultTokenFile = "/export-token/token"

	// ProgressMessage is the log message of a progress line. The controller
	// looks for it in the pod log to fill in DataUpload.status.progress.
	ProgressMessage = "transfer progress"

	// terminationLogPath receives the failure reason so it shows up as the
	// pod's termination message.
	terminationLogPath = "/dev/termination-log"

	// ExportReadyReason is the reason of the Progressing condition of a
	// pull-mode backup whose export endpoints can be read.
	ExportReadyReason = "ExportReady"

	defaultProgressInterval = 10 * time.Second
	minPartSize             = 16 << 20
	maxUploadParts          = 8000
)

// pausePollInterval is how often the VM is checked for a pause.
var pausePollInterval = 3 * time.Second

// transferDefaults supplies the transfer options; tests shorten the retries.
var transferDefaults = DefaultTransferOptions

// ErrVMPaused ends a transfer because the VM was paused while its backup was open.
var ErrVMPaused = errors.New("the virtual machine is paused")

// Config is the configuration of the pull subcommand.
type Config struct {
	*uploader.UploaderConfig

	TokenFile        string
	Readers          int
	RateLimitBytes   int64
	ProgressInterval time.Duration
}

// LoadConfigFromEnv reads the configuration of the pull subcommand.
func LoadConfigFromEnv() (*Config, error) {
	base, err := uploader.LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	base.Format = uploader.CheckpointFormatBlocks
	cfg := &Config{
		UploaderConfig:   base,
		TokenFile:        os.Getenv(EnvTokenFile),
		Readers:          DefaultTransferOptions().Readers,
		ProgressInterval: defaultProgressInterval,
	}
	if cfg.TokenFile == "" {
		cfg.TokenFile = DefaultTokenFile
	}
	if v := os.Getenv(EnvReaders); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 64 {
			return nil, fmt.Errorf("%s must be a number between 1 and 64, got %q", EnvReaders, v)
		}
		cfg.Readers = n
	}
	if v := os.Getenv(EnvRateLimitMiB); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%s must be a non-negative number of MiB/s, got %q", EnvRateLimitMiB, v)
		}
		cfg.RateLimitBytes = n << 20
	}
	if v := os.Getenv(EnvProgressEvery); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%s must be a positive duration, got %q", EnvProgressEvery, v)
		}
		cfg.ProgressInterval = d
	}
	if cfg.VMBName == "" {
		return nil, fmt.Errorf("%s is required", uploader.EnvVMBName)
	}
	return cfg, nil
}

// exportVolume is one volume of a pull-mode backup as published in its status.
type exportVolume struct {
	Name        string
	DataURL     string
	MapURL      string
	Incremental bool
}

// exportInfo is what the transfer needs from a VirtualMachineBackup.
type exportInfo struct {
	CACert  []byte
	Volumes []exportVolume
}

// parseExportInfo extracts the export endpoints and per-volume backup types
// from a VirtualMachineBackup. The fields are newer than the vendored API
// types, so the object is read as unstructured.
func parseExportInfo(vmb *unstructured.Unstructured) (*exportInfo, error) {
	conditions, _, _ := unstructured.NestedSlice(vmb.Object, "status", "conditions")
	ready := false
	for _, c := range conditions {
		m, _ := c.(map[string]any)
		if m["type"] == string(kubevirtbackupv1alpha1.ConditionProgressing) &&
			m["status"] == string(corev1.ConditionTrue) && m["reason"] == ExportReadyReason {
			ready = true
		}
	}
	if !ready {
		return nil, fmt.Errorf("backup %s is not ready for export", vmb.GetName())
	}
	cert, _, _ := unstructured.NestedString(vmb.Object, "status", "links", "internal", "cert")
	if cert == "" {
		return nil, fmt.Errorf("backup %s publishes no export certificate", vmb.GetName())
	}
	types := map[string]string{}
	included, _, _ := unstructured.NestedSlice(vmb.Object, "status", "includedVolumes")
	for _, v := range included {
		m, _ := v.(map[string]any)
		name, _ := m["volumeName"].(string)
		typ, _ := m["type"].(string)
		types[name] = typ
	}
	info := &exportInfo{CACert: []byte(cert)}
	links, _, _ := unstructured.NestedSlice(vmb.Object, "status", "links", "internal", "volumes")
	for _, v := range links {
		m, _ := v.(map[string]any)
		vol := exportVolume{}
		vol.Name, _ = m["volumeName"].(string)
		vol.DataURL, _ = m["dataEndpoint"].(string)
		vol.MapURL, _ = m["mapEndpoint"].(string)
		if vol.Name == "" || vol.DataURL == "" || vol.MapURL == "" {
			return nil, fmt.Errorf("backup %s has an incomplete export link: %v", vmb.GetName(), m)
		}
		// The map has to be read differently for full and incremental
		// backups, so a volume without a type cannot be transferred.
		switch {
		case strings.EqualFold(types[vol.Name], uploader.BackupTypeFull):
		case strings.EqualFold(types[vol.Name], uploader.BackupTypeIncremental):
			vol.Incremental = true
		default:
			return nil, fmt.Errorf("backup %s reports no backup type for volume %q", vmb.GetName(), vol.Name)
		}
		info.Volumes = append(info.Volumes, vol)
	}
	if len(info.Volumes) == 0 {
		return nil, fmt.Errorf("backup %s publishes no export volumes", vmb.GetName())
	}
	return info, nil
}

// Run is the entrypoint of the pull subcommand.
func Run(ctx context.Context, logger logr.Logger) error {
	logger.Info("Starting kubevirt datamover pull transfer")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	logger.Info("Config loaded", "vm", cfg.VMNamespace+"/"+cfg.VMName, "checkpoint", cfg.CheckpointName,
		"type", cfg.BackupType, "readers", cfg.Readers, "rateLimitBytesPerSecond", cfg.RateLimitBytes)

	store, err := uploader.InitObjectStore(&cfg.ObjectStoreConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}
	k8sClient, err := uploader.NewKubeClient()
	if err != nil {
		return fmt.Errorf("failed to create K8s client: %w", err)
	}

	err = runTransfer(ctx, cfg, store, k8sClient, logger)
	if err != nil {
		// Any failure ends the backup at once: an open pull backup keeps the
		// copy-before-write scratch volume growing, and a paused VM only
		// resumes when the backup is gone. Ending it without an index entry
		// makes the next backup a full one.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		deleteVMB(cleanupCtx, k8sClient, cfg, logger)
		_ = os.WriteFile(terminationLogPath, []byte(err.Error()), 0o644) //nolint:gosec // read by the kubelet
		return err
	}
	logger.Info("Pull transfer completed successfully")
	return nil
}

func deleteVMB(ctx context.Context, k8sClient client.Client, cfg *Config, logger logr.Logger) {
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	vmb.Name, vmb.Namespace = cfg.VMBName, cfg.VMNamespace
	if err := k8sClient.Delete(ctx, vmb); err != nil && !apierrors.IsNotFound(err) {
		logger.Info("Failed to delete VirtualMachineBackup after a failed transfer", "vmb", cfg.VMBName, "reason", err.Error())
		return
	}
	logger.Info("Deleted VirtualMachineBackup after a failed transfer", "vmb", cfg.VMBName)
}

// runTransfer moves all volumes of the backup and records the checkpoint.
func runTransfer(parent context.Context, cfg *Config, store velero.ObjectStore, k8sClient client.Client, logger logr.Logger) error {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	vmb := &unstructured.Unstructured{}
	vmb.SetGroupVersionKind(kubevirtbackupv1alpha1.SchemeGroupVersion.WithKind("VirtualMachineBackup"))
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: cfg.VMBName, Namespace: cfg.VMNamespace}, vmb); err != nil {
		return fmt.Errorf("failed to read VirtualMachineBackup %s/%s: %w", cfg.VMNamespace, cfg.VMBName, err)
	}
	info, err := parseExportInfo(vmb)
	if err != nil {
		return err
	}
	tokenData, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("failed to read export token: %w", err)
	}
	token := strings.TrimSpace(string(tokenData))

	go watchPaused(ctx, k8sClient, cfg, cancel, logger)

	progress := &Progress{}
	opts := transferDefaults()
	opts.Readers = cfg.Readers
	opts.Limiter = NewRateLimiter(cfg.RateLimitBytes)
	opts.Progress = progress

	// Plan all volumes first so the progress total is known from the start.
	type volumeJob struct {
		vol    exportVolume
		client *ExportClient
		plan   *BlockPlan
	}
	jobs := make([]volumeJob, 0, len(info.Volumes))
	for _, vol := range info.Volumes {
		exportClient, err := NewExportClient(info.CACert, token, vol.DataURL, vol.MapURL)
		if err != nil {
			return err
		}
		extents, err := exportClient.Map(ctx)
		if err != nil {
			return fmt.Errorf("volume %s: %w", vol.Name, causeOr(ctx, err))
		}
		plan, err := PlanBlocks(extents, DefaultBlockSize, vol.Incremental)
		if err != nil {
			return fmt.Errorf("volume %s: %w", vol.Name, err)
		}
		logger.Info("Planned volume", "volume", vol.Name, "incremental", vol.Incremental,
			"diskSize", plan.DiskSize, "extents", len(extents),
			"blocksToRead", len(plan.Read), "blocksZeroInMap", len(plan.Zero), "bytesToRead", plan.ReadBytes())
		progress.AddTotal(plan.ReadBytes())
		jobs = append(jobs, volumeJob{vol: vol, client: exportClient, plan: plan})
	}

	stopProgress := make(chan struct{})
	defer close(stopProgress)
	go logProgress(progress, cfg.ProgressInterval, stopProgress, logger)

	var dataFiles, indexFiles []uploader.CheckpointFile
	indexes := make([]*BlockIndex, 0, len(jobs))
	for _, job := range jobs {
		dataPath := uploader.GetQCOWPath(cfg.VMNamespace, cfg.VMName, cfg.CheckpointName, DataObjectName(job.vol.Name))
		started := time.Now()
		entries, dataSize, err := TransferVolume(ctx, job.client, job.plan, func(r io.Reader) error {
			return uploader.PutObjectStream(store, cfg.BSLBucket, dataPath, r, partSizeFor(job.plan.DiskSize))
		}, opts)
		if err != nil {
			return fmt.Errorf("volume %s: %w", job.vol.Name, causeOr(ctx, err))
		}
		ix := &BlockIndex{
			Version:     IndexVersion,
			Format:      uploader.CheckpointFormatBlocks,
			VMNamespace: cfg.VMNamespace,
			VMName:      cfg.VMName,
			Volume:      job.vol.Name,
			Checkpoint:  cfg.CheckpointName,
			BackupType:  uploader.BackupTypeFull,
			DiskSize:    job.plan.DiskSize,
			BlockSize:   job.plan.BlockSize,
			Compression: compressionZstd,
			Checksum:    checksumSHA256,
			DataObject:  dataPath,
			DataSize:    dataSize,
			Blocks:      entries,
		}
		if job.vol.Incremental {
			ix.BackupType = uploader.BackupTypeIncremental
		}
		var zero int64
		for _, e := range entries {
			if e.Zero {
				zero++
			}
		}
		elapsed := time.Since(started)
		logger.Info("Volume transferred", "volume", job.vol.Name, "bytesRead", job.plan.ReadBytes(),
			"blocks", len(entries), "zeroBlocks", zero, "storedBytes", dataSize,
			"seconds", int64(elapsed.Seconds()),
			"readMiBPerSecond", int64(float64(job.plan.ReadBytes())/(1<<20)/max(elapsed.Seconds(), 0.001)))
		indexes = append(indexes, ix)
		dataFiles = append(dataFiles, uploader.CheckpointFile{
			Filename: DataObjectName(job.vol.Name), DiskName: job.vol.Name, Size: dataSize, ObjectPath: dataPath,
		})
	}
	logDone, logTotal := progress.Snapshot()
	logger.Info(ProgressMessage, "bytesDone", logDone, "totalBytes", logTotal)

	// A VM that paused during the last reads must not get a checkpoint.
	if err := context.Cause(ctx); err != nil {
		return err
	}

	// Everything from here on makes the checkpoint visible. The block index
	// objects and then the per-VM index entry are written only now that
	// every block is stored; the backup is deleted last (CommitCheckpoint).
	parent2, err := parentCheckpoint(store, cfg, logger)
	if err != nil {
		return err
	}
	for _, ix := range indexes {
		if !ix.IsFull() {
			ix.Parent = parent2
			if err := checkParentIndex(store, cfg, ix); err != nil {
				return err
			}
		}
		encoded, err := EncodeIndex(ix)
		if err != nil {
			return fmt.Errorf("volume %s: %w", ix.Volume, err)
		}
		indexPath := IndexPathForData(ix.DataObject)
		if err := uploader.PutObjectBytes(store, cfg.BSLBucket, indexPath, encoded); err != nil {
			return fmt.Errorf("failed to write block index %s: %w", indexPath, err)
		}
		// Index objects carry no disk name: the per-VM index keeps one
		// named file per disk, aligned with its PVC list.
		indexFiles = append(indexFiles, uploader.CheckpointFile{
			Filename: IndexObjectName(ix.Volume), Size: int64(len(encoded)), ObjectPath: indexPath,
		})
	}
	files := append(dataFiles, indexFiles...)
	if err := uploader.CommitCheckpoint(ctx, store, k8sClient, cfg.UploaderConfig, files, logger); err != nil {
		return causeOr(ctx, err)
	}
	return nil
}

// parentCheckpoint returns the checkpoint an incremental backup builds on:
// the latest entry of the per-VM index.
func parentCheckpoint(store velero.ObjectStore, cfg *Config, logger logr.Logger) (string, error) {
	if !strings.EqualFold(cfg.BackupType, uploader.BackupTypeIncremental) {
		return "", nil
	}
	vmIndex, found, err := uploader.GetVMIndex(store, cfg.VMNamespace, cfg.VMName, cfg.BSLBucket, logger)
	if err != nil {
		return "", err
	}
	if !found || len(vmIndex.Checkpoints) == 0 {
		return "", fmt.Errorf("incremental backup but the VM has no checkpoint in the object store")
	}
	return vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1].ID, nil
}

// checkParentIndex makes sure an incremental index can be merged onto its
// parent: same disk, same geometry. A resized disk needs a new full backup.
func checkParentIndex(store velero.ObjectStore, cfg *Config, ix *BlockIndex) error {
	parentData := uploader.GetQCOWPath(cfg.VMNamespace, cfg.VMName, ix.Parent, DataObjectName(ix.Volume))
	parentIndex, err := LoadIndex(store, cfg.BSLBucket, IndexPathForData(parentData))
	if err != nil {
		return fmt.Errorf("incremental backup of volume %s has no usable parent: %w", ix.Volume, err)
	}
	if parentIndex.DiskSize != ix.DiskSize || parentIndex.BlockSize != ix.BlockSize {
		return fmt.Errorf("volume %s changed from %d to %d bytes since checkpoint %s; a full backup is needed",
			ix.Volume, parentIndex.DiskSize, ix.DiskSize, ix.Parent)
	}
	return nil
}

// causeOr returns the cancellation cause of ctx if there is one (for example
// ErrVMPaused), else err.
func causeOr(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// partSizeFor picks a multipart part size that fits a data object of up to
// diskSize bytes into the part count limit of the object store.
func partSizeFor(diskSize int64) int64 {
	const mib = 1 << 20
	size := (diskSize/maxUploadParts + mib) / mib * mib
	return max(size, minPartSize)
}

// vmiPaused reports whether a VirtualMachineInstance has the Paused condition.
func vmiPaused(vmi *kubevirtcorev1.VirtualMachineInstance) (bool, string) {
	for _, c := range vmi.Status.Conditions {
		if c.Type == kubevirtcorev1.VirtualMachineInstancePaused && c.Status == corev1.ConditionTrue {
			return true, strings.TrimSpace(c.Reason + " " + c.Message)
		}
	}
	return false, ""
}

// watchPaused cancels the transfer as soon as the VM reports Paused. A VM
// pauses when the copy-before-write scratch volume of its backup is full,
// and it stays paused until the backup ends.
func watchPaused(
	ctx context.Context, k8sClient client.Client, cfg *Config, cancel context.CancelCauseFunc, logger logr.Logger,
) {
	ticker := time.NewTicker(pausePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		vmi := &kubevirtcorev1.VirtualMachineInstance{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: cfg.VMName, Namespace: cfg.VMNamespace}, vmi); err != nil {
			if ctx.Err() == nil {
				logger.V(1).Info("Could not read VirtualMachineInstance for the pause check", "reason", err.Error())
			}
			continue
		}
		if paused, detail := vmiPaused(vmi); paused {
			logger.Info("VirtualMachineInstance is paused, ending the backup", "detail", detail)
			cancel(fmt.Errorf("%w during the backup (%s); the backup was ended to let it resume", ErrVMPaused, detail))
			return
		}
	}
}

func logProgress(progress *Progress, every time.Duration, stop <-chan struct{}, logger logr.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			done, total := progress.Snapshot()
			logger.Info(ProgressMessage, "bytesDone", done, "totalBytes", total)
		}
	}
}
