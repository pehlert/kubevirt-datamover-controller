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
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

const testToken = "s3cret"

// exportServer imitates the export endpoints of one volume.
type exportServer struct {
	*httptest.Server
	mu          sync.Mutex
	disk        []byte
	extents     []Extent
	truncate    bool
	dataRequest int
}

func newExportServer(t *testing.T, disk []byte, extents []Extent) *exportServer {
	t.Helper()
	s := &exportServer{disk: disk, extents: extents}
	mux := http.NewServeMux()
	mux.HandleFunc("/exports/root/map", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(exportTokenHeader) != testToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Two extents per page to exercise the paging.
		offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		page := mapPage{}
		for i, e := range s.extents {
			if e.Offset < offset {
				continue
			}
			if len(page.Extents) == 2 {
				next := s.extents[i].Offset
				page.NextOffset = &next
				break
			}
			page.Extents = append(page.Extents, e)
		}
		_ = json.NewEncoder(w).Encode(page)
	})
	mux.HandleFunc("/exports/root/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(exportTokenHeader) != testToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		length, _ := strconv.ParseInt(r.URL.Query().Get("length"), 10, 64)
		s.mu.Lock()
		s.dataRequest++
		truncate := s.truncate
		s.mu.Unlock()
		chunk := s.disk[offset:min(offset+length, int64(len(s.disk)))]
		if truncate {
			chunk = chunk[:len(chunk)/2]
		}
		_, _ = w.Write(chunk)
	})
	s.Server = httptest.NewTLSServer(mux)
	t.Cleanup(s.Close)
	return s
}

func (s *exportServer) caPEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
}

// recordingStore notes the order of writes and can fail chosen keys.
type recordingStore struct {
	*uploader.MockObjectStore
	mu      sync.Mutex
	events  *[]string
	failPut string
}

func (r *recordingStore) PutObject(bucket, key string, body io.Reader) error {
	if r.failPut != "" && strings.HasSuffix(key, r.failPut) {
		_, _ = io.Copy(io.Discard, io.LimitReader(body, 1))
		return errors.New("object store rejected the upload")
	}
	if err := r.MockObjectStore.PutObject(bucket, key, body); err != nil {
		return err
	}
	r.mu.Lock()
	*r.events = append(*r.events, "put "+key)
	r.mu.Unlock()
	return nil
}

type transferEnv struct {
	cfg     *Config
	store   *recordingStore
	client  client.Client
	server  *exportServer
	events  []string
	vmbType string
	paused  bool
	mu      sync.Mutex
}

func newTransferEnv(t *testing.T, disk []byte, extents []Extent, backupType string) *transferEnv {
	t.Helper()
	env := &transferEnv{server: newExportServer(t, disk, extents), vmbType: "Full"}
	if backupType == uploader.BackupTypeIncremental {
		env.vmbType = "Incremental"
	}
	env.store = &recordingStore{MockObjectStore: uploader.NewMockObjectStore(testBucket, ""), events: &env.events}

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, kubevirtbackupv1alpha1.AddToScheme(scheme))
	require.NoError(t, kubevirtcorev1.AddToScheme(scheme))
	apiGroup := "backup.kubevirt.io"
	objects := []client.Object{
		&kubevirtbackupv1alpha1.VirtualMachineBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "vmb1", Namespace: "ns"},
			Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
				Source: corev1.TypedLocalObjectReference{APIGroup: &apiGroup, Kind: "VirtualMachineBackupTracker", Name: "vmbt1"},
			},
		},
		&kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: "vm", Namespace: "ns"},
			Spec: kubevirtcorev1.VirtualMachineSpec{Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtcorev1.VirtualMachineInstanceSpec{Volumes: []kubevirtcorev1.Volume{{
					Name: "root",
					VolumeSource: kubevirtcorev1.VolumeSource{PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
						PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: "vm-root"},
					}},
				}}},
			}},
		},
		&kubevirtcorev1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: "vm", Namespace: "ns"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "vm-root", Namespace: "ns"},
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			}},
		},
	}
	env.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			env.mu.Lock()
			defer env.mu.Unlock()
			switch o := obj.(type) {
			case *unstructured.Unstructured:
				// The fields the transfer reads are newer than the typed API.
				o.SetName(key.Name)
				o.SetNamespace(key.Namespace)
				o.Object["status"] = map[string]any{
					"conditions":      []any{map[string]any{"type": "Progressing", "status": "True", "reason": ExportReadyReason}},
					"includedVolumes": []any{map[string]any{"volumeName": "root", "type": env.vmbType}},
					"links": map[string]any{"internal": map[string]any{
						"cert": env.server.caPEM(),
						"volumes": []any{map[string]any{
							"volumeName":   "root",
							"dataEndpoint": env.server.URL + "/exports/root/data",
							"mapEndpoint":  env.server.URL + "/exports/root/map",
						}},
					}},
				}
				return nil
			case *kubevirtcorev1.VirtualMachineInstance:
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if env.paused {
					o.Status.Conditions = []kubevirtcorev1.VirtualMachineInstanceCondition{{
						Type: kubevirtcorev1.VirtualMachineInstancePaused, Status: corev1.ConditionTrue, Reason: "PausedIOError",
					}}
				}
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*kubevirtbackupv1alpha1.VirtualMachineBackup); ok {
				env.store.mu.Lock()
				env.events = append(env.events, "delete vmb")
				env.store.mu.Unlock()
			}
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600))
	env.cfg = &Config{
		UploaderConfig: &uploader.UploaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: testBucket},
			VMName:            "vm", VMNamespace: "ns", CheckpointName: "cp-new", BackupType: backupType,
			ExpectedBackupType: backupType, VeleroBackupName: "backup1", VMBName: "vmb1", VMBTName: "vmbt1",
			Format: uploader.CheckpointFormatBlocks,
		},
		TokenFile: tokenFile, Readers: 2, ProgressInterval: time.Hour,
	}
	return env
}

func (e *transferEnv) run() error {
	return runTransfer(context.Background(), e.cfg, e.store, e.client, logr.Discard())
}

func (e *transferEnv) hasObject(suffix string) bool {
	for key := range e.store.GetAllObjects() {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

func (e *transferEnv) eventIndex(t *testing.T, needle string) int {
	t.Helper()
	for i, ev := range e.events {
		if strings.Contains(ev, needle) {
			return i
		}
	}
	t.Fatalf("no event containing %q in %v", needle, e.events)
	return -1
}

func bigBlockDisk(t *testing.T, blocks int, zero map[int]bool) []byte {
	t.Helper()
	// The pull subcommand always uses DefaultBlockSize; keep the disk small
	// by making most blocks zero.
	data := make([]byte, int64(blocks)*DefaultBlockSize)
	for n := range blocks {
		if !zero[n] {
			pattern := []byte(fmt.Sprintf("block-%d-", n))
			copy(data[int64(n)*DefaultBlockSize:], bytes.Repeat(pattern, 64))
		}
	}
	return data
}

func TestRunTransferCommitsInOrder(t *testing.T) {
	disk := bigBlockDisk(t, 6, map[int]bool{1: true, 4: true, 5: true})
	extents := []Extent{
		{Offset: 0, Length: 3 * DefaultBlockSize, Type: 0},
		{Offset: 3 * DefaultBlockSize, Length: DefaultBlockSize, Type: 0},
		{Offset: 4 * DefaultBlockSize, Length: 2 * DefaultBlockSize, Type: 3},
	}
	env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
	require.NoError(t, env.run())

	// Checkpoint ordering rule: all data, then the block index, then the
	// per-VM index entry, and only then the end of the backup.
	data := env.eventIndex(t, "cp-new/root.blocks")
	blockIndex := env.eventIndex(t, "cp-new/root.blocks.idx")
	vmIndex := env.eventIndex(t, "checkpoints/ns/vm/index.json")
	manifest := env.eventIndex(t, "manifests/backup1/ns-vm.json")
	deleted := env.eventIndex(t, "delete vmb")
	assert.Less(t, data, blockIndex)
	assert.Less(t, blockIndex, vmIndex)
	assert.Less(t, vmIndex, manifest)
	assert.Less(t, manifest, deleted)
	assert.Equal(t, len(env.events)-1, deleted, "nothing is written after the backup was ended")

	vmIdx, found, err := uploader.GetVMIndex(env.store, "ns", "vm", testBucket, logr.Discard())
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, vmIdx.Checkpoints, 1)
	entry := vmIdx.Checkpoints[0]
	assert.Equal(t, uploader.CheckpointFormatBlocks, entry.Format)
	assert.Equal(t, uploader.BackupTypeFull, entry.Type)
	assert.Equal(t, []string{"vm-root"}, entry.PVCs)
	require.Len(t, entry.Files, 2)
	assert.Equal(t, "root", entry.Files[0].DiskName)
	assert.Empty(t, entry.Files[1].DiskName, "the index object is not a disk of its own")

	// Deleting a checkpoint goes by its file list, so the list must name
	// every object the transfer wrote into the checkpoint directory.
	listed := map[string]bool{entry.VMBObjectPath: true, entry.VMBTObjectPath: true}
	for _, f := range entry.Files {
		assert.Equal(t, uploader.GetQCOWPath("ns", "vm", "cp-new", f.Filename), f.ObjectPath)
		listed[f.ObjectPath] = true
	}
	for key := range env.store.GetAllObjects() {
		if strings.HasPrefix(key, "checkpoints/ns/vm/cp-new/") {
			assert.True(t, listed[key], "object %s is not referenced by the checkpoint entry", key)
		}
	}

	// And the stored checkpoint restores to the same bytes.
	target := filepath.Join(t.TempDir(), "disk.img")
	require.NoError(t, RestoreChain(context.Background(), env.store, testBucket, []string{entry.Files[0].ObjectPath}, target, RestoreOptions{}, logr.Discard()))
	restored, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(disk, restored))

	verified, err := VerifyVM(context.Background(), env.store, testBucket, "ns", "vm", "cp-new", 2, logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, 1, verified)
}

// assertNoCheckpoint checks what the checkpoint safety rule promises after a
// failed transfer: no per-VM index entry, and the backup was not ended by
// the commit path (the caller ends it, without an entry).
func assertNoCheckpoint(t *testing.T, env *transferEnv) {
	t.Helper()
	assert.False(t, env.hasObject("checkpoints/ns/vm/index.json"), "a failed transfer must not record a checkpoint")
	assert.False(t, env.hasObject(".blocks.idx"), "a failed transfer must not write a block index")
	assert.False(t, env.hasObject("manifests/backup1/index.json"))
	for _, ev := range env.events {
		assert.NotEqual(t, "delete vmb", ev)
	}
}

func TestRunTransferFailureLeavesNoCheckpoint(t *testing.T) {
	disk := bigBlockDisk(t, 3, nil)
	extents := fullExtents(int64(len(disk)))

	t.Run("upload fails", func(t *testing.T) {
		env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
		env.store.failPut = DataObjectSuffix
		require.ErrorContains(t, env.run(), "rejected the upload")
		assertNoCheckpoint(t, env)
	})
	t.Run("block index cannot be written", func(t *testing.T) {
		env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
		env.store.failPut = IndexObjectSuffix
		require.ErrorContains(t, env.run(), "failed to write block index")
		assert.False(t, env.hasObject("checkpoints/ns/vm/index.json"))
	})
	t.Run("export keeps returning short data", func(t *testing.T) {
		env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
		env.server.truncate = true
		err := runTransferWithOptions(env, func(o *TransferOptions) { o.Retries, o.RetryDelay = 2, time.Millisecond })
		require.ErrorContains(t, err, "short read")
		assertNoCheckpoint(t, env)
	})
	t.Run("wrong token", func(t *testing.T) {
		env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
		require.NoError(t, os.WriteFile(env.cfg.TokenFile, []byte("wrong"), 0o600))
		require.ErrorContains(t, env.run(), "HTTP 401")
		assertNoCheckpoint(t, env)
	})
	t.Run("VM pauses during the backup", func(t *testing.T) {
		oldInterval := pausePollInterval
		pausePollInterval = 5 * time.Millisecond
		defer func() { pausePollInterval = oldInterval }()
		env := newTransferEnv(t, disk, extents, uploader.BackupTypeFull)
		env.paused = true
		// Keep the transfer busy long enough for the watchdog to look.
		env.cfg.RateLimitBytes = DefaultBlockSize
		err := env.run()
		require.ErrorIs(t, err, ErrVMPaused)
		assertNoCheckpoint(t, env)
	})
}

// runTransferWithOptions runs the transfer with adjusted retry settings.
func runTransferWithOptions(env *transferEnv, adjust func(*TransferOptions)) error {
	oldDefaults := transferDefaults
	transferDefaults = func() TransferOptions {
		o := oldDefaults()
		adjust(&o)
		return o
	}
	defer func() { transferDefaults = oldDefaults }()
	return env.run()
}

func TestRunTransferIncremental(t *testing.T) {
	disk := bigBlockDisk(t, 4, nil)
	full := newTransferEnv(t, disk, fullExtents(int64(len(disk))), uploader.BackupTypeFull)
	full.cfg.CheckpointName = "cp1"
	require.NoError(t, full.run())

	// Second backup of the same VM into the same store: one dirty block.
	disk2 := append([]byte(nil), disk...)
	copy(disk2[2*DefaultBlockSize:], []byte("changed"))
	dirty := []Extent{
		{Offset: 0, Length: 2 * DefaultBlockSize, Type: 0},
		{Offset: 2 * DefaultBlockSize, Length: 4096, Type: 1},
		{Offset: 2*DefaultBlockSize + 4096, Length: 2*DefaultBlockSize - 4096, Type: 0},
	}
	inc := newTransferEnv(t, disk2, dirty, uploader.BackupTypeIncremental)
	inc.store.MockObjectStore = full.store.MockObjectStore
	inc.cfg.CheckpointName = "cp2"
	inc.cfg.VeleroBackupName = "backup2"
	require.NoError(t, inc.run())
	assert.Equal(t, 1, inc.server.dataRequest, "only the dirty block is read")

	vmIdx, _, err := uploader.GetVMIndex(inc.store, "ns", "vm", testBucket, logr.Discard())
	require.NoError(t, err)
	require.Len(t, vmIdx.Checkpoints, 2)
	assert.Equal(t, "cp1", vmIdx.Checkpoints[1].Parent)
	assert.Equal(t, uploader.BackupTypeIncremental, vmIdx.Checkpoints[1].Type)
	assert.ElementsMatch(t, []string{"backup1", "backup2"}, vmIdx.Checkpoints[0].ReferencedBy,
		"the full is kept alive by the incremental that depends on it")

	target := filepath.Join(t.TempDir(), "disk.img")
	require.NoError(t, RestoreChain(context.Background(), inc.store, testBucket,
		[]string{vmIdx.Checkpoints[0].Files[0].ObjectPath, vmIdx.Checkpoints[1].Files[0].ObjectPath}, target, RestoreOptions{}, logr.Discard()))
	restored, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(disk2, restored))
}

func TestRunTransferRefusesIncrementalOntoOtherFormat(t *testing.T) {
	disk := bigBlockDisk(t, 2, nil)
	dirty := []Extent{{Offset: 0, Length: 4096, Type: 1}, {Offset: 4096, Length: int64(len(disk)) - 4096, Type: 0}}
	env := newTransferEnv(t, disk, dirty, uploader.BackupTypeIncremental)
	qcow := uploader.CheckpointFile{Filename: "vmb-root.qcow2", DiskName: "root", ObjectPath: "checkpoints/ns/vm/old/vmb-root.qcow2"}
	require.NoError(t, env.store.MockObjectStore.PutObject(testBucket, qcow.ObjectPath, strings.NewReader("qcow2")))
	require.NoError(t, uploader.PutVMIndex(env.store, "ns", "vm", testBucket, uploader.VMIndex{
		VMName: "vm", Namespace: "ns",
		Checkpoints: []uploader.CheckpointEntry{{ID: "old", Type: uploader.BackupTypeFull, Files: []uploader.CheckpointFile{qcow}}},
	}))
	env.events = nil

	require.Error(t, env.run())
	vmIdx, _, err := uploader.GetVMIndex(env.store, "ns", "vm", testBucket, logr.Discard())
	require.NoError(t, err)
	require.Len(t, vmIdx.Checkpoints, 1, "the mixed-format incremental was not recorded")
	for _, ev := range env.events {
		assert.NotEqual(t, "delete vmb", ev)
	}
}

func TestParseExportInfo(t *testing.T) {
	build := func(mutate func(status map[string]any)) *unstructured.Unstructured {
		status := map[string]any{
			"conditions":      []any{map[string]any{"type": "Progressing", "status": "True", "reason": ExportReadyReason}},
			"includedVolumes": []any{map[string]any{"volumeName": "root", "type": "Full"}, map[string]any{"volumeName": "data", "type": "Incremental"}},
			"links": map[string]any{"internal": map[string]any{
				"cert": "PEM",
				"volumes": []any{
					map[string]any{"volumeName": "root", "dataEndpoint": "https://h/exports/root/data", "mapEndpoint": "https://h/exports/root/map"},
					map[string]any{"volumeName": "data", "dataEndpoint": "https://h/exports/data/data", "mapEndpoint": "https://h/exports/data/map"},
				},
			}},
		}
		if mutate != nil {
			mutate(status)
		}
		u := &unstructured.Unstructured{Object: map[string]any{"status": status}}
		u.SetName("vmb1")
		return u
	}
	info, err := parseExportInfo(build(nil))
	require.NoError(t, err)
	require.Len(t, info.Volumes, 2)
	assert.False(t, info.Volumes[0].Incremental)
	assert.True(t, info.Volumes[1].Incremental)
	assert.Equal(t, "https://h/exports/data/map", info.Volumes[1].MapURL)

	_, err = parseExportInfo(build(func(s map[string]any) {
		s["conditions"] = []any{map[string]any{"type": "Progressing", "status": "True", "reason": "Initializing"}}
	}))
	require.ErrorContains(t, err, "not ready")
	_, err = parseExportInfo(build(func(s map[string]any) { delete(s, "includedVolumes") }))
	require.ErrorContains(t, err, "no backup type")
	_, err = parseExportInfo(build(func(s map[string]any) { delete(s, "links") }))
	require.Error(t, err)
}

func TestVMIPaused(t *testing.T) {
	vmi := &kubevirtcorev1.VirtualMachineInstance{}
	paused, _ := vmiPaused(vmi)
	assert.False(t, paused)
	vmi.Status.Conditions = []kubevirtcorev1.VirtualMachineInstanceCondition{
		{Type: kubevirtcorev1.VirtualMachineInstanceReady, Status: corev1.ConditionTrue},
		{Type: kubevirtcorev1.VirtualMachineInstancePaused, Status: corev1.ConditionFalse},
	}
	paused, _ = vmiPaused(vmi)
	assert.False(t, paused)
	vmi.Status.Conditions[1].Status = corev1.ConditionTrue
	vmi.Status.Conditions[1].Reason = "PausedIOError"
	paused, detail := vmiPaused(vmi)
	assert.True(t, paused)
	assert.Contains(t, detail, "PausedIOError")
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv(common.EnvBSLBucket, "b")
	t.Setenv(uploader.EnvVMName, "vm")
	t.Setenv(uploader.EnvVMNamespace, "ns")
	t.Setenv(uploader.EnvCheckpointName, "cp")
	t.Setenv(uploader.EnvVMBName, "vmb")
	cfg, err := LoadConfigFromEnv()
	require.NoError(t, err)
	assert.Equal(t, uploader.CheckpointFormatBlocks, cfg.Format)
	assert.Equal(t, 4, cfg.Readers)
	assert.Equal(t, DefaultTokenFile, cfg.TokenFile)
	assert.Zero(t, cfg.RateLimitBytes)

	t.Setenv(EnvReaders, "8")
	t.Setenv(EnvRateLimitMiB, "100")
	cfg, err = LoadConfigFromEnv()
	require.NoError(t, err)
	assert.Equal(t, 8, cfg.Readers)
	assert.Equal(t, int64(100<<20), cfg.RateLimitBytes)

	t.Setenv(EnvReaders, "0")
	_, err = LoadConfigFromEnv()
	require.Error(t, err)
	t.Setenv(EnvReaders, "4")
	t.Setenv(uploader.EnvVMBName, "")
	_, err = LoadConfigFromEnv()
	require.Error(t, err)
}
