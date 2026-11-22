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

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/puller"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

const (
	pullVMNamespace = "vms"
	pullOADPNS      = "backup-system"
	pullVMName      = "vm1"
	pullDUName      = "du1"
	pullDUUID       = "0a0a0a0a-1111-2222-3333-444444444444"
	pullBucket      = "bucket"
)

func pullScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, velerov1.AddToScheme(scheme))
	require.NoError(t, velerov2alpha1.AddToScheme(scheme))
	require.NoError(t, kubevirtbackupv1alpha1.AddToScheme(scheme))
	require.NoError(t, kubevirtcorev1.AddToScheme(scheme))
	return scheme
}

func pullDataUpload(phase velerov2alpha1.DataUploadPhase, annotations map[string]string) *velerov2alpha1.DataUpload {
	all := map[string]string{
		common.AnnotationVMName:      pullVMName,
		common.AnnotationVMNamespace: pullVMNamespace,
	}
	for k, v := range annotations {
		all[k] = v
	}
	return &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: pullDUName, Namespace: pullOADPNS, UID: types.UID(pullDUUID), Annotations: all,
			Labels: map[string]string{common.LabelVeleroBackupName: "backup1"},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt, SourceNamespace: pullVMNamespace, BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{Phase: phase},
	}
}

func pullVM(annotations map[string]string) *kubevirtcorev1.VirtualMachine {
	return &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: pullVMName, Namespace: pullVMNamespace, Annotations: annotations},
		Spec: kubevirtcorev1.VirtualMachineSpec{Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
			Spec: kubevirtcorev1.VirtualMachineInstanceSpec{Volumes: []kubevirtcorev1.Volume{{
				Name: "root",
				VolumeSource: kubevirtcorev1.VolumeSource{PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: "vm1-root"},
				}},
			}}},
		}},
	}
}

func pullBaseObjects() []client.Object {
	return []client.Object{
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "vm1-root", Namespace: pullVMNamespace},
			Status:     corev1.PersistentVolumeClaimStatus{Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")}},
		},
		&velerov1.BackupStorageLocation{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: pullOADPNS},
			Spec: velerov1.BackupStorageLocationSpec{
				Provider:    "aws",
				StorageType: velerov1.StorageType{ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: pullBucket, Prefix: "p"}},
				Credential:  &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}, Key: "cloud"},
			},
			Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: pullOADPNS},
			Data:       map[string][]byte{"cloud": []byte("[default]\n")},
		},
	}
}

// pullEnv is a reconciler on a fake client that records the unstructured
// VirtualMachineBackups it is asked to create: the typed scheme would drop
// the pull-mode fields the vendored API does not know.
type pullEnv struct {
	r          *KubeVirtDataUploadReconciler
	client     client.Client
	store      *uploader.MockObjectStore
	createdVMB []*unstructured.Unstructured
}

func newPullEnv(t *testing.T, objects ...client.Object) *pullEnv {
	t.Helper()
	env := &pullEnv{store: uploader.NewMockObjectStore(pullBucket, "p-kubevirt-datamover")}
	scheme := pullScheme(t)
	env.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "VirtualMachineBackup" {
					env.createdVMB = append(env.createdVMB, u.DeepCopy())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	env.r = &KubeVirtDataUploadReconciler{
		Client: env.client, APIReader: env.client, Scheme: scheme, Log: logr.Discard(),
		OADPNamespace: pullOADPNS, DatamoverImage: "example.invalid/datamover:test",
		ObjectStoreFactory: func(*common.ObjectStoreConfig) (velero.ObjectStore, error) { return env.store, nil },
	}
	return env
}

func (e *pullEnv) du(t *testing.T) *velerov2alpha1.DataUpload {
	t.Helper()
	du := &velerov2alpha1.DataUpload{}
	require.NoError(t, e.client.Get(context.Background(), types.NamespacedName{Name: pullDUName, Namespace: pullOADPNS}, du))
	return du
}

func TestEnsureBackupModePrecedence(t *testing.T) {
	cases := []struct {
		name         string
		flag         string
		vmAnnotation map[string]string
		duAnnotation map[string]string
		want         string
	}{
		{name: "default is push", want: common.BackupModePush},
		{name: "flag selects pull", flag: common.BackupModePull, want: common.BackupModePull},
		{name: "VM annotation overrides the flag", flag: common.BackupModePush,
			vmAnnotation: map[string]string{common.AnnotationBackupMode: common.BackupModePull}, want: common.BackupModePull},
		{name: "VM can opt out of pull", flag: common.BackupModePull,
			vmAnnotation: map[string]string{common.AnnotationBackupMode: common.BackupModePush}, want: common.BackupModePush},
		{name: "invalid VM annotation is ignored", flag: common.BackupModePull,
			vmAnnotation: map[string]string{common.AnnotationBackupMode: "sideways"}, want: common.BackupModePull},
		{name: "a recorded decision is kept", flag: common.BackupModePull,
			duAnnotation: map[string]string{common.AnnotationBackupMode: common.BackupModePush}, want: common.BackupModePush},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, tc.duAnnotation)
			env := newPullEnv(t, du, pullVM(tc.vmAnnotation))
			env.r.BackupMode = tc.flag
			require.NoError(t, env.r.ensureBackupMode(context.Background(), logr.Discard(), du, &common.VMReference{Name: pullVMName, Namespace: pullVMNamespace}))
			assert.Equal(t, tc.want, env.du(t).Annotations[common.AnnotationBackupMode])
			assert.Equal(t, tc.want == common.BackupModePull, isPullMode(du))
		})
	}
}

func TestHandleAcceptedPullCreatesSecretScratchAndPullBackup(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, nil)
	env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil))...)
	env.r.BackupMode = common.BackupModePull
	env.r.PullBackupTTL = 6 * time.Hour
	ctx := context.Background()

	result, err := env.r.handleAccepted(ctx, logr.Discard(), du)
	require.NoError(t, err)
	assert.Equal(t, RequeueAfterShort, result.RequeueAfter)

	// Scratch PVC: filesystem, disk capacity plus 10 %.
	pvcs := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, env.client.List(ctx, pvcs, client.InNamespace(pullVMNamespace), client.MatchingLabels{common.LabelDataUploadUID: pullDUUID}))
	require.Len(t, pvcs.Items, 1)
	scratch := pvcs.Items[0]
	require.NotNil(t, scratch.Spec.VolumeMode)
	assert.Equal(t, corev1.PersistentVolumeFilesystem, *scratch.Spec.VolumeMode)
	size := scratch.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, int64(110)<<30, size.Value())

	// Token secret in the VM namespace.
	secret := &corev1.Secret{}
	require.NoError(t, env.client.Get(ctx, types.NamespacedName{Name: common.PullTokenSecretPrefix + pullDUUID, Namespace: pullVMNamespace}, secret))
	assert.Len(t, secret.Data[common.PullTokenSecretKey], 64)

	// The backup is created in pull mode, with the token, a TTL and forced full (no checkpoint yet).
	require.Len(t, env.createdVMB, 1)
	spec := env.createdVMB[0].Object["spec"].(map[string]any)
	assert.Equal(t, "Pull", spec["mode"])
	assert.Equal(t, secret.Name, spec["tokenSecretRef"])
	assert.Equal(t, "6h0m0s", spec["ttlDuration"])
	assert.Equal(t, scratch.Name, spec["pvcName"])
	assert.Equal(t, true, spec["forceFullBackup"])
	assert.Equal(t, "VirtualMachineBackupTracker", spec["source"].(map[string]any)["kind"])

	stored := env.du(t)
	assert.Equal(t, common.BackupModePull, stored.Annotations[common.AnnotationBackupMode])
	assert.Equal(t, uploader.BackupTypeFull, stored.Annotations[common.AnnotationExpectedBackupType])

	// A second reconcile creates nothing new and keeps waiting for the export.
	_, err = env.r.handleAccepted(ctx, logr.Discard(), env.du(t))
	require.NoError(t, err)
	assert.Len(t, env.createdVMB, 1)
	assert.Equal(t, velerov2alpha1.DataUploadPhaseAccepted, env.du(t).Status.Phase)
}

func TestPushModeIsUnchanged(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, nil)
	env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil))...)
	ctx := context.Background()

	_, err := env.r.handleAccepted(ctx, logr.Discard(), du)
	require.NoError(t, err)
	assert.Empty(t, env.createdVMB, "a push backup is created through the typed API")
	vmbs := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	require.NoError(t, env.client.List(ctx, vmbs, client.InNamespace(pullVMNamespace)))
	require.Len(t, vmbs.Items, 1)
	assert.Nil(t, vmbs.Items[0].Spec.Mode)

	pvcs := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, env.client.List(ctx, pvcs, client.InNamespace(pullVMNamespace), client.MatchingLabels{common.LabelDataUploadUID: pullDUUID}))
	require.Len(t, pvcs.Items, 1)
	assert.Nil(t, pvcs.Items[0].Spec.VolumeMode)
	size := pvcs.Items[0].Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, int64(120)<<30, size.Value())

	secrets := &corev1.SecretList{}
	require.NoError(t, env.client.List(ctx, secrets, client.InNamespace(pullVMNamespace)))
	assert.Empty(t, secrets.Items)

	// A terminal push DataUpload is left alone by the pull cleanup.
	stored := env.du(t)
	result, err := env.r.finishPullCleanup(ctx, logr.Discard(), stored)
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
	assert.NotContains(t, env.du(t).Annotations, common.AnnotationPullCleanupDone)
}

// seedCheckpoint writes a per-VM index with one full checkpoint in the given
// format and puts a matching tracker on the cluster.
func seedCheckpoint(t *testing.T, env *pullEnv, format string) {
	t.Helper()
	file := uploader.CheckpointFile{Filename: "f", DiskName: "root", ObjectPath: "checkpoints/vms/vm1/cp1/f"}
	require.NoError(t, env.store.PutObjectBytes(file.ObjectPath, []byte("x")))
	require.NoError(t, uploader.PutVMIndex(env.store, pullVMNamespace, pullVMName, pullBucket, uploader.VMIndex{
		VMName: pullVMName, Namespace: pullVMNamespace,
		Checkpoints: []uploader.CheckpointEntry{{ID: "cp1", Type: uploader.BackupTypeFull, Format: format, Files: []uploader.CheckpointFile{file}}},
	}))
}

func trackerWithCheckpoint(checkpoint string) *kubevirtbackupv1alpha1.VirtualMachineBackupTracker {
	apiGroup := "kubevirt.io"
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vmbt-vm1", Namespace: pullVMNamespace,
			Labels: map[string]string{common.LabelVMNameHash: common.HashForLabel(pullVMName)},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{APIGroup: &apiGroup, Kind: "VirtualMachine", Name: pullVMName},
		},
	}
	if checkpoint != "" {
		vmbt.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{Name: checkpoint},
		}
	}
	return vmbt
}

func TestPullBackupTypeDecision(t *testing.T) {
	cases := []struct {
		name            string
		indexFormat     string
		trackerCP       string
		wantForcedFull  bool
		wantExpectation string
	}{
		{name: "tracker and index agree: incremental", indexFormat: uploader.CheckpointFormatBlocks, trackerCP: "cp1",
			wantForcedFull: false, wantExpectation: uploader.BackupTypeIncremental},
		{name: "tracker ahead of the index (a backup ended without being stored): full",
			indexFormat: uploader.CheckpointFormatBlocks, trackerCP: "cp2-never-stored",
			wantForcedFull: true, wantExpectation: uploader.BackupTypeFull},
		{name: "latest checkpoint is qcow2 (VM switched to pull): full",
			indexFormat: uploader.CheckpointFormatQcow2, trackerCP: "cp1",
			wantForcedFull: true, wantExpectation: uploader.BackupTypeFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, nil)
			env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil), trackerWithCheckpoint(tc.trackerCP))...)
			env.r.BackupMode = common.BackupModePull
			seedCheckpoint(t, env, tc.indexFormat)

			_, err := env.r.handleAccepted(context.Background(), logr.Discard(), du)
			require.NoError(t, err)
			require.Len(t, env.createdVMB, 1)
			forced, _, _ := unstructured.NestedBool(env.createdVMB[0].Object, "spec", "forceFullBackup")
			assert.Equal(t, tc.wantForcedFull, forced)
			assert.Equal(t, tc.wantExpectation, env.du(t).Annotations[common.AnnotationExpectedBackupType])
		})
	}
}

func TestPushOntoBlockCheckpointForcesFull(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, nil)
	env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil), trackerWithCheckpoint("cp1"))...)
	seedCheckpoint(t, env, uploader.CheckpointFormatBlocks)

	_, err := env.r.handleAccepted(context.Background(), logr.Discard(), du)
	require.NoError(t, err)
	vmbs := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	require.NoError(t, env.client.List(context.Background(), vmbs, client.InNamespace(pullVMNamespace)))
	require.Len(t, vmbs.Items, 1)
	assert.True(t, vmbs.Items[0].Spec.ForceFullBackup, "a VM switched back to push starts a new chain")
}

func pullVMB(conditions ...kubevirtbackupv1alpha1.Condition) *kubevirtbackupv1alpha1.VirtualMachineBackup {
	checkpoint := "cp2"
	apiGroup := "backup.kubevirt.io"
	pvcName := "scratch"
	return &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vmb-du1", Namespace: pullVMNamespace, Labels: map[string]string{common.LabelDataUploadUID: pullDUUID},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source:  corev1.TypedLocalObjectReference{APIGroup: &apiGroup, Kind: "VirtualMachineBackupTracker", Name: "vmbt-vm1"},
			PvcName: &pvcName,
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{CheckpointName: &checkpoint, Conditions: conditions},
	}
}

func exportReady() kubevirtbackupv1alpha1.Condition {
	return kubevirtbackupv1alpha1.Condition{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionTrue, Reason: puller.ExportReadyReason}
}

func TestEvaluatePullBackupStatus(t *testing.T) {
	pullAnnotation := map[string]string{common.AnnotationBackupMode: common.BackupModePull}
	cases := []struct {
		name       string
		conditions []kubevirtbackupv1alpha1.Condition
		wantPhase  velerov2alpha1.DataUploadPhase
	}{
		{name: "export ready moves on", conditions: []kubevirtbackupv1alpha1.Condition{exportReady()},
			wantPhase: velerov2alpha1.DataUploadPhasePrepared},
		{name: "progressing without ExportReady keeps waiting", conditions: []kubevirtbackupv1alpha1.Condition{
			{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionTrue, Reason: "Starting"}},
			wantPhase: velerov2alpha1.DataUploadPhaseAccepted},
		{name: "no conditions keeps waiting", wantPhase: velerov2alpha1.DataUploadPhaseAccepted},
		{name: "a backup that ended by itself is a failure", conditions: []kubevirtbackupv1alpha1.Condition{
			{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue, Reason: "Completed VirtualMachineBackup"},
			{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse}},
			wantPhase: velerov2alpha1.DataUploadPhaseFailed},
		{name: "done false and not progressing is a failure", conditions: []kubevirtbackupv1alpha1.Condition{
			{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse, Reason: "Backup has failed: no space"},
			{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse}},
			wantPhase: velerov2alpha1.DataUploadPhaseFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			du := pullDataUpload(velerov2alpha1.DataUploadPhaseAccepted, pullAnnotation)
			vmb := pullVMB(tc.conditions...)
			env := newPullEnv(t, du, vmb, trackerWithCheckpoint("cp1"))
			_, err := env.r.evaluatePullBackupStatus(context.Background(), logr.Discard(), du, vmb)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPhase, env.du(t).Status.Phase)
		})
	}
}

// includedVolumes is not part of the vendored API types, so the reconciler's
// backup type lookup is given an unstructured view of the backup.
type includedVolumesReader struct {
	client.Reader
	backupType string
}

func (r includedVolumesReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "VirtualMachineBackup" {
		u.SetName(key.Name)
		u.SetNamespace(key.Namespace)
		return unstructured.SetNestedSlice(u.Object, []any{map[string]any{"volumeName": "root", "type": r.backupType}}, "status", "includedVolumes")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestHandlePreparedPullLaunchesTransferPod(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhasePrepared, map[string]string{
		common.AnnotationBackupMode:         common.BackupModePull,
		common.AnnotationExpectedBackupType: uploader.BackupTypeIncremental,
		AnnotationVMBTName:                  "vmbt-vm1",
	})
	token := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: common.PullTokenSecretPrefix + pullDUUID, Namespace: pullVMNamespace},
		Data:       map[string][]byte{common.PullTokenSecretKey: []byte("secret-token")},
	}
	env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil), pullVMB(exportReady()), token)...)
	env.r.APIReader = includedVolumesReader{Reader: env.client, backupType: "Incremental"}
	env.r.PullReaders = 6
	env.r.PullRateLimitMiB = 200
	ctx := context.Background()

	_, err := env.r.handlePrepared(ctx, logr.Discard(), du)
	require.NoError(t, err)
	assert.Equal(t, velerov2alpha1.DataUploadPhaseInProgress, env.du(t).Status.Phase)

	pods := &corev1.PodList{}
	require.NoError(t, env.client.List(ctx, pods, client.InNamespace(pullOADPNS)))
	require.Len(t, pods.Items, 1)
	pod := pods.Items[0]
	container := pod.Spec.Containers[0]
	assert.Equal(t, []string{"/manager", "pull"}, container.Command)
	assert.Equal(t, common.PodTypePull, pod.Labels[common.LabelDatamoverPod])
	env2 := map[string]string{}
	for _, e := range container.Env {
		env2[e.Name] = e.Value
	}
	assert.Equal(t, "cp2", env2[uploader.EnvCheckpointName])
	assert.Equal(t, uploader.BackupTypeIncremental, env2[uploader.EnvBackupType])
	assert.Equal(t, "vmb-du1", env2[uploader.EnvVMBName])
	assert.Equal(t, "vmbt-vm1", env2[uploader.EnvVMBTName])
	assert.Equal(t, "backup1", env2[uploader.EnvVeleroBackupName])
	assert.Equal(t, "6", env2[puller.EnvReaders])
	assert.Equal(t, "200", env2[puller.EnvRateLimitMiB])
	assert.Equal(t, puller.DefaultTokenFile, env2[puller.EnvTokenFile])

	// No PVC is mounted: the data path is the network.
	for _, v := range pod.Spec.Volumes {
		assert.Nil(t, v.PersistentVolumeClaim, "volume %s", v.Name)
	}
	// The token is copied next to the pod and mounted.
	copied := &corev1.Secret{}
	require.NoError(t, env.client.Get(ctx, types.NamespacedName{Name: token.Name, Namespace: pullOADPNS}, copied))
	assert.Equal(t, "secret-token", string(copied.Data[common.PullTokenSecretKey]))
	var mounted bool
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == token.Name {
			mounted = true
		}
	}
	assert.True(t, mounted)

	// No PV was touched.
	pvs := &corev1.PersistentVolumeList{}
	require.NoError(t, env.client.List(ctx, pvs))
	assert.Empty(t, pvs.Items)
}

func TestHandlePreparedPullFailsWithoutBackup(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhasePrepared, map[string]string{
		common.AnnotationBackupMode: common.BackupModePull, AnnotationVMBTName: "vmbt-vm1",
	})
	env := newPullEnv(t, append(pullBaseObjects(), du, pullVM(nil))...)
	_, err := env.r.handlePrepared(context.Background(), logr.Discard(), du)
	require.NoError(t, err)
	assert.Equal(t, velerov2alpha1.DataUploadPhaseFailed, env.du(t).Status.Phase)
}

func TestUpdatePullProgress(t *testing.T) {
	du := pullDataUpload(velerov2alpha1.DataUploadPhaseInProgress, map[string]string{common.AnnotationBackupMode: common.BackupModePull})
	env := newPullEnv(t, du)
	logs := `{"msg":"transfer progress","bytesDone":1048576,"totalBytes":4194304}` + "\n"
	env.r.PodLogCollector = func(context.Context, string, string) (string, error) { return logs, nil }
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: pullOADPNS}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}

	env.r.updatePullProgress(context.Background(), logr.Discard(), du, pod)
	stored := env.du(t)
	assert.Equal(t, int64(1048576), stored.Status.Progress.BytesDone)
	assert.Equal(t, int64(4194304), stored.Status.Progress.TotalBytes)

	// Logs without a progress line leave the last value in place.
	logs = "starting\n"
	env.r.updatePullProgress(context.Background(), logr.Discard(), du, pod)
	assert.Equal(t, int64(1048576), env.du(t).Status.Progress.BytesDone)

	// Push-mode DataUploads are never touched.
	push := pullDataUpload(velerov2alpha1.DataUploadPhaseInProgress, nil)
	logs = `{"msg":"transfer progress","bytesDone":5,"totalBytes":10}`
	env.r.updatePullProgress(context.Background(), logr.Discard(), push, pod)
	assert.Zero(t, push.Status.Progress.TotalBytes)
}

func TestPullCleanupOnEveryTerminalPhase(t *testing.T) {
	for _, phase := range []velerov2alpha1.DataUploadPhase{
		velerov2alpha1.DataUploadPhaseCompleted, velerov2alpha1.DataUploadPhaseFailed, velerov2alpha1.DataUploadPhaseCanceled,
	} {
		t.Run(string(phase), func(t *testing.T) {
			du := pullDataUpload(phase, map[string]string{common.AnnotationBackupMode: common.BackupModePull})
			labels := map[string]string{common.LabelDataUploadUID: pullDUUID}
			secretName := common.PullTokenSecretPrefix + pullDUUID
			env := newPullEnv(t, du, pullVMB(exportReady()),
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: pullVMNamespace, Labels: labels}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: pullOADPNS, Labels: labels}},
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "scratch", Namespace: pullVMNamespace, Labels: labels}},
				// Things that belong to someone else must survive.
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "vm1-root", Namespace: pullVMNamespace}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: pullVMNamespace}},
				trackerWithCheckpoint("cp1"),
			)
			ctx := context.Background()
			req := types.NamespacedName{Name: pullDUName, Namespace: pullOADPNS}

			// First pass deletes the backup and waits for it to be gone
			// before touching the PVC that is attached through it.
			result, err := env.r.Reconcile(ctx, reconcileRequest(req))
			require.NoError(t, err)
			assert.Equal(t, RequeueAfterShort, result.RequeueAfter)
			vmbs := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
			require.NoError(t, env.client.List(ctx, vmbs, client.InNamespace(pullVMNamespace)))
			assert.Empty(t, vmbs.Items)
			pvc := &corev1.PersistentVolumeClaim{}
			require.NoError(t, env.client.Get(ctx, types.NamespacedName{Name: "scratch", Namespace: pullVMNamespace}, pvc),
				"the scratch PVC stays until the backup is confirmed gone")

			result, err = env.r.Reconcile(ctx, reconcileRequest(req))
			require.NoError(t, err)
			assert.Zero(t, result.RequeueAfter)
			assert.Equal(t, "true", env.du(t).Annotations[common.AnnotationPullCleanupDone])
			assert.Equal(t, phase, env.du(t).Status.Phase)

			pvcs := &corev1.PersistentVolumeClaimList{}
			require.NoError(t, env.client.List(ctx, pvcs, client.InNamespace(pullVMNamespace)))
			require.Len(t, pvcs.Items, 1)
			assert.Equal(t, "vm1-root", pvcs.Items[0].Name)
			for _, ns := range []string{pullVMNamespace, pullOADPNS} {
				secrets := &corev1.SecretList{}
				require.NoError(t, env.client.List(ctx, secrets, client.InNamespace(ns)))
				for _, s := range secrets.Items {
					assert.NotEqual(t, secretName, s.Name)
				}
			}
			// The tracker stays: KubeVirt owns its checkpoint.
			vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
			require.NoError(t, env.client.Get(ctx, types.NamespacedName{Name: "vmbt-vm1", Namespace: pullVMNamespace}, vmbt))
		})
	}
}

func TestPullFailedPodAndLostPodFailTheDataUpload(t *testing.T) {
	annotations := map[string]string{common.AnnotationBackupMode: common.BackupModePull}
	t.Run("failed pod", func(t *testing.T) {
		du := pullDataUpload(velerov2alpha1.DataUploadPhaseInProgress, annotations)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: pullOADPNS, Labels: map[string]string{common.LabelDataUploadUID: pullDUUID}},
			Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "the virtual machine is paused during the backup"}},
			}}},
		}
		env := newPullEnv(t, du, pod)
		_, err := env.r.handleInProgress(context.Background(), logr.Discard(), du)
		require.NoError(t, err)
		stored := env.du(t)
		assert.Equal(t, velerov2alpha1.DataUploadPhaseFailed, stored.Status.Phase)
		assert.Contains(t, stored.Status.Message, "paused")
	})
	t.Run("pod killed and gone", func(t *testing.T) {
		du := pullDataUpload(velerov2alpha1.DataUploadPhaseInProgress, annotations)
		env := newPullEnv(t, du)
		_, err := env.r.handleInProgress(context.Background(), logr.Discard(), du)
		require.NoError(t, err)
		assert.Equal(t, velerov2alpha1.DataUploadPhaseFailed, env.du(t).Status.Phase)
	})
}

func TestBuildPullPod(t *testing.T) {
	pod := buildDatamoverPod(&DatamoverPodConfig{
		OperationMode: OperationModePull, Name: "x", Namespace: pullOADPNS, Image: "img",
		CredentialSecretName: "creds", CredentialSecretKey: "cloud", ExportTokenSecretName: "tok",
		SSECSecretName: "ssec", SSECSecretKey: "key", ResourceUID: pullDUUID, ResourceName: pullDUName,
	})
	assert.Equal(t, []string{"/manager", "pull"}, pod.Spec.Containers[0].Command)
	assert.Equal(t, pullDUUID, pod.Labels[common.LabelDataUploadUID])
	names := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		names[v.Name] = true
	}
	assert.True(t, names[exportTokenVolumeName] && names[cloudCredentialsVolumeName] && names["sse-c-key"])
	assert.False(t, names[backupDataVolumeName])
	for _, e := range pod.Spec.Containers[0].Env {
		assert.NotEqual(t, puller.EnvReaders, e.Name, "unset tuning leaves the client default")
	}
}

func reconcileRequest(key types.NamespacedName) ctrl.Request {
	return ctrl.Request{NamespacedName: key}
}
