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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/vmware-tanzu/velero/pkg/apis/velero/shared"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/puller"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// Pull mode in short: instead of letting KubeVirt write a backup image into
// a PVC, the controller opens the backup for export (VirtualMachineBackup
// with mode Pull), waits until the export endpoints are ready and runs a pod
// that streams the disk to the object store. The PVC that still exists only
// holds KubeVirt's copy-before-write scratch file.
//
// Ending a pull backup makes KubeVirt advance the tracker's checkpoint no
// matter whether the data was stored. The transfer pod therefore writes the
// checkpoint index entry before it deletes the VirtualMachineBackup, and
// every other way out (failure, timeout, cancel, a lost pod) deletes the
// backup without an index entry. The next backup then finds tracker and
// index in disagreement and is forced to be a full one.

const (
	// pullScratchOverheadPercent is added to the disk capacity when sizing
	// the scratch PVC. The scratch file receives the old content of every
	// block the guest overwrites for the first time while the backup is
	// open, so it can grow to the size of the disk; if it fills up, the VM
	// is paused. The margin covers filesystem and image metadata.
	pullScratchOverheadPercent = 10

	// DefaultPullBackupTTL is the default lifetime of a pull-mode backup.
	DefaultPullBackupTTL = 8 * time.Hour

	pullModeValue = "Pull"
	trueValue     = "true"
)

// isPullMode reports whether the controller chose pull mode for du.
func isPullMode(du *velerov2alpha1.DataUpload) bool {
	return du.Annotations[common.AnnotationBackupMode] == common.BackupModePull
}

// checkpointFormatFor returns the storage format du's checkpoint is written in.
func checkpointFormatFor(du *velerov2alpha1.DataUpload) string {
	if isPullMode(du) {
		return uploader.CheckpointFormatBlocks
	}
	return uploader.CheckpointFormatQcow2
}

func validBackupMode(mode string) bool {
	return mode == common.BackupModePush || mode == common.BackupModePull
}

func (r *KubeVirtDataUploadReconciler) pullBackupTTL() time.Duration {
	if r.PullBackupTTL > 0 {
		return r.PullBackupTTL
	}
	return DefaultPullBackupTTL
}

// ensureBackupMode decides once per DataUpload whether it is transferred in
// push or pull mode and records the decision as an annotation, so later
// reconciles and a changed flag or VM annotation cannot switch modes midway.
// Precedence: VM annotation, then the controller's --backup-mode, then push.
func (r *KubeVirtDataUploadReconciler) ensureBackupMode(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) error {
	if validBackupMode(du.Annotations[common.AnnotationBackupMode]) {
		return nil
	}
	mode := common.BackupModePush
	if validBackupMode(r.BackupMode) {
		mode = r.BackupMode
	}
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err != nil {
		// A VM that is gone cannot override anything and fails the backup
		// later on its own. Any other error is retried: guessing the mode
		// could stage a full backup that was meant to be streamed.
		if !errors.IsNotFound(err) && !runtime.IsNotRegisteredError(err) {
			return fmt.Errorf("failed to get VirtualMachine to determine the backup mode: %w", err)
		}
	} else if override, ok := vm.Annotations[common.AnnotationBackupMode]; ok {
		if validBackupMode(override) {
			mode = override
		} else {
			logger.Info("Ignoring invalid backup-mode annotation on VirtualMachine", "value", override)
		}
	}
	original := du.DeepCopy()
	if du.Annotations == nil {
		du.Annotations = make(map[string]string)
	}
	du.Annotations[common.AnnotationBackupMode] = mode
	if err := r.Patch(ctx, du, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("failed to record the backup mode on the DataUpload: %w", err)
	}
	logger.Info("Backup mode selected", "mode", mode)
	return nil
}

// pullTokenSecretName is the name of the export token Secret of a DataUpload,
// used both in the VM's namespace and in the datamover pod's namespace.
func pullTokenSecretName(du *velerov2alpha1.DataUpload) string {
	return common.PullTokenSecretPrefix + string(du.UID)
}

func (r *KubeVirtDataUploadReconciler) uncachedReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// ensurePullTokenSecret creates the Secret with the export token in the VM's
// namespace. KubeVirt hands the backup data to whoever presents this token.
func (r *KubeVirtDataUploadReconciler) ensurePullTokenSecret(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (string, error) {
	name := pullTokenSecretName(du)
	existing := &corev1.Secret{}
	err := r.uncachedReader().Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, existing)
	if err == nil {
		return name, nil
	}
	if !errors.IsNotFound(err) {
		return "", fmt.Errorf("failed to check for the export token secret: %w", err)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("failed to generate an export token: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      map[string]string{common.LabelDataUploadUID: string(du.UID)},
			Annotations: map[string]string{common.AnnotationDataUploadName: du.Name},
		},
		Data: map[string][]byte{common.PullTokenSecretKey: []byte(hex.EncodeToString(raw))},
	}
	if err := r.Create(ctx, secret); err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create the export token secret: %w", err)
	}
	logger.Info("Created export token secret", "secret", name, "namespace", namespace)
	return name, nil
}

// copyPullTokenSecret makes the export token available in the datamover
// pod's namespace, where the pod mounts it.
func (r *KubeVirtDataUploadReconciler) copyPullTokenSecret(ctx context.Context, du *velerov2alpha1.DataUpload, vmNamespace, podNamespace string) (string, error) {
	name := pullTokenSecretName(du)
	if vmNamespace == podNamespace {
		return name, nil
	}
	source := &corev1.Secret{}
	if err := r.uncachedReader().Get(ctx, types.NamespacedName{Name: name, Namespace: vmNamespace}, source); err != nil {
		return "", fmt.Errorf("failed to read the export token secret: %w", err)
	}
	target := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   podNamespace,
			Labels:      map[string]string{common.LabelDataUploadUID: string(du.UID)},
			Annotations: map[string]string{common.AnnotationDataUploadName: du.Name},
		},
		Data: map[string][]byte{common.PullTokenSecretKey: source.Data[common.PullTokenSecretKey]},
	}
	if du.Namespace == podNamespace {
		if err := controllerutil.SetOwnerReference(du, target, r.Scheme); err != nil {
			return "", fmt.Errorf("failed to set owner reference on the export token secret: %w", err)
		}
	}
	if err := r.Create(ctx, target); err != nil && !errors.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to copy the export token secret: %w", err)
	}
	return name, nil
}

// createPullVMBackup creates vmb as a pull-mode backup. The pull fields
// (mode, tokenSecretRef, ttlDuration) are newer than the vendored API types,
// so the object is sent as unstructured. On success vmb carries the name
// and metadata the API server assigned.
func (r *KubeVirtDataUploadReconciler) createPullVMBackup(ctx context.Context, vmb *kubevirtbackupv1alpha1.VirtualMachineBackup, tokenSecret string) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vmb)
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{Object: content}
	obj.SetGroupVersionKind(kubevirtbackupv1alpha1.SchemeGroupVersion.WithKind("VirtualMachineBackup"))
	for field, value := range map[string]string{
		"mode":           pullModeValue,
		"tokenSecretRef": tokenSecret,
		"ttlDuration":    metav1.Duration{Duration: r.pullBackupTTL()}.Duration.String(),
	} {
		if err := unstructured.SetNestedField(obj.Object, value, "spec", field); err != nil {
			return err
		}
	}
	unstructured.RemoveNestedField(obj.Object, "status")
	if err := r.Create(ctx, obj); err != nil {
		return err
	}
	vmb.ObjectMeta.Name = obj.GetName()
	vmb.ObjectMeta.UID = obj.GetUID()
	vmb.ObjectMeta.ResourceVersion = obj.GetResourceVersion()
	return nil
}

// evaluatePullBackupStatus decides whether a pull-mode backup is ready to be
// read. A pull backup never reports Done while it is open: readiness is
// Progressing=True with reason ExportReady. Done=True before the transfer
// has run means the backup ended on its own (for example its TTL expired).
func (r *KubeVirtDataUploadReconciler) evaluatePullBackupStatus(
	ctx context.Context, logger logr.Logger,
	du *velerov2alpha1.DataUpload, vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
) (ctrl.Result, error) {
	for i := range vmb.Status.Conditions {
		cond := &vmb.Status.Conditions[i]
		switch cond.Type {
		case kubevirtbackupv1alpha1.ConditionProgressing:
			if cond.Status == corev1.ConditionTrue && cond.Reason == puller.ExportReadyReason {
				backupType, typeErr := r.resolveVMBBackupType(ctx, vmb)
				if typeErr != nil {
					backupType = "unknown"
				}
				logger.Info("VirtualMachineBackup export is ready", "vmb", vmb.Name, "type", backupType,
					"checkpoint", vmb.Status.CheckpointName)
				if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhasePrepared,
					fmt.Sprintf("VMBackup export ready (type=%s)", backupType)); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
			}
		case kubevirtbackupv1alpha1.ConditionDone, conditionComplete:
			if cond.Status == corev1.ConditionTrue {
				detail := vmBackupConditionDetail(cond)
				logger.Error(nil, "Pull-mode VirtualMachineBackup ended before the transfer started", "vmb", vmb.Name, "detail", detail)
				if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("VMBackup ended before the transfer started: %s", detail)); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
		}
	}
	// Not ready yet: the shared evaluation recognizes failed and stuck backups.
	return r.evaluateVMBackupStatus(ctx, logger, du, vmb)
}

// handlePreparedPull launches the transfer pod of a pull-mode backup. Unlike
// push mode there is no volume to hand over: the pod reads the export
// endpoint over the network.
func (r *KubeVirtDataUploadReconciler) handlePreparedPull(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	fail := func(message string) (ctrl.Result, error) {
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, message); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	vmRef, err := common.GetVMReference(du)
	if err != nil {
		return fail(fmt.Sprintf("Missing VM reference: %v", err))
	}
	podNamespace := r.getPodNamespace(du)

	pod, err := r.findPodForDataUpload(ctx, du, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing datamover pod: %w", err)
	}
	if pod != nil {
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod running"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	if gated, result, err := r.checkConcurrentDataMoverLimit(ctx, logger, du); err != nil {
		return ctrl.Result{}, err
	} else if gated {
		return result, nil
	}

	bsl, err := r.getBackupStorageLocationForDU(ctx, du)
	if err != nil {
		if isTransientBSLLookupError(err, du.Spec.BackupStorageLocation) {
			return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
		}
		return fail(fmt.Sprintf("Failed to get BSL: %v", err))
	}

	vmb, err := r.findVMBForDataUpload(ctx, du, vmRef.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachineBackup: %w", err)
	}
	if vmb == nil {
		return fail("VirtualMachineBackup not found")
	}

	backupType, err := r.resolveVMBBackupType(ctx, vmb)
	if err != nil {
		logger.Error(err, "Cannot determine backup type; requeuing")
		return ctrl.Result{}, err
	}
	expectedBackupType := du.Annotations[common.AnnotationExpectedBackupType]
	if expectedBackupType != "" && !strings.EqualFold(expectedBackupType, backupType) {
		logger.Info("Backup type mismatch detected: VM may have lost its libvirt checkpoint",
			"expected", expectedBackupType, "actual", backupType)
	}
	if vmb.Status == nil || vmb.Status.CheckpointName == nil || *vmb.Status.CheckpointName == "" {
		return fail(fmt.Sprintf("VirtualMachineBackup %s reports no checkpoint name", vmb.Name))
	}
	vmbtName := du.Annotations[AnnotationVMBTName]
	if vmbtName == "" {
		return fail(fmt.Sprintf("VMBT name annotation %s not found on DataUpload", AnnotationVMBTName))
	}

	podConfig, err := r.buildDatamoverPodConfig(du, bsl, vmb, vmRef, backupType, expectedBackupType, *vmb.Status.CheckpointName, vmbtName)
	if err != nil {
		return fail(fmt.Sprintf("Failed to build pod config: %v", err))
	}
	tokenSecret, err := r.copyPullTokenSecret(ctx, du, vmRef.Namespace, podNamespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	podConfig.OperationMode = OperationModePull
	podConfig.Namespace = podNamespace
	podConfig.ExportTokenSecretName = tokenSecret
	podConfig.PullReaders = r.PullReaders
	podConfig.PullRateLimitMiB = r.PullRateLimitMiB
	podConfig.CompressFullBackups = false

	podToCreate := buildDatamoverPod(podConfig)
	podToCreate.GenerateName = safeGenerateNamePrefix(fmt.Sprintf("%s%s-", common.DatamoverPodNamePrefix, du.Name), 63)
	podToCreate.Name = ""
	if err := controllerutil.SetOwnerReference(du, podToCreate, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set owner reference on pod: %w", err)
	}
	if err := r.Create(ctx, podToCreate); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create datamover pod: %w", err)
	}
	logger.Info("Created pull datamover pod", "generateName", podToCreate.GenerateName, "namespace", podNamespace,
		"backupType", backupType, "checkpoint", *vmb.Status.CheckpointName)

	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod launched"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// updatePullProgress copies the transfer pod's latest progress line into
// DataUpload.status.progress. Best effort: a failure only delays the update.
func (r *KubeVirtDataUploadReconciler) updatePullProgress(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, pod *corev1.Pod) {
	if !isPullMode(du) || r.PodLogCollector == nil || pod.Status.Phase == corev1.PodPending {
		return
	}
	logs, err := r.PodLogCollector(ctx, pod.Name, pod.Namespace)
	if err != nil {
		logger.V(1).Info("Could not read datamover pod log for progress", "pod", pod.Name, "reason", err.Error())
		return
	}
	done, total, ok := puller.ParseProgress(logs)
	if !ok || (du.Status.Progress.BytesDone == done && du.Status.Progress.TotalBytes == total) {
		return
	}
	original := du.DeepCopy()
	du.Status.Progress = shared.DataMoveOperationProgress{TotalBytes: total, BytesDone: done}
	if err := r.Patch(ctx, du, client.MergeFrom(original)); err != nil {
		du.Status.Progress = original.Status.Progress
		logger.V(1).Info("Could not update DataUpload progress", "reason", err.Error())
	}
}

// finishPullCleanup removes what a pull-mode backup leaves behind once its
// DataUpload is in a terminal phase: the VirtualMachineBackup (which ends
// the backup and lets a paused VM resume), the export token Secrets and the
// scratch PVC. It runs for every terminal phase, so it covers success,
// failure, timeout and cancel alike, and it is retried until done.
func (r *KubeVirtDataUploadReconciler) finishPullCleanup(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	if !isPullMode(du) || du.Annotations[common.AnnotationPullCleanupDone] == trueValue {
		return ctrl.Result{}, nil
	}
	if vmRef, err := common.GetVMReference(du); err == nil {
		if done, err := r.cleanupPullResources(ctx, logger, du, vmRef.Namespace); err != nil {
			return ctrl.Result{}, err
		} else if !done {
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}
	}
	original := du.DeepCopy()
	du.Annotations[common.AnnotationPullCleanupDone] = trueValue
	if err := r.Patch(ctx, du, client.MergeFrom(original)); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to mark pull cleanup as done: %w", err)
	}
	logger.Info("Pull-mode backup resources cleaned up", "phase", du.Status.Phase)
	return ctrl.Result{}, nil
}

// cleanupPullResources deletes the VirtualMachineBackup, the token Secrets
// and the scratch PVC of du. The backup goes first and the PVC only once the
// backup is gone, because the PVC is attached to the VM for as long as the
// backup exists. Returns false while the backup is still being removed.
func (r *KubeVirtDataUploadReconciler) cleanupPullResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmNamespace string) (bool, error) {
	reader := r.uncachedReader()
	selector := client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}

	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := reader.List(ctx, vmbList, client.InNamespace(vmNamespace), selector); err != nil {
		return false, fmt.Errorf("failed to list VirtualMachineBackups for cleanup: %w", err)
	}
	for i := range vmbList.Items {
		vmb := &vmbList.Items[i]
		if vmb.DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, vmb); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("failed to delete VirtualMachineBackup %s: %w", vmb.Name, err)
		}
		logger.Info("Deleted VirtualMachineBackup", "vmb", vmb.Name, "namespace", vmNamespace)
	}
	if len(vmbList.Items) > 0 {
		return false, nil
	}

	for _, namespace := range []string{vmNamespace, r.getPodNamespace(du)} {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pullTokenSecretName(du), Namespace: namespace}}
		if err := r.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("failed to delete export token secret in %s: %w", namespace, err)
		}
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcList, client.InNamespace(vmNamespace), selector); err != nil {
		return false, fmt.Errorf("failed to list scratch PVCs for cleanup: %w", err)
	}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("failed to delete scratch PVC %s: %w", pvc.Name, err)
		}
		logger.Info("Deleted scratch PVC", "pvc", pvc.Name, "namespace", vmNamespace)
	}
	return true, nil
}
