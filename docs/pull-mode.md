# Pull backup mode

By default the controller uses KubeVirt's push mode: QEMU writes the backup
image into a temporary PVC and a datamover pod uploads it afterwards. A full
backup therefore writes the whole disk to the cluster's storage once before
anything reaches the object store.

Pull mode avoids that. The controller opens the backup for export, and a
transfer pod reads the disk over the network and streams it to the object
store. No backup data is staged on a volume.

```text
QEMU (dirty bitmap, point-in-time view of the disk)
  -> KubeVirt export endpoint (HTTPS, one per disk)
    -> transfer pod: parallel ranged reads
         -> skip all-zero blocks -> zstd per 4 MiB block
         -> multipart upload + block index with checksums
```

## Enabling it

Pull mode is opt-in.

| Scope | Setting |
|---|---|
| All VMs | controller flag `--backup-mode=pull` (default `push`) |
| One VM | annotation `kubevirt-datamover.io/backup-mode: pull` (or `push`) on the VirtualMachine; it wins over the flag |

Related controller flags:

| Flag | Default | Meaning |
|---|---|---|
| `--pull-readers` | 4 | parallel ranged reads per transfer |
| `--pull-read-rate-limit-mib` | 0 (unlimited) | read rate limit in MiB/s |
| `--pull-backup-ttl` | 8h | lifetime of the VirtualMachineBackup; KubeVirt ends the backup when it expires |

The controller's ClusterRole needs `create` and `delete` on Secrets for the
export token.

## What happens during a backup

1. The controller records the chosen mode on the DataUpload.
2. It creates an export token Secret and a **filesystem** PVC in the VM's
   namespace. The PVC does not receive backup data. It holds KubeVirt's
   copy-before-write scratch file: the old content of every block the guest
   overwrites for the first time while the backup is open. That file grows
   one to one with the guest's writes and can reach the size of the disk, so
   the PVC is requested at disk capacity plus 10 %. **If it fills up, KubeVirt
   pauses the VM** until the backup ends.
3. It creates the VirtualMachineBackup with `mode: Pull`, the token and a TTL
   and waits for the `Progressing` condition with reason `ExportReady`.
4. It starts the transfer pod (`/manager pull`) in the Velero namespace. The
   pod mounts no PVC, so no PV is rebound.
5. The pod reads the block map, plans 4 MiB blocks (full backup: every block;
   incremental: blocks that overlap a dirty extent), reads them with parallel
   ranged requests, skips all-zero blocks, compresses each block and streams
   the result into one multipart upload per disk.
6. Only when every block is stored does the pod write the block index, then
   the checkpoint's entry in the per-VM index, then the backup manifests, and
   last it deletes the VirtualMachineBackup.
7. The controller removes the token Secrets and the scratch PVC.

Progress is logged by the pod and copied to `DataUpload.status.progress`.

## The checkpoint rule

Ending a pull-mode backup makes KubeVirt advance the tracker's checkpoint,
whether or not the data was stored: deleting the VirtualMachineBackup, its
TTL expiring and a node drain all count as a successful end.

The per-VM index in the object store is therefore the only record of what was
really stored, and it is written before the backup is ended. Every other way
out (a failed or killed transfer pod, a timeout, a cancel, an expired TTL)
ends the backup without an index entry. The next backup finds the tracker and
the index in disagreement and is forced to be a full backup.

A VM that reports `Paused` while its backup is open ends the transfer at
once: the pod deletes the VirtualMachineBackup, which lets the VM resume, and
fails. The next backup is a full one.

The transfer pod also watches the VirtualMachineBackup itself. If it stops
exporting before everything is read (its TTL expired, or it was deleted),
the transfer fails at once and says so, instead of running into network
errors. Choose `--pull-backup-ttl` longer than the longest full backup.

A live migration requested while a backup is open is not rejected: KubeVirt
keeps it pending until the backup has ended and then runs it.

## Storage format

Per disk and checkpoint:

```text
checkpoints/<ns>/<vm>/<checkpoint>/<volume>.blocks      data object
checkpoints/<ns>/<vm>/<checkpoint>/<volume>.blocks.idx  block index
```

The data object is a concatenation of independent zstd frames, one per stored
block. The block index is zstd-compressed JSON with the disk size, the block
size, the checkpoint, its parent and one entry per block: block number,
offset and length of its frame, SHA-256 of the uncompressed block, or a zero
flag for an all-zero block (which has no frame).

The checkpoint's entry in the per-VM `index.json` carries `"format": "blocks"`
and lists both objects in `files`, so the existing delete action removes them
under the usual reference counting: a checkpoint is deleted when no backup
references it any more, and an incremental keeps its ancestors referenced.

A checkpoint chain never mixes formats. Switching a VM between push and pull
forces one full backup; existing qcow2 chains stay restorable.

## Restore

For a chain of block-format checkpoints the download pod merges the block
indexes from the newest full backup to the wanted checkpoint and fetches each
block once, from the newest checkpoint that has it, with ranged reads. Each
block is checked against its checksum and written to its place in the
target; zero blocks are not written. Nothing is staged on the scratch volume
and no chain is replayed.

## Verify

`/manager verify` re-reads the data objects of a VM's block-format
checkpoints and checks every block against its checksum. It takes the same
`KUBEVIRT_DM_BSL_*` and `KUBEVIRT_DM_CREDENTIALS_FILE` settings as the other
datamover pods plus:

| Variable | Meaning |
|---|---|
| `KUBEVIRT_DM_VM_NAMESPACE`, `KUBEVIRT_DM_VM_NAME` | the VM |
| `KUBEVIRT_DM_VERIFY_CHECKPOINT` | one checkpoint ID; unset verifies all block-format checkpoints |
| `KUBEVIRT_DM_VERIFY_WORKERS` | parallel ranged reads (default 4) |

It exits non-zero on the first block that is missing, short or fails its
checksum. Run it as a Job with the BSL credentials Secret (and the SSE-C key,
if used) mounted like in a datamover pod.

## Limits

- One backup per VM at a time (a KubeVirt rule).
- A node drain ends an open backup; the next backup is then a full one.
- Ranged reads use the S3 implementation; other providers fall back to
  reading an object from its start, which is slow for restores.
- Stream uploads buffer up to four parts in memory; the part size grows with
  the disk (16 MiB up to about 100 GiB, then disk size / 8000).
