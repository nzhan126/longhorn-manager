package backup

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupValidator{ds: ds}
}

func (b *backupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Backup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (b *backupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backup, ok := newObj.(*longhorn.Backup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Backup", newObj), "")
	}

	if !util.ValidateName(backup.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", backup.Name), "")
	}

	if backup.Spec.BackupMode != longhorn.BackupModeFull &&
		backup.Spec.BackupMode != longhorn.BackupModeIncremental {
		return werror.NewInvalidError(fmt.Sprintf("BackupMode %v is not a valid option", backup.Spec.BackupMode), "")
	}

	// Check if backup target exists and is available
	backupTargetName := backup.Labels[types.LonghornLabelBackupTarget]

	backupTarget, err := b.ds.GetBackupTarget(backupTargetName)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to get backup target %s: %v", backupTargetName, err), "")
	}

	if !backupTarget.Status.Available {
		return werror.NewInvalidError(fmt.Sprintf("backup target %s is not available", backupTargetName), "")
	}

	//check if label volume name matches snapshot volume name
	volumeName := backup.Labels[types.LonghornLabelBackupVolume]
	snapshotName := backup.Spec.SnapshotName
	snapshot, _ := b.ds.GetSnapshot(backup.Spec.SnapshotName)
	volume, _ := b.ds.GetVolume(snapshot.Spec.Volume)
	snapshotVolumeName := volume.Name

	if volumeName != snapshotVolumeName {
		return werror.NewInvalidError(fmt.Sprintf("snapshot %s and volume %s does not match", snapshotName, volumeName), "")
	}

	//check if volume backup target matches labelbackup target
	volumeBackupTargetName := volume.Spec.BackupTargetName
	if volumeBackupTargetName != backupTargetName {
		return werror.NewInvalidError(fmt.Sprintf("volume backup target %s and label backup target %s does not match", volumeBackupTargetName, backupTargetName), "")
	}

	return nil
}
