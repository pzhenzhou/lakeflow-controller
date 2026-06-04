// Package cloud holds the internal-only cloud-provider abstraction used by the
// reconciler/adapter. A Provider describes the DESIRED Kubernetes objects
// (StorageClass, PVC) and provider-specific configuration for a LakeFlow's
// storage spec. Providers are PURE: they never read from or write to the
// cluster. Applying the returned objects is the reconciler's responsibility.
//
// The interface is intentionally not surfaced in any CRD. Today only Aliyun is
// implemented; the abstraction exists so additional providers can be added
// without spreading vendor-specific literals across the adapter.
package cloud

import (
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Provider produces the desired storage manifests for a given LakeFlow storage
// spec. Implementations must be deterministic and side-effect free so their
// output can be golden-tested and applied idempotently by the reconciler.
type Provider interface {
	// Name returns the provider identifier (e.g. "aliyun").
	Name() string

	// DescribeObjectStorageClass returns the desired cluster-scoped StorageClass
	// backing an object-storage spec. credRef points at the user-provided
	// credential Secret consumed by the CSI driver (may be nil).
	DescribeObjectStorageClass(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string) *storagev1.StorageClass

	// DescribeObjectStoragePVC returns the desired namespaced PVC for an
	// object-storage spec. Callers guard on MountSpec before invoking; the
	// returned object assumes a non-nil MountSpec. The PVC name is the explicit
	// mountSpec.pvcName or, when empty, a deterministic shared name derived from
	// the storage identity (which is why credRef is required).
	DescribeObjectStoragePVC(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string) *corev1.PersistentVolumeClaim

	// DescribeBlockStorageClass returns the desired cluster-scoped StorageClass
	// used for executor local-dir / block storage of the given name.
	DescribeBlockStorageClass(storageClassName string) *storagev1.StorageClass

	// DescribeWorkflowStorage returns all storage objects needed by a LakeFlow.
	DescribeWorkflowStorage(lw *v1alpha1.LakeFlow, localDir *BlockStorageRequest) (*WorkflowStorage, error)
}

// WorkflowStorage is the set of desired, provider-described storage objects a
// LakeFlow needs. It is computed purely from the LakeFlow spec.
type WorkflowStorage struct {
	StorageClasses []*storagev1.StorageClass
	PVCs           []*corev1.PersistentVolumeClaim
}

// BlockStorageRequest describes workflow-wide Spark local-dir PVC needs parsed
// from LakeFlow labels by the adapter package.
type BlockStorageRequest struct {
	Enabled   bool
	SizeLimit string
}

type profileProvider struct {
	profile cloudprofile.CloudProfile
}

// NewProvider returns a profile-driven Provider. The profile is copied so
// callers cannot mutate future provider output.
func NewProvider(profile cloudprofile.CloudProfile) Provider {
	return &profileProvider{profile: cloneProfile(profile)}
}

// NewAliyunProvider returns the default Aliyun cloud Provider implementation.
func NewAliyunProvider() Provider {
	profile, _ := cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	return NewProvider(profile)
}

func (p *profileProvider) Name() string { return p.profile.Name }

func (p *profileProvider) DescribeObjectStorageClass(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string) *storagev1.StorageClass {
	if objStorage == nil {
		return nil
	}
	secretName, secretNamespace := ResolveSecretRef(credRef, namespace)
	params := make(map[string]string, len(p.profile.ObjectStorage.StorageClassParameterRules))
	for _, rule := range p.profile.ObjectStorage.StorageClassParameterRules {
		value := p.objectStorageParameterValue(rule, objStorage, secretName, secretNamespace)
		params[rule.Key] = value
	}
	sc := newStorageClass(
		EffectiveObjectStorageStorageClassName(objStorage, credRef, namespace, p.profile),
		p.profile.ObjectStorage.ReclaimPolicy,
		p.profile.ObjectStorage.CSIDriver,
		p.profile.ObjectStorage.VolumeBindingMode,
		params,
	)
	if objStorage.StorageClass == "" {
		sc.Labels = map[string]string{v1alpha1.SharedObjectStoragePVCLabel: "true"}
	}
	return sc
}

func (p *profileProvider) DescribeObjectStoragePVC(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string) *corev1.PersistentVolumeClaim {
	if objStorage == nil || objStorage.MountSpec == nil {
		return nil
	}
	mount := objStorage.MountSpec
	storageQuantity := mount.StorageSize
	volumeMode := corev1.PersistentVolumeFilesystem
	storageClass := EffectiveObjectStorageStorageClassName(objStorage, credRef, namespace, p.profile)
	var labels map[string]string
	if mount.PVCName == "" {
		labels = map[string]string{v1alpha1.SharedObjectStoragePVCLabel: "true"}
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EffectiveObjectStoragePVCName(objStorage, credRef, namespace, p.profile),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
			VolumeMode: &volumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageQuantity,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceStorage: storageQuantity,
				},
			},
			StorageClassName: &storageClass,
		},
	}
}

func (p *profileProvider) DescribeBlockStorageClass(storageClassName string) *storagev1.StorageClass {
	return newStorageClass(
		effectiveStorageClass(storageClassName, p.profile.BlockStorage.DefaultStorageClass),
		p.profile.BlockStorage.ReclaimPolicy,
		p.profile.BlockStorage.CSIDriver,
		p.profile.BlockStorage.VolumeBindingMode,
		cloneStringMap(p.profile.BlockStorage.Params),
	)
}

func (p *profileProvider) DescribeWorkflowStorage(lw *v1alpha1.LakeFlow, localDir *BlockStorageRequest) (*WorkflowStorage, error) {
	storage := &WorkflowStorage{}
	if lw == nil {
		return storage, nil
	}

	scSeen := make(map[string]bool)
	pvcSeen := make(map[string]bool)

	addSC := func(sc *storagev1.StorageClass) {
		if sc == nil || sc.Name == "" || scSeen[sc.Name] {
			return
		}
		scSeen[sc.Name] = true
		storage.StorageClasses = append(storage.StorageClasses, sc)
	}
	addPVC := func(pvc *corev1.PersistentVolumeClaim) {
		if pvc == nil || pvc.Name == "" || pvcSeen[pvc.Name] {
			return
		}
		pvcSeen[pvc.Name] = true
		storage.PVCs = append(storage.PVCs, pvc)
	}
	addObjectStorage := func(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference) {
		if objStorage == nil {
			return
		}
		mount := objStorage.MountSpec
		// A StorageClass is needed whenever a PVC will be mounted (the PVC binds
		// to it) or when the user/profile pins an explicit/default class. For a
		// mounted bucket with no explicit class, the operator provisions a
		// content-addressed StorageClass so the derived PVC has a class encoding
		// its bucket. uri-mode (no mount) with no explicit class creates nothing,
		// matching prior behavior.
		explicitOrDefaultSC := objStorage.StorageClass != "" || p.profile.ObjectStorage.DefaultStorageClass != ""
		if mount != nil || explicitOrDefaultSC {
			addSC(p.DescribeObjectStorageClass(objStorage, credRef, lw.Namespace))
		}
		if mount == nil {
			return
		}
		// Derived (empty pvcName) PVCs are operator-managed and always created;
		// AutoCreatePVC only gates explicitly named PVCs (which may pre-exist).
		derived := mount.PVCName == ""
		if derived || mount.AutoCreatePVC {
			addPVC(p.DescribeObjectStoragePVC(objStorage, credRef, lw.Namespace))
		}
	}

	for i := range lw.Spec.Tasks {
		task := &lw.Spec.Tasks[i]
		if spark := task.TaskSpec.SparkExecutorSpec; spark != nil {
			addObjectStorage(spark.ObjectStorage, spark.CredentialsRef)
			if localDir != nil && localDir.Enabled {
				addSC(p.DescribeBlockStorageClass(p.profile.BlockStorage.DefaultStorageClass))
			}
		}
		if cmd := task.TaskSpec.CommandExecutorSpec; cmd != nil {
			addObjectStorage(cmd.ObjectStorage, cmd.CredentialsRef)
		}
	}

	return storage, nil
}

func (p *profileProvider) objectStorageParameterValue(rule cloudprofile.StorageClassParameterRule, objStorage *v1alpha1.ObjectStorage, secretName, secretNamespace string) string {
	switch rule.Source {
	case cloudprofile.ParamSourceLiteral:
		return rule.Literal
	case cloudprofile.ParamSourceBucket:
		return objStorage.Bucket
	case cloudprofile.ParamSourcePath:
		return objStorage.Path
	case cloudprofile.ParamSourceEndpoint:
		return effectiveStorageClass(objStorage.Endpoint, p.profile.ObjectStorage.DefaultEndpoint)
	case cloudprofile.ParamSourceRegion:
		return effectiveStorageClass(objStorage.Region, p.profile.ObjectStorage.DefaultRegion)
	case cloudprofile.ParamSourceSecretName:
		return secretName
	case cloudprofile.ParamSourceSecretNamespace:
		return secretNamespace
	default:
		return ""
	}
}

// ResolveSecretRef returns the secret name and namespace for a credential
// reference, defaulting the namespace to defaultNamespace when not set.
func ResolveSecretRef(credRef *corev1.SecretReference, defaultNamespace string) (name, namespace string) {
	namespace = defaultNamespace
	if credRef == nil {
		return "", namespace
	}
	if credRef.Namespace != "" {
		namespace = credRef.Namespace
	}
	return credRef.Name, namespace
}

// newStorageClass builds a StorageClass with the conventions shared by all
// provider StorageClasses: WaitForFirstConsumer binding and volume expansion
// enabled. reclaimPolicy is optional (empty string leaves it unset).
func newStorageClass(name, reclaimPolicy, provisioner string, bindingMode storagev1.VolumeBindingMode, params map[string]string) *storagev1.StorageClass {
	if bindingMode == "" {
		bindingMode = storagev1.VolumeBindingWaitForFirstConsumer
	}
	allowExpansion := true
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner:          provisioner,
		Parameters:           cloneStringMap(params),
		VolumeBindingMode:    &bindingMode,
		AllowVolumeExpansion: &allowExpansion,
	}
	if reclaimPolicy != "" {
		policy := corev1.PersistentVolumeReclaimPolicy(reclaimPolicy)
		sc.ReclaimPolicy = &policy
	}
	return sc
}

func effectiveStorageClass(value, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}

func cloneProfile(profile cloudprofile.CloudProfile) cloudprofile.CloudProfile {
	profile.ObjectStorage.ExtraClasspath = cloneStringSlice(profile.ObjectStorage.ExtraClasspath)
	profile.ObjectStorage.StorageClassParameterRules = cloneParameterRules(profile.ObjectStorage.StorageClassParameterRules)
	profile.BlockStorage.Params = cloneStringMap(profile.BlockStorage.Params)
	return profile
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneParameterRules(src []cloudprofile.StorageClassParameterRule) []cloudprofile.StorageClassParameterRule {
	if src == nil {
		return nil
	}
	dst := make([]cloudprofile.StorageClassParameterRule, len(src))
	copy(dst, src)
	return dst
}
