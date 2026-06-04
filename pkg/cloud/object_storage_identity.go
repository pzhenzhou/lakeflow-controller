package cloud

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	corev1 "k8s.io/api/core/v1"
)

const (
	// derivedPVCNamePrefix and derivedStorageClassNamePrefix are the fixed
	// prefixes for operator-derived, content-addressed OSS resource names.
	derivedPVCNamePrefix          = "lakeflow-oss-"
	derivedStorageClassNamePrefix = "lakeflow-oss-sc-"

	// identityHashLen is the number of hex chars of the sha256 identity digest
	// kept in derived names. 16 hex chars = 64 bits, making accidental
	// collisions (which would mount the wrong bucket) negligible while staying
	// well within Kubernetes name length limits.
	identityHashLen = 16
)

// ObjectStorageIdentity captures the full, effective identity of an object-storage
// mount. Two ObjectStorage specs that resolve to the same identity describe the
// same physical bucket location + access path and can therefore safely share a
// single StorageClass and PVC. The identity is what derived (content-addressed)
// names are computed from, so it must include every field that the provider
// bakes into the StorageClass parameters (bucket/path/endpoint/region/secret),
// plus the requested size, so that differing requests do not silently collapse.
type ObjectStorageIdentity struct {
	Provider        string
	StorageClass    string // explicit objectStorage.storageClass, "" when derived
	Bucket          string
	Path            string
	Endpoint        string
	Region          string
	StorageSize     string
	SecretName      string
	SecretNamespace string
}

// NewObjectStorageIdentity resolves an ObjectStorage spec into its effective
// identity using the cloud profile defaults (endpoint/region) and the credential
// reference (secret name/namespace). It is pure and deterministic.
func NewObjectStorageIdentity(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string, profile cloudprofile.CloudProfile) ObjectStorageIdentity {
	if objStorage == nil {
		return ObjectStorageIdentity{Provider: profile.Name}
	}
	secretName, secretNamespace := ResolveSecretRef(credRef, namespace)
	var storageSize string
	if objStorage.MountSpec != nil {
		storageSize = objStorage.MountSpec.StorageSize.String()
	}
	return ObjectStorageIdentity{
		Provider:        profile.Name,
		StorageClass:    objStorage.StorageClass,
		Bucket:          objStorage.Bucket,
		Path:            objStorage.Path,
		Endpoint:        effectiveStorageClass(objStorage.Endpoint, profile.ObjectStorage.DefaultEndpoint),
		Region:          effectiveStorageClass(objStorage.Region, profile.ObjectStorage.DefaultRegion),
		StorageSize:     storageSize,
		SecretName:      secretName,
		SecretNamespace: secretNamespace,
	}
}

// hash returns the stable identityHashLen-char hex digest of the identity.
func (id ObjectStorageIdentity) hash() string {
	parts := []string{
		id.Provider,
		id.StorageClass,
		id.Bucket,
		id.Path,
		id.Endpoint,
		id.Region,
		id.StorageSize,
		id.SecretName,
		id.SecretNamespace,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:identityHashLen]
}

// EffectiveObjectStoragePVCName returns the PVC name a Spark/command task should
// reference and the reconciler should provision for the given object storage:
// the explicit mountSpec.pvcName when set, otherwise a deterministic
// content-addressed name derived from the storage identity. This is the single
// source of truth shared by the provider (which creates the PVC) and the
// adapter (which mounts it), guaranteeing both agree on the name.
func EffectiveObjectStoragePVCName(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string, profile cloudprofile.CloudProfile) string {
	if objStorage == nil {
		return ""
	}
	if objStorage.MountSpec != nil && objStorage.MountSpec.PVCName != "" {
		return objStorage.MountSpec.PVCName
	}
	return derivedPVCNamePrefix + NewObjectStorageIdentity(objStorage, credRef, namespace, profile).hash()
}

// EffectiveObjectStorageStorageClassName returns the StorageClass name backing
// the object storage: the explicit objectStorage.storageClass when set,
// otherwise a deterministic content-addressed name derived from the same
// storage identity. Keeping this 1:1 with the derived PVC name guarantees a
// derived PVC binds to the StorageClass that actually encodes its bucket.
func EffectiveObjectStorageStorageClassName(objStorage *v1alpha1.ObjectStorage, credRef *corev1.SecretReference, namespace string, profile cloudprofile.CloudProfile) string {
	if objStorage == nil {
		return ""
	}
	if objStorage.StorageClass != "" {
		return objStorage.StorageClass
	}
	return derivedStorageClassNamePrefix + NewObjectStorageIdentity(objStorage, credRef, namespace, profile).hash()
}
