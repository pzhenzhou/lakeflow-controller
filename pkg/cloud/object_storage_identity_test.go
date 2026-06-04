package cloud

import (
	"strings"
	"testing"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func mustProfile(t *testing.T, name string) cloudprofile.CloudProfile {
	t.Helper()
	profile, err := cloudprofile.Builtin(name)
	require.NoError(t, err)
	return profile
}

func mountObjectStorage(bucket, path, storageClass, pvcName, size string) *v1alpha1.ObjectStorage {
	mount := &v1alpha1.ObjectStorageMount{PVCName: pvcName}
	if size != "" {
		mount.StorageSize = resource.MustParse(size)
	}
	return &v1alpha1.ObjectStorage{
		Bucket:       bucket,
		Path:         path,
		StorageClass: storageClass,
		MountSpec:    mount,
	}
}

func TestEffectiveObjectStoragePVCNameExplicitPassthrough(t *testing.T) {
	profile := mustProfile(t, cloudprofile.ProviderAliyun)
	obj := mountObjectStorage("bucket", "path", "oss-sc", "my-pvc", "20Gi")
	assert.Equal(t, "my-pvc", EffectiveObjectStoragePVCName(obj, nil, "ns", profile))
	assert.Equal(t, "oss-sc", EffectiveObjectStorageStorageClassName(obj, nil, "ns", profile))
}

func TestEffectiveObjectStoragePVCNameDerivedDeterministic(t *testing.T) {
	profile := mustProfile(t, cloudprofile.ProviderAliyun)
	obj := mountObjectStorage("bucket", "path", "", "", "20Gi")

	name1 := EffectiveObjectStoragePVCName(obj, nil, "ns", profile)
	name2 := EffectiveObjectStoragePVCName(obj, nil, "ns", profile)
	assert.Equal(t, name1, name2, "derived name must be deterministic")
	assert.True(t, strings.HasPrefix(name1, "lakeflow-oss-"), "got %q", name1)
	// 16 hex chars after the prefix.
	assert.Len(t, strings.TrimPrefix(name1, "lakeflow-oss-"), 16)

	sc1 := EffectiveObjectStorageStorageClassName(obj, nil, "ns", profile)
	assert.True(t, strings.HasPrefix(sc1, "lakeflow-oss-sc-"), "got %q", sc1)
	assert.Len(t, strings.TrimPrefix(sc1, "lakeflow-oss-sc-"), 16)
}

func TestEffectiveObjectStoragePVCNameIdentityDistinctions(t *testing.T) {
	profile := mustProfile(t, cloudprofile.ProviderAliyun)
	base := mountObjectStorage("bucket", "path", "", "", "20Gi")
	baseName := EffectiveObjectStoragePVCName(base, nil, "ns", profile)

	cases := map[string]*v1alpha1.ObjectStorage{
		"different bucket": mountObjectStorage("other", "path", "", "", "20Gi"),
		"different path":   mountObjectStorage("bucket", "other", "", "", "20Gi"),
		"different size":   mountObjectStorage("bucket", "path", "", "", "50Gi"),
	}
	for name, obj := range cases {
		t.Run(name, func(t *testing.T) {
			assert.NotEqual(t, baseName, EffectiveObjectStoragePVCName(obj, nil, "ns", profile))
		})
	}

	// Credential (secret) identity must change the derived name.
	withCred := EffectiveObjectStoragePVCName(base, &corev1.SecretReference{Name: "creds", Namespace: "ns"}, "ns", profile)
	assert.NotEqual(t, baseName, withCred)
	withCredOtherNs := EffectiveObjectStoragePVCName(base, &corev1.SecretReference{Name: "creds", Namespace: "other"}, "ns", profile)
	assert.NotEqual(t, withCred, withCredOtherNs)
}

func TestEffectiveObjectStoragePVCNameProviderDefaultsAWS(t *testing.T) {
	aws := mustProfile(t, cloudprofile.ProviderAWS)

	// Omitting endpoint/region must resolve to AWS defaults, so an explicit spec
	// that restates the defaults yields the SAME identity.
	omitted := mountObjectStorage("bucket", "path", "", "", "20Gi")
	explicitDefaults := mountObjectStorage("bucket", "path", "", "", "20Gi")
	explicitDefaults.Endpoint = "s3.amazonaws.com"
	explicitDefaults.Region = "us-east-1"
	assert.Equal(t,
		EffectiveObjectStoragePVCName(omitted, nil, "ns", aws),
		EffectiveObjectStoragePVCName(explicitDefaults, nil, "ns", aws),
		"default endpoint/region must match an explicit restatement of the defaults")

	// A non-default region must change the derived name.
	otherRegion := mountObjectStorage("bucket", "path", "", "", "20Gi")
	otherRegion.Region = "eu-west-1"
	assert.NotEqual(t,
		EffectiveObjectStoragePVCName(omitted, nil, "ns", aws),
		EffectiveObjectStoragePVCName(otherRegion, nil, "ns", aws))

	// The same logical config resolves to different names on different providers.
	aliyun := mustProfile(t, cloudprofile.ProviderAliyun)
	assert.NotEqual(t,
		EffectiveObjectStoragePVCName(omitted, nil, "ns", aws),
		EffectiveObjectStoragePVCName(omitted, nil, "ns", aliyun))
}
