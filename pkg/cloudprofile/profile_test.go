package cloudprofile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuiltinDefaultAndSelection(t *testing.T) {
	defaultProfile, err := Builtin("")
	require.NoError(t, err)
	assert.Equal(t, ProviderAliyun, defaultProfile.Name)

	aws, err := Builtin(ProviderAWS)
	require.NoError(t, err)
	assert.Equal(t, ProviderAWS, aws.Name)
	assert.Equal(t, "s3a", aws.ObjectStorage.FilesystemScheme)
}

func TestBuiltinUnknownProvider(t *testing.T) {
	_, err := Builtin("does-not-exist")
	assert.Error(t, err)
}

func TestBuiltinReturnsIndependentCopies(t *testing.T) {
	first, err := Builtin(ProviderAliyun)
	require.NoError(t, err)
	first.BlockStorage.Params["type"] = "mutated"
	first.ObjectStorage.ExtraClasspath[0] = "mutated"

	second, err := Builtin(ProviderAliyun)
	require.NoError(t, err)
	assert.Equal(t, "cloud_essd", second.BlockStorage.Params["type"])
	assert.NotEqual(t, "mutated", second.ObjectStorage.ExtraClasspath[0])
}
