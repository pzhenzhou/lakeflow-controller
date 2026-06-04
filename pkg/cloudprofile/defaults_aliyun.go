package cloudprofile

import storagev1 "k8s.io/api/storage/v1"

const (
	aliyunOSSProvisioner  = "ossplugin.csi.alibabacloud.com"
	aliyunDiskProvisioner = "diskplugin.csi.alibabacloud.com"

	aliyunOSSReclaimPolicy   = "Retain"
	aliyunBlockReclaimPolicy = "Delete"
	aliyunOSSMountOptions    = "-o allow_other -o umask=000"

	aliyunFilesystemScheme         = "oss"
	aliyunFilesystemImpl           = "com.aliyun.jindodata.oss.JindoOssFileSystem"
	aliyunAbstractFilesystemImpl   = "com.aliyun.jindodata.oss.JindoOSS"
	aliyunSparkConfPrefix          = "spark.hadoop.fs.oss"
	aliyunDefaultBlockStorageClass = "alicloud-disk-essd-pl0"
	aliyunJindoSdkJarPath          = "/mnt/spark/jars/jindosdk-6.9.1/jindo-core-6.9.1-nextarch.jar:/mnt/spark/jars/jindosdk-6.9.1/jindo-sdk-6.9.1-nextarch.jar:/mnt/spark/jars/jindosdk-6.9.1/jindo-core-linux-ubuntu22-x86_64-6.9.1-nextarch.jar"
)

func aliyunProfile() CloudProfile {
	return CloudProfile{
		Name: ProviderAliyun,
		ObjectStorage: ObjectStorageProfile{
			CSIDriver:              aliyunOSSProvisioner,
			ReclaimPolicy:          aliyunOSSReclaimPolicy,
			VolumeBindingMode:      storagev1.VolumeBindingWaitForFirstConsumer,
			DefaultEndpoint:        "",
			FilesystemScheme:       aliyunFilesystemScheme,
			FilesystemImpl:         aliyunFilesystemImpl,
			AbstractFilesystemImpl: aliyunAbstractFilesystemImpl,
			SparkConfPrefix:        aliyunSparkConfPrefix,
			ExtraClasspath:         []string{aliyunJindoSdkJarPath},
			StorageClassParameterRules: []StorageClassParameterRule{
				{Key: "bucket", Source: ParamSourceBucket},
				{Key: "path", Source: ParamSourcePath},
				{Key: "url", Source: ParamSourceEndpoint},
				{Key: "otherOpts", Source: ParamSourceLiteral, Literal: aliyunOSSMountOptions},
				{Key: "csi.storage.k8s.io/node-publish-secret-name", Source: ParamSourceSecretName},
				{Key: "csi.storage.k8s.io/node-publish-secret-namespace", Source: ParamSourceSecretNamespace},
			},
		},
		BlockStorage: BlockStorageProfile{
			CSIDriver:           aliyunDiskProvisioner,
			ReclaimPolicy:       aliyunBlockReclaimPolicy,
			VolumeBindingMode:   storagev1.VolumeBindingWaitForFirstConsumer,
			DefaultStorageClass: aliyunDefaultBlockStorageClass,
			Params: map[string]string{
				"type":             "cloud_essd",
				"fstype":           "xfs",
				"performanceLevel": "PL1",
			},
		},
		Auth: AuthProfile{
			DefaultMode:       "secretRef",
			RoleAnnotationKey: "alibabacloud.com/role-name",
		},
		SparkRuntime: defaultSparkRuntimeProfile(),
	}
}
