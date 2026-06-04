package cloudprofile

import storagev1 "k8s.io/api/storage/v1"

const (
	awsS3Provisioner    = "s3.csi.aws.com"
	awsEBSProvisioner   = "ebs.csi.aws.com"
	awsFilesystemScheme = "s3a"

	awsS3ReclaimPolicy     = "Retain"
	awsEBSReclaimPolicy    = "Delete"
	awsSparkConfPrefix     = "spark.hadoop.fs.s3a"
	awsFilesystemImpl      = "org.apache.hadoop.fs.s3a.S3AFileSystem"
	awsDefaultRegion       = "us-east-1"
	awsDefaultEndpoint     = "s3.amazonaws.com"
	awsDefaultStorageClass = "ebs-sc"
	awsHadoopAWSJarPath    = "/mnt/spark/jars/hadoop-aws/hadoop-aws.jar:/mnt/spark/jars/hadoop-aws/aws-java-sdk-bundle.jar"
)

func awsProfile() CloudProfile {
	return CloudProfile{
		Name: ProviderAWS,
		ObjectStorage: ObjectStorageProfile{
			CSIDriver:         awsS3Provisioner,
			ReclaimPolicy:     awsS3ReclaimPolicy,
			VolumeBindingMode: storagev1.VolumeBindingWaitForFirstConsumer,
			DefaultEndpoint:   awsDefaultEndpoint,
			DefaultRegion:     awsDefaultRegion,
			FilesystemScheme:  awsFilesystemScheme,
			FilesystemImpl:    awsFilesystemImpl,
			SparkConfPrefix:   awsSparkConfPrefix,
			ExtraClasspath:    []string{awsHadoopAWSJarPath},
			StorageClassParameterRules: []StorageClassParameterRule{
				{Key: "bucketName", Source: ParamSourceBucket},
				{Key: "prefix", Source: ParamSourcePath},
				{Key: "region", Source: ParamSourceRegion},
				{Key: "endpoint", Source: ParamSourceEndpoint},
				{Key: "csi.storage.k8s.io/node-publish-secret-name", Source: ParamSourceSecretName},
				{Key: "csi.storage.k8s.io/node-publish-secret-namespace", Source: ParamSourceSecretNamespace},
			},
		},
		BlockStorage: BlockStorageProfile{
			CSIDriver:           awsEBSProvisioner,
			ReclaimPolicy:       awsEBSReclaimPolicy,
			VolumeBindingMode:   storagev1.VolumeBindingWaitForFirstConsumer,
			DefaultStorageClass: awsDefaultStorageClass,
			Params: map[string]string{
				"type":   "gp3",
				"fsType": "xfs",
			},
		},
		Auth: AuthProfile{
			DefaultMode:       "secretRef",
			RoleAnnotationKey: "eks.amazonaws.com/role-arn",
		},
		SparkRuntime: defaultSparkRuntimeProfile(),
	}
}
