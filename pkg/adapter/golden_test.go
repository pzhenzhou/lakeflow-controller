package adapter

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloud"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// updateGolden regenerates the golden files instead of comparing against them.
// Run: go test ./pkg/adapter/ -run TestGolden -update
var updateGolden = flag.Bool("update", false, "update golden files in testdata/golden")

// goldenClock is a fixed clock so versioned template names are deterministic.
// 1700000000 -> 2023-11-14T22:13:20Z.
func goldenClock() time.Time { return time.Unix(1700000000, 0).UTC() }

// TestGolden is a characterization (golden) safety net that locks the current
// adapter output before the upcoming pkg/adapter refactor and LakeFlow type
// rename. It snapshots the pure, deterministic build artifacts:
//   - the assembled Spark SparkConf map,
//   - the cleaned SparkApplication manifest,
//   - the Argo WorkflowTemplate/CronWorkflow/Workflow from a full Convert(),
//   - the desired OSS StorageClass/Secret/PVC and executor StorageClass.
//
// Determinism is handled by pinning DEPLOYMENT_ENV (image selection) and injecting
// a fixed clock (versioned template names). Fixtures use obviously-fake credentials
// and inline Hive3 creds so no Kubernetes client is required.
func TestGolden(t *testing.T) {
	runGoldenProfile(t, cloudprofile.ProviderAliyun, "")
}

func TestGoldenAWS(t *testing.T) {
	runGoldenProfile(t, cloudprofile.ProviderAWS, "aws")
}

func runGoldenProfile(t *testing.T, providerName, goldenPrefix string) {
	// Pin image-selection env so Spark/command image fields are stable.
	t.Setenv(common.DeploymentEnv, "production")
	scheme := goldenScheme(t)
	profile, err := cloudprofile.Builtin(providerName)
	require.NoError(t, err)

	cases := []struct {
		name        string // fixture file base under testdata/fixtures
		sparkTask   string // spark task to snapshot SparkConf + SparkApplication (empty = skip)
		storageTask string // spark task whose OSS PVCSpec to snapshot as storage (empty = skip)
		convert     bool   // snapshot full Convert() output
	}{
		{name: "spark-schedule", sparkTask: "daily-transform", convert: true},
		{name: "spark-features", sparkTask: "feature-job", convert: true},
		{name: "pyspark", sparkTask: "py-job", convert: true},
		{name: "command-script", convert: true},
		{name: "command-python-project", convert: true},
		{name: "dependency", convert: true},
		{name: "storage", sparkTask: "storage-job", storageTask: "storage-job"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lw := loadFixture(t, scheme, tc.name)

			if tc.sparkTask != "" {
				conf, manifest := snapshotSpark(t, lw, tc.sparkTask, profile)
				assertGolden(t, goldenName(goldenPrefix, tc.name+".sparkconf.golden.yaml"), conf)
				assertGolden(t, goldenName(goldenPrefix, tc.name+".sparkapp.golden.yaml"), manifest)
			}

			if tc.convert {
				conv := NewWorkflowConverterWithClock(profile, goldenClock)
				cr, err := conv.Convert(lw, nil)
				require.NoError(t, err)
				if cr.WorkflowTemplate != nil {
					assertGolden(t, goldenName(goldenPrefix, tc.name+".workflowtemplate.golden.yaml"), marshalYAML(t, cr.WorkflowTemplate))
				}
				if cr.CronWorkflow != nil {
					assertGolden(t, goldenName(goldenPrefix, tc.name+".cronworkflow.golden.yaml"), marshalYAML(t, cr.CronWorkflow))
				}
				if cr.Workflow != nil {
					assertGolden(t, goldenName(goldenPrefix, tc.name+".workflow.golden.yaml"), marshalYAML(t, cr.Workflow))
				}
			}

			if tc.storageTask != "" {
				snapshotStorage(t, lw, tc.storageTask, profile, goldenPrefix)
			}
		})
	}
}

func goldenName(prefix, filename string) string {
	if prefix == "" {
		return filename
	}
	return filepath.Join(prefix, filename)
}

func goldenScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	return scheme
}

func loadFixture(t *testing.T, scheme *runtime.Scheme, name string) *v1alpha1.LakeFlow {
	t.Helper()
	path := filepath.Join("testdata", "fixtures", name+".yaml")
	lw, err := common.LoadFirstLakeFlowFromYaml(scheme, path)
	require.NoError(t, err, "failed to load fixture %s", path)
	return lw
}

func findTask(t *testing.T, lw *v1alpha1.LakeFlow, name string) *v1alpha1.Task {
	t.Helper()
	for i := range lw.Spec.Tasks {
		if lw.Spec.Tasks[i].Name == name {
			return &lw.Spec.Tasks[i]
		}
	}
	t.Fatalf("task %q not found in fixture %q", name, lw.Name)
	return nil
}

// snapshotSpark mirrors the pure portion of sparkTaskRenderer.build: it builds
// the SparkApplication and assembles its SparkConf without performing any cluster side
// effects (PVC/StorageClass/Secret creation), so it is safe to run without a cluster.
// Inline Hive credentials resolve offline through the k8s resolver, so the snapshot
// stays byte-identical to production output.
func snapshotSpark(t *testing.T, lw *v1alpha1.LakeFlow, taskName string, profile cloudprofile.CloudProfile) (sparkConf string, manifest string) {
	t.Helper()
	task := findTask(t, lw, taskName)
	require.NotNil(t, task.TaskSpec.SparkExecutorSpec, "task %q has no sparkExecutor", taskName)

	sb := &sparkTaskRenderer{}
	ctx := &ArgoRenderContext{LakeFlow: lw, Namespace: lw.Namespace, CloudProfile: profile}
	app := sb.buildSparkApplicationFromSpec(lw, task)
	sb.setSparkSpecConfig(app, *task.TaskSpec.SparkExecutorSpec, newK8sCredentialResolver(lw.Namespace), profile)
	sb.setPySparkDependencies(app, task.TaskSpec.SparkExecutorSpec)
	sb.setSparkSpecVolumes(ctx, app, *task.TaskSpec.SparkExecutorSpec)

	confBytes, err := yaml.Marshal(app.Spec.SparkConf)
	require.NoError(t, err)
	manifestBytes, err := yaml.Marshal(app)
	require.NoError(t, err)
	return string(confBytes), cleanSparkApplicationManifest(string(manifestBytes))
}

// snapshotStorage captures the desired OSS StorageClass/PVC and executor StorageClass
// produced by the pure describe* helpers. Object-storage credentials are now supplied
// via a user-provided Secret referenced by credentialsRef, so the operator no longer
// builds (and this snapshot no longer captures) a credential Secret.
func snapshotStorage(t *testing.T, lw *v1alpha1.LakeFlow, taskName string, profile cloudprofile.CloudProfile, goldenPrefix string) {
	t.Helper()
	task := findTask(t, lw, taskName)
	spark := task.TaskSpec.SparkExecutorSpec
	require.NotNil(t, spark, "task %q has no sparkExecutor", taskName)
	require.NotNil(t, spark.ObjectStorage, "task %q has no objectStorage", taskName)
	require.NotNil(t, spark.BlockStorage, "task %q has no blockStorage", taskName)

	provider := cloud.NewProvider(profile)
	sc := provider.DescribeObjectStorageClass(spark.ObjectStorage, spark.CredentialsRef, lw.Namespace)
	assertGolden(t, goldenName(goldenPrefix, "storage.storageclass.golden.yaml"), marshalYAML(t, sc))

	pvc := provider.DescribeObjectStoragePVC(spark.ObjectStorage, spark.CredentialsRef, lw.Namespace)
	assertGolden(t, goldenName(goldenPrefix, "storage.pvc.golden.yaml"), marshalYAML(t, pvc))

	execSC := provider.DescribeBlockStorageClass(spark.BlockStorage.StorageClass)
	assertGolden(t, goldenName(goldenPrefix, "storage.executor-storageclass.golden.yaml"), marshalYAML(t, execSC))
}

func marshalYAML(t *testing.T, obj interface{}) string {
	t.Helper()
	b, err := yaml.Marshal(obj)
	require.NoError(t, err)
	return string(b)
}

func assertGolden(t *testing.T, filename, got string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", filename)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "missing golden %s (run with -update to create)", filename)
	assert.Equal(t, string(want), got, "golden mismatch for %s (run with -update to refresh)", filename)
}
