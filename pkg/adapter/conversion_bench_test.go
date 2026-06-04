package adapter

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	"k8s.io/apimachinery/pkg/runtime"
)

// benchClock is a fixed clock so versioned template names are deterministic and
// the benchmark measures conversion cost rather than time syscalls.
func benchClock() time.Time { return time.Unix(1700000000, 0).UTC() }

// quietLogger swaps the package logger for a no-op during a benchmark and
// returns a restore func. The adapter builders log heavily at Debug level; left
// enabled the benchmark output is unusable and dominated by stdout I/O, which
// would swamp the signal we are trying to baseline.
func quietLogger() func() {
	orig := logger
	logger = logr.Discard()
	return func() { logger = orig }
}

func benchScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		tb.Fatalf("add scheme: %v", err)
	}
	return s
}

func loadBenchFixture(tb testing.TB, name string) *v1alpha1.LakeFlow {
	tb.Helper()
	path := filepath.Join("testdata", "fixtures", name+".yaml")
	lw, err := common.LoadFirstLakeFlowFromYaml(benchScheme(tb), path)
	if err != nil {
		tb.Fatalf("load fixture %s: %v", name, err)
	}
	return lw
}

// scaleTasks returns a deep copy of lw whose first task is duplicated n times
// with unique names and no inter-task dependencies. This exposes the per-task
// task-slice rescan cost in the conversion path (each executor builder currently
// re-finds its own task by scanning Spec.Tasks, making conversion O(n^2)).
func scaleTasks(lw *v1alpha1.LakeFlow, n int) *v1alpha1.LakeFlow {
	out := lw.DeepCopy()
	base := out.Spec.Tasks[0]
	tasks := make([]v1alpha1.Task, 0, n)
	for i := 0; i < n; i++ {
		t := *base.DeepCopy()
		t.Name = fmt.Sprintf("%s-%d", base.Name, i)
		t.DependsOn = nil
		tasks = append(tasks, t)
	}
	out.Spec.Tasks = tasks
	return out
}

// BenchmarkConvert baselines full LakeFlow -> Argo conversion cost across
// workflow sizes. The super-linear scaling here is the O(n^2) task-lookup the
// task-index fix targets; ReportAllocs surfaces allocation pressure.
func BenchmarkConvert(b *testing.B) {
	defer quietLogger()()
	b.Setenv(common.DeploymentEnv, "production")
	lw := loadBenchFixture(b, "spark-features")
	for _, n := range []int{10, 50, 100} {
		lwN := scaleTasks(lw, n)
		b.Run(fmt.Sprintf("tasks=%d", n), func(b *testing.B) {
			conv := NewWorkflowConverterWithClock(testAliyunProfile(), benchClock)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := conv.Convert(lwN, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSparkConfAssembly baselines the SparkConf map assembly (defaults ->
// feature configs -> user overrides). ReportAllocs is the key signal: the
// SparkConf mutator refactor must not increase allocations vs the current
// clone-and-lo.Assign helper chain.
func BenchmarkSparkConfAssembly(b *testing.B) {
	defer quietLogger()()
	lw := loadBenchFixture(b, "spark-features")
	task := &lw.Spec.Tasks[0]
	sb := &sparkTaskRenderer{}
	app := sb.buildSparkApplicationFromSpec(lw, task)
	spec := *task.TaskSpec.SparkExecutorSpec
	resolver := newK8sCredentialResolver(lw.Namespace)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sb.setSparkSpecConfig(app, spec, resolver, testAliyunProfile())
	}
}
