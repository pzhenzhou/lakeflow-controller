package adapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
	"github.com/pzhenzhou/lakeflow-controller/pkg/cloudprofile"
	"github.com/pzhenzhou/lakeflow-controller/pkg/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CredentialResolver resolves cluster-backed credentials referenced by a spec.
// It is the single, explicit seam for the conversion path's side effects: the
// renderer and task builders are otherwise pure (they only describe desired
// objects). Implementations must memoize within a single conversion so a
// credentialsRef shared by many tasks is fetched at most once.
type CredentialResolver interface {
	// HiveMetastore returns the username/password for a Hive metastore spec,
	// resolving a referenced Secret when present and otherwise falling back to
	// inline credentials. The returned error is informational only; callers
	// treat a failure as "no credentials" (the connection is still attempted).
	HiveMetastore(spec *v1alpha1.HiveMetastoreSpec) (user, password string, err error)
}

// ArgoRenderContext carries the per-conversion state shared by the Argo renderer
// and the task renderers. It is built once per Convert and never stored on the
// singleton TaskRendererFactory renderers, so per-reconciliation state (clock,
// credential resolver) cannot leak across conversions. The converter passes the
// already-resolved *v1alpha1.Task directly to RenderTask, so no task index is needed.
type ArgoRenderContext struct {
	// LakeFlow is the source object being converted.
	LakeFlow *v1alpha1.LakeFlow
	// Clock is the injectable clock used for versioned template names (defaults
	// to time.Now in production; fixed in tests for deterministic output).
	Clock func() time.Time
	// CredentialResolver resolves Secret-backed credentials during conversion.
	CredentialResolver CredentialResolver
	// Namespace is the target namespace (the LakeFlow namespace); it is also the
	// default Secret namespace for credential resolution.
	Namespace string
	// CloudProfile provides provider-specific Spark/storage rendering defaults.
	CloudProfile cloudprofile.CloudProfile
}

// newArgoRenderContext builds the shared context for one conversion: it wires a
// per-conversion credential resolver whose default Secret namespace is the
// LakeFlow namespace.
func newArgoRenderContext(lw *v1alpha1.LakeFlow, clock func() time.Time, profile cloudprofile.CloudProfile) ArgoRenderContext {
	if clock == nil {
		clock = time.Now
	}
	if profile.Name == "" {
		profile, _ = cloudprofile.Builtin(cloudprofile.ProviderAliyun)
	}
	return ArgoRenderContext{
		LakeFlow:           lw,
		Clock:              clock,
		CredentialResolver: newK8sCredentialResolver(lw.Namespace),
		Namespace:          lw.Namespace,
		CloudProfile:       profile,
	}
}

var _ CredentialResolver = (*k8sCredentialResolver)(nil)

// k8sCredentialResolver resolves credentials from Kubernetes Secrets, memoizing
// results for the lifetime of one conversion. The Kubernetes client is created
// lazily on the first Secret lookup, so pure conversions (no credentialsRef)
// never touch the cluster.
type k8sCredentialResolver struct {
	defaultNamespace string

	mu          sync.Mutex
	clientSet   kubernetes.Interface
	clientErr   error
	clientReady bool
	cache       map[string]hiveCredential
}

type hiveCredential struct {
	user     string
	password string
}

func newK8sCredentialResolver(defaultNamespace string) *k8sCredentialResolver {
	return &k8sCredentialResolver{
		defaultNamespace: defaultNamespace,
		cache:            make(map[string]hiveCredential),
	}
}

func (r *k8sCredentialResolver) client() (kubernetes.Interface, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.clientReady {
		r.clientSet, r.clientErr = common.NewKubClientSet()
		r.clientReady = true
	}
	return r.clientSet, r.clientErr
}

func (r *k8sCredentialResolver) HiveMetastore(spec *v1alpha1.HiveMetastoreSpec) (string, string, error) {
	if ref := spec.CredentialsRef; ref != nil && ref.Name != "" {
		ns := ref.Namespace
		if ns == "" {
			ns = r.defaultNamespace
		}
		cacheKey := ns + "/" + ref.Name
		r.mu.Lock()
		cached, ok := r.cache[cacheKey]
		r.mu.Unlock()
		if ok {
			return cached.user, cached.password, nil
		}

		clientSet, err := r.client()
		if err != nil {
			return "", "", err
		}
		secret, getErr := clientSet.CoreV1().Secrets(ns).Get(context.Background(), ref.Name, metav1.GetOptions{})
		if getErr != nil {
			logger.Error(getErr, "Failed to get secret", "secret", ref.Name, "namespace", ns)
			return "", "", getErr
		}
		creds := hiveCredential{
			user:     string(secret.Data[hiveMetaUserNameKey]),
			password: string(secret.Data[hiveMetaPasswordKey]),
		}
		r.mu.Lock()
		r.cache[cacheKey] = creds
		r.mu.Unlock()
		logger.Info("Successfully retrieved Hive metastore credentials from secret",
			"UserName", creds.user, "namespace", ns)
		return creds.user, creds.password, nil
	}
	if spec.UserName != "" && spec.Password != "" {
		return spec.UserName, spec.Password, nil
	}
	return "", "", fmt.Errorf("no credentials found for Hive metastore")
}
