# lakeflow-controller

A Kubernetes operator for data-processing workflows. It exposes a single declarative
API — the **LakeFlow** custom resource (`lakeflow.io/v1alpha1`) — and compiles it into the
underlying engine resources, so you describe _what_ you want to run and the controller
manages _how_ it runs on the cluster.

## Why lakeflow-controller

Running data pipelines on Kubernetes usually means stitching together several systems by
hand: Argo Workflows for the DAG, Spark Operator for Spark jobs, Volcano for batch
scheduling, plus all the storage, retry, and lifecycle glue in between. Each has its own API,
and keeping them consistent is tedious and error-prone.

`lakeflow-controller` collapses that into **one resource you actually care about — the
workflow**. You write a `LakeFlow` describing your tasks and how they depend on each other;
the controller generates and reconciles the right Argo/Spark/Volcano objects, wires up
storage and credentials, and keeps their status in sync. Concretely, it gives you:

- **One declarative surface** instead of three or four engine-specific APIs.
- **A data-oriented task model** (`spark` / `python` / `bash` executors) rather than raw pod
  templates — common concerns like Spark config layering, queues, and local-dir storage are
  built in.
- **Built-in operational behavior** — retries with backoff, resubmit/retry of failed runs,
  and suspend/stop/resume/terminate lifecycle control.
- **No lock-in to a custom engine** — it orchestrates standard, battle-tested projects, so
  your workloads still run on tools the ecosystem already knows.

## What it does

A `LakeFlow` is a workflow: a trigger plus a DAG of tasks. Each task runs on an executor
(`spark`, `python`, or `bash`). The controller handles:

- **Trigger modes** — scheduled (cron), immediate, and cross-workflow dependency triggers.
- **Task DAGs** — linear and graph dependencies between tasks, with bounded parallelism.
- **Retries & resubmit** — automatic task retry with exponential backoff, plus
  annotation-driven retry/resubmit of failed runs. These rerun operations are guarded by a
  **distributed lock** (Kubernetes `coordination/Lease` per Spark task), so concurrent or
  duplicate reruns — even across multiple controller replicas — are serialized instead of
  racing to launch the same task twice.
- **Versioned workflow templates** — when a LakeFlow's task topology changes, the controller
  renders a **new, timestamp-versioned `WorkflowTemplate`** (e.g. `my-flow-template-<unix>`)
  rather than mutating the existing one. In-flight runs keep using the template version they
  started with, while new runs pick up the latest — so editing a workflow never corrupts
  workflows already executing.
- **Lifecycle control** — suspend, stop, resume, and terminate running workflows.

## How it relates to Argo Workflows and Spark Operator

`lakeflow-controller` does not reimplement orchestration or Spark execution. It translates
each `LakeFlow` into resources owned by mature, battle-tested projects:

| Concern                           | Backing project                                                     |
|-----------------------------------|---------------------------------------------------------------------|
| Workflow orchestration / task DAG | **Argo Workflows** (`WorkflowTemplate`, `Workflow`, `CronWorkflow`) |
| Spark task execution              | **Spark Operator** (Kubeflow) `SparkApplication`                    |
| Batch scheduling                  | **Volcano**                                                         |

By trigger type:

- **Schedule** → `WorkflowTemplate` + `CronWorkflow`
- **Immediate** → `WorkflowTemplate` + `Workflow`
- **Dependency** → `WorkflowTemplate` only

This means the LakeFlow API gives you a concise, data-oriented surface while delegating the
heavy lifting to the standard Kubernetes data stack.

### Workflow-on-workflow dependencies

`lakeflow-controller` intentionally does **not** evaluate cross-workflow dependencies itself.
For a `Dependency`-triggered `LakeFlow`, the controller only renders the `WorkflowTemplate`
and then stops — it does not decide _when_ the workflow should run.

That decision is owned by a separate [`lakeflow-sensor`](https://github.com/pzhenzhou/lakeflow-sensor) project , which
watches upstream workflow states and creates the actual `Workflow` from the template once the
declared upstream conditions are satisfied. Keeping the controller out of dependency
evaluation keeps it stateless and focused on translation; the sensor handles the
event-driven, cross-workflow logic.

## Prerequisites

- A Kubernetes cluster (v1.24+)
- [Argo Workflows](https://argoproj.github.io/workflows/), [Spark Operator](https://github.com/kubeflow/spark-operator),
  and [Volcano](https://volcano.sh/) installed in the cluster
- `kubectl` and (for building) Go 1.24+, Docker 17.03+

## Installation

Install the CRD and deploy the controller:

```sh
# 1. Install the LakeFlow CRD
make install

# 2. Build and push the controller image
make docker-build docker-push IMG=<some-registry>/lakeflow-controller:tag

# 3. Deploy the controller
make deploy IMG=<some-registry>/lakeflow-controller:tag
```

Apply a sample workflow:

```sh
kubectl apply -k config/samples/
```

To tear everything down:

```sh
kubectl delete -k config/samples/   # remove sample workflows
make undeploy                        # remove the controller
make uninstall                       # remove the CRD
```

Run `make help` to see all available targets.

## Quick example

```yaml
apiVersion: lakeflow.io/v1alpha1
kind: LakeFlow
metadata:
  name: daily-etl
  namespace: default
spec:
  trigger:
    schedule:
      cron: "0 9 * * *"
      timezone: UTC
  parallelism: 2
  tasks:
    - name: extract
      executor: spark
      sparkExecutor:
        mainClass: com.example.Extract
        mainApplicationFile: s3://my-bucket/etl.jar
        queueName: default
    - name: validate
      executor: bash
      dependsOn: [ "extract" ]
      commandExecutor:
        inlineMode:
          command: [ "/bin/bash", "-c" ]
          args: [ "echo 'validation passed'" ]
```

## Using the API from Go

External projects can manage `LakeFlow` resources with a standard controller-runtime client —
no generated client needed. Import the types and register the scheme:

```go
import (
"k8s.io/apimachinery/pkg/runtime"
"sigs.k8s.io/controller-runtime/pkg/client"

"github.com/pzhenzhou/lakeflow-controller/api/v1alpha1"
)

scheme := runtime.NewScheme()
_ = v1alpha1.AddToScheme(scheme)

k8sClient, _ := client.New(config, client.Options{Scheme: scheme})

lf := &v1alpha1.LakeFlow{ /* ObjectMeta + Spec */ }
_ = k8sClient.Create(ctx, lf) // also: Get / Update / List / Delete
```

## Roadmap

LakeFlow is designed as an engine-agnostic surface. Today it targets Spark via the Spark
Operator; planned work includes first-class support for additional engines so a single
workflow can mix executors:

- [x] **`lakeflow-sensor`** — companion project that evaluates workflow-on-workflow
  dependencies and triggers `Dependency` workflows
- [ ] **Ray** task executor
- [ ] Pluggable engine abstraction so new compute backends can be added without API changes

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0. You may obtain a copy of the License at
<http://www.apache.org/licenses/LICENSE-2.0>.
