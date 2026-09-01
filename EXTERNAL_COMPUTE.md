# Running Metaflow steps on AWS compute

`@remote_step` moves a step's *body* onto AWS compute. The Metaflow task stays on EKS doing
nothing but submitting and waiting, so it can be declared small while the work runs on an instance
sized per job.

Two backends today, **SageMaker** and **AWS Batch**, chosen per step by passing one to the
decorator.

Outerbounds bills by the size a step **declares**, not what it uses. A step pinned at
`@kubernetes(cpu=190)` bills at that band for its whole duration, even while idle.

> Snowflake compute pools were evaluated and dropped — one job per node, no bin-packing, pool size
> fixed rather than per-job, `MAX_NODES` concurrency ceiling. Kept on
> `poc/snowflake-compute-to-run-metaflow-steps` for reference. Snowflake remains the *data* source.

> **This is the library half.** The decorator and its backends live here; the flows that exercise
> them live in `data-science-projects` under `domains/reference/cost-tracking/`. A project picks up
> a work-in-progress version by pointing at this branch:
>
> ```
> uv add "ds-platform-utils @ git+https://github.com/patterninc/ds-platform-utils.git@feat/external-compute-aws"
> ```

**Status:** verified end to end on Kubernetes — task on EKS, body on SageMaker, including
`ds_platform_utils` reading and writing Snowflake from the container as the same role an ordinary
step uses. Batch is verified from a pod too, on Fargate Spot.

## Usage

```python
from ds_platform_utils.metaflow import BACKEND_PACKAGES, PYTHON_VERSION, remote_step

FLOW_PACKAGES = {**BACKEND_PACKAGES, "xgboost": ""}

@pypi_base(python=PYTHON_VERSION, packages=FLOW_PACKAGES)
class MyFlow(FlowSpec):

    @step
    @remote_step("sagemaker", cpu=8, memory=32, snowflake=True, warm_seconds=1800)
    def train(self):
        df = query_pandas_from_snowflake(query=SQL_DIR / "training_sample.sql")
        self.model = fit(df)
        self.next(self.end)
```

The body is written exactly as it would be for EKS. `@step` stays outermost so Metaflow still sees
the step, and `@pypi_base` is unchanged from any other flow in the repo.

Everything comes from one import. `backend` is `"sagemaker"` or `"batch"` — a string rather than a
built object, which keeps the choice explicit without making a flow import a factory and know which
module it lives in. Pass a `ComputeBackend` instead for anything the names cannot express: another
account, a different queue, a custom image.

| argument | |
| --- | --- |
| `cpu` / `memory` / `gpu` / `instances` | sizes the job; the backend picks the smallest instance that fits |
| `snowflake=True` | stages short-lived credentials so the body can read and write Snowflake |
| `warm_seconds=` | keeps the instance for the next job (SageMaker only; refused on Batch rather than ignored) |
| `packages=` | rarely needed — the step inherits the flow's `@pypi_base` |
| `fingerprint=False` | skips recording where the body ran, which simplifies a foreach join |

## Choosing a backend

Same body, same decorator, one argument apart. Measured on an identical RandomForest fit with
`n_jobs` pinned to the requested vCPU, `r2` matching to four decimals across every run.

Overhead is queue + container start + pip; body is the fit itself. All figures submitted from a
Metaflow pod:

| | SageMaker warm | SageMaker cold | Batch (Fargate Spot) |
| --- | --- | --- | --- |
| overhead | **31.3s** | 169.0s | 75–86s |
| body, same work | **121s** | 121s | 174–180s |
| total | **153s** | 290s | 257–265s |

**SageMaker with a warm pool wins on both axes** — the same job end to end in 153s against Batch's
~260s. Spot pricing has to beat a 1.7× wall-clock ratio before Batch is actually cheaper, which is
a higher bar than "a third the instance price" suggests.

Three things that took measuring to see:

- **Fargate's vCPU is ~40% slower.** 174s against 121s for identical work at two workers each.
  m7i is a current-generation dedicated core; a Fargate vCPU is shared and throttled. An earlier
  run appeared to show the opposite, but that was `n_jobs=-1` reading `os.cpu_count()` — which
  reports the *host*, so Fargate got four workers and SageMaker two. Pin `n_jobs`.
- **SageMaker's cold path is much slower from a pod than from a laptop** — 169s against ~85s,
  reproduced across runs. Batch shows no such penalty. Warm pools skip the cold path entirely and
  the penalty disappears with it, landing below even the local warm figure.
- **Warm pools only help back-to-back work.** The first job creates the pool and pays full price;
  a step scheduled daily always pays the cold 169s. The instance also bills while idle.

| | prefer |
| --- | --- |
| several decorated steps, or a foreach | **SageMaker** with `keep_alive_seconds` |
| a single step run once a day | either — both pay a cold start |
| over 16 vCPU / 120 GB, or GPU | **SageMaker** (Fargate's ceiling) |
| cost-sensitive, interruption-tolerant | **Batch** on Spot, if the discount beats 1.7× the runtime |
| non-ML work | **Batch** — SageMaker's job conventions are overhead |

Spot capacity can be reclaimed mid-job. Batch reports that as a failure with a reason, which
surfaces in the exception rather than appearing as a mystery.

## Where it applies

- **Yes:** training, prediction, heavy pandas — anything you had to size medium or large.
- **No — SQL steps.** `publish(query=...)` already runs in a warehouse.
- **No — short steps.** ~46s (warm) to ~85s (cold) of fixed overhead has to be earned back.

Adopt one step at a time; undecorated steps are untouched.

## Adopting it

**1. Declare packages once, on the flow.** A decorated step inherits the flow's `@pypi_base`, so
the body imports the same names wherever it runs. There is no second list.

```python
FLOW_PACKAGES = {**BACKEND_PACKAGES, "xgboost": ""}

@pypi_base(python=PYTHON_VERSION, packages=FLOW_PACKAGES)
```

The backend then drops whatever the image already has (`IMAGE_PACKAGES`), which keeps the install
cheap. Only *unversioned* requirements are dropped — a pin is a statement of intent.

**2. Anything a later step reads must be assigned unconditionally.** A write inside an `if` that
does not run simply will not exist. The decorator now says so at the boundary
(`'x' not set -- its branch did not run`) instead of leaving you an `AttributeError` two steps
later, but it does not create the attribute.

**3. In a foreach, fix the join.** Each branch returns its own `self.remote_runtimes`, so
`merge_artifacts` refuses to choose. Set it explicitly and exclude it — `join_probe` in
`oos_replica_flow.py` shows the shape.

**4. Pass `--package-suffixes='.csv,.sql,.json,.toml,.yaml,.yml,.txt'`** so `.sql`/`.yaml` reach
the pod. Generated `poe flows:` tasks already do; raw `uv run` does not.

## What it handles

| | |
| --- | --- |
| **`self`** | AST finds which `self.X` the body reads and writes; only those ship. A proxy stands in; writes are copied back on success. |
| **`metaflow`** | Never imported remotely — the body is cloudpickled by value, and module-scope imports never travel. |
| **Your code** | Every directory beside the flow file holding Python is uploaded (`helpers`, `utils`, namespace packages). Nothing is hardcoded. |
| **DataFrames** | Parquet-encoded both ways, so pandas versions on either side stop mattering. |
| **Other artifacts** | Returned as one opaque pickled blob, so any picklable object survives — fitted `XGBClassifier`, int-keyed dicts, all verified. |
| **`Path` globals** | A module-level `SQL_DIR` is bundled and rewritten, so `.sql` reads work unchanged. |
| **Python version** | Body is bytecode; a mismatch with the image fails with that explanation, not an opaque unpickling error. |
| **Transport** | Three S3 URIs in environment variables -- payload, code, result. No service's own input/output channels, which is what lets one runtime serve SageMaker, Batch and anything after them. |

Objects come back as ordinary Metaflow artifacts. Their class must be importable on the Metaflow
side too — that is why `xgboost` sits in `@pypi_base` even though the fit happens remotely.

## Failure and interruption

**A body that raises does not leave a runaway job.** The container exits non-zero, SageMaker marks
the job `Failed`, and billing stops. Nothing is left running, and the decorator does not try to
stop a job AWS already ended.

The exception carries the container's log tail, because SageMaker's own `FailureReason` for a
crashed body is `AlgorithmError: , exit code: 1` — true and useless.

**A job whose watcher goes away is stopped.** It has no idea nobody is listening, so:

| | |
| --- | --- |
| interrupt / exception while waiting | job stopped, staged Snowflake secret deleted |
| pod eviction (SIGTERM) | same, via an explicit handler — Python's default for SIGTERM skips unwinding entirely |
| SIGKILL | nothing can run; `max_runtime_seconds` is the only backstop |

`max_runtime_seconds` defaults to **24 hours**. It is a safety net, not a timeout: this decorator
is for heavy steps, and a cap that kills legitimate training breaks the flow every time where a
stranded job merely costs money occasionally. Tighten it when you know the duration:
pass a `ComputeBackend` built with
`max_runtime_seconds=`, since it is a backend setting rather than a per-step one.

**Nothing is applied on failure** — a failed step leaves flow state untouched.

## Cost and time

Overhead is everything before the body starts: queue, provisioning, image pull, pip.

| configuration | overhead |
| --- | --- |
| stock PyTorch DLC (~5 GB image) | ~169s |
| slim py311, m5, cold | 92–99s |
| slim py311, m7i, cold | **85.0s** |
| slim py311, m7i, **warm pool reused** | **45.9s** |

**Warm pools cut the floor 46%**, confirmed by `WarmPoolStatus: Reused`. Quota is the catch:

- **m5 is 0** in every account checked — `keep_alive_seconds` on m5 is silently ineffective.
- **m7i/c7i/r7i are 30.** `SAGEMAKER_INSTANCE_TYPES` uses m7i for that reason; a test guards it.
- **GPU families are 0**, so a GPU step always pays the cold floor.

A warm instance bills while idle for up to `keep_alive_seconds` — worth it for a flow with several
decorated steps, wasteful for one step run daily.

**The waiting task is cheap.** Measured from OBC attribution: a cpu=2/8GB pod bills **~1 OBC/min**
(~$0.006/min), so the round trip costs ~$0.014 cold, ~$0.008 warm.

**Still unmeasured:** OBC avoided versus SageMaker instance-seconds spent. That is the number that
decides adoption. OBC attribution lands daily, so a run is measurable the following day.

## Account setup

Currently **sandbox (`847068433460`)** — we hold AdministratorAccess there, so nothing waits on a
platform request. `data-science-prod` (`209479263910`) is the eventual home, being where Outerbounds
runs Metaflow pods. Moving is a change of `REMOTE_STEP_AWS_ACCOUNT` and profile; the sandbox
resources deliberately carry the same names.

Provisioned (all in `aws_env.py`):

- `sagemaker-us-west-2-<account>` — payload bucket. The `sagemaker-*` name matters:
  `AmazonSageMakerFullAccess` grants S3 **and** `AmazonSageMaker-*` secrets on it, so no extra policy.
- `remote-step-runtime:py311` — the image (see `docker/Dockerfile`).
- `RemoteStepSageMakerExecutionRole` — trusts `sagemaker.amazonaws.com`, has `AmazonSageMakerFullAccess`.
- `RemoteStepSubmitterRole` — assumed by the Metaflow pod.

### Batch (Fargate Spot)

Created once per account, all self-service:

```bash
aws batch create-compute-environment --compute-environment-name remote-step-fargate-spot \
  --type MANAGED --state ENABLED \
  --compute-resources "type=FARGATE_SPOT,maxvCpus=64,subnets=<public-subnets>,securityGroupIds=<sg>"

aws batch create-job-queue --job-queue-name remote-step-spot --state ENABLED --priority 1 \
  --compute-environment-order "order=1,computeEnvironment=remote-step-fargate-spot"
```

Plus a job definition (`remote-step`), an execution role (pulls the image, writes logs) and a job
role (S3 + the ephemeral Snowflake secret). One job definition serves every step: sizing, command
and environment are container overrides at submit time, so no revision accumulates.

Two things that cost a debugging round each:

- **The security group must allow egress.** The shared default SG had its outbound rule stripped,
  so tasks could not reach ECR and failed with `ResourceInitializationError ... i/o timeout`. A
  dedicated `remote-step-batch` SG was created rather than loosening a shared one that is locked
  down deliberately.
- **The image cannot be a container override.** Batch pins it in the job definition and accepts
  only sizing, command and environment, so changing the Batch image means a new job definition
  revision. `BatchBackend` therefore takes no `image_uri` at all.

### The two IAM findings that cost the most to discover

**1. Source identity propagates the whole assume-role chain.**

```
obp-<id>-task  →  RemoteStepSubmitterRole  →  (SageMaker) RemoteStepSageMakerExecutionRole
```

Outerbounds stamps a source identity on the pod's credentials. **Every** trust policy in that chain
needs `sts:SetSourceIdentity` alongside `sts:AssumeRole` — including the execution role's, for the
`sagemaker.amazonaws.com` principal. Missing it fails with `Could not assume role`.

**2. The pod can already assume tagged roles — nothing is needed from Outerbounds.** Its inline
policy allows `sts:AssumeRole` on anything tagged
`outerbounds.com/accessible-by-deployment=pattern`. Tag your role and the door is open; without the
tag every assume is denied.

`metaflow/aws_env.py` detects the pod (`METAFLOW_KUBERNETES_POD_NAME`) and assumes the submitter role only
there — locally the SSO profile has the permissions directly. This has to be a default in code:
**Metaflow does not forward environment variables to Kubernetes tasks.**

### Run it

```bash
# local
uv run src/sagemaker_processing_flow.py --environment=pypi run

# on EKS
uv run src/sagemaker_processing_flow.py --environment=fast-bakery run --with kubernetes
```

## Limits

- **Dynamic access** (`getattr`/`setattr(self, name)`) is invisible to the AST pass — use
  `extra_inputs=[...]` / `extra_outputs=[...]`.
- **Do not import from the flow module inside a body.** `from my_flow import helper` fails with
  `ModuleNotFoundError` in the container: only the packages *beside* the flow file are shipped,
  never the flow itself. Reference the name instead — a module-level function the body references
  travels with it, cloudpickled by value. Natural to write, and it fails only remotely.
- **The body must be picklable by value** — no closures over unpicklable objects.
- **Python minor version must match** between submitting side and image.
- **`current.card`** writes are swallowed in the container.
- **Logs in the Outerbounds UI appear only at task end.** Metaflow writes them to the datastore in
  one shot (`runtime.py` → `save_logs`) and the UI reads the datastore, so a local run can never
  stream there however promptly the container writes. Your terminal does get them live, via a 5s
  CloudWatch poll.

## Next

1. **Measure Spot pricing.** Now the only argument left for Batch, and it has to overcome a 1.7×
   wall-clock disadvantage, not just an instance-price gap.
2. **Repeat the comparison.** One run each is not a result; Spot capacity and image pulls vary.
3. **Try shrinking the image.** If ~85s is mostly pull and pip, that helps *both* backends more
   than choosing between them — and it is cheaper than either.
5. **EC2 compute environment** if anything needs GPU or more than 16 vCPU / 120 GB.
