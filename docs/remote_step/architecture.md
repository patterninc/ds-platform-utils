# `@remote_step` — architecture

How a Metaflow step's compute is moved off Outerbounds and onto Pattern's own
EKS cluster, and why.

Companion documents:
- [eks.md](eks.md) — the cluster itself: network, nodes, queueing, identity
- [gaps.md](gaps.md) — what is not implemented yet, per-feature status
- [security_review.md](security_review.md) — the auth chain and its findings

---

## 1. Intent

Outerbounds bills OBC per Metaflow task per minute, tiered by the resources
the task *requests*:

| Tier | CPU | Memory | GPU | OBC / min |
|---|---|---|---|---|
| Local | — | — | — | 0.1 |
| Small | 1–3 | 1–15 GB | 0 | 1 |
| Medium | 4–7 | 16–31 GB | 1 | 6 |
| Large | 8+ | 32+ GB | 2+ | 10 |

The tier is set by the ask, not by usage. A step declaring
`@resources(cpu=20, memory=29000)` is billed at Large for its whole runtime
whether it saturates those cores or idles.

`@remote_step` splits such a step in two. A **thin driver** stays on
Outerbounds, requests 2 vCPU / 8 GB — Small tier — and does nothing but
submit, poll and stream logs. The **step body** runs in a pod on our EKS
cluster, where the cost is EC2 on-demand billed directly by AWS and the
instance is chosen per-step by Karpenter.

So a long Large-tier step becomes a long Small-tier driver plus an EC2
instance sized to the actual ask. The DAG, `self.<attr>` semantics, `@retry`,
`@timeout` and artifact lineage are all preserved — swapping `@kubernetes` for
`@remote_step(team=...)` is the only source change.

**What this does not do.** It does not reduce the compute needed; it changes
who bills for it and how precisely it is sized. A step that genuinely needs
20 cores for an hour still needs that. The saving is the tier multiplier on
the Metaflow task, plus Karpenter picking the cheapest instance that fits
rather than a tier bucket.

---

## 2. The split

```
┌─ Outerbounds ─────────────────┐        ┌─ our EKS (209479263910) ────────┐
│                               │        │                                 │
│  driver task                  │        │  runner pod                     │
│    2 vCPU / 8 GB, Small tier  │        │    the step's real @resources    │
│    - collects self.<attr>     │        │    - unpickles inputs           │
│    - uploads spec + code      │──────▶ │    - runs the user's step body  │
│    - creates the k8s Job      │        │    - pickles outputs to S3      │
│    - polls, streams the log   │ ◀───── │    - writes output-manifest     │
│    - reads the manifest       │        │                                 │
│    - assigns refs back        │        │  Kueue admits, Karpenter places  │
└───────────────────────────────┘        └─────────────────────────────────┘
                │                                          │
                └──────────  s3://pattern-ml-platform  ────┘
                             spec, code, inputs, outputs
```

The driver never materialises a payload. It holds references, not data — see
§6.

---

## 3. Lifecycle of one step

`step_init`, at flow initialisation:

1. Reject unsupported combinations (`@parallel`, `@remote_step` written by
   hand on `start`/`end`), and resolve the team from the decorator or the
   run's `ds.domain:` tag (§8).
2. Resolve the resource ask from `@resources` **and** `@kubernetes`, taking
   the max of each dimension.
3. Resolve the pypi environment from `@pypi_base`/`@pypi`, caching it to
   `.remote_step_env.json` because Metaflow blanks those attributes at
   task-run time.
4. Decide whether this step submits at all (§8).
5. If submitting: drop any sibling `@kubernetes`, inject a driver-sized one
   inheriting its placement, shrink `@resources` so Metaflow renders a Small
   pod, and inject `@secrets` for the GitHub token.

`task_decorate` replaces the step body with the driver body. At execution:

6. Acquire cluster credentials (§4) — **before** any S3 call, because the
   ambient Outerbounds identity cannot write our bucket.
7. Collect every `self.<attr>` as the step's inputs.
8. Build and upload `spec.json` plus a code tarball.
9. Create a suspended `batch/v1` Job labelled for Kueue.
10. Poll to completion, streaming the pod log through to stdout.
11. Read `output-manifest.json`, assign `RemoteArtifact` refs back onto
    `self`, and replay `self.next(...)` preserving the transition shape
    (linear, split, split-switch, foreach).

Metaflow then persists those small refs as ordinary artifacts.

---

## 4. Authentication

Two independent planes. IAM gets a caller **to** the API server; EKS access
entries decide what it may **do** once there. Broad IAM with no access entry
still yields a 403.

```
identity source
  ├─ in a pod        OIDC task role (obp-*-task), ambient
  └─ on a laptop     no AWS credentials needed — Outerbounds' own client
                     provider vends the same obp-*-task role via
                     AssumeRoleWithWebIdentity, valid 24h, self-refreshing
        │
        ├─(1) sts:AssumeRole ─────▶ pattern-ml-platform-ob-submitter
        │        trust requires AssumeRole + TagSession + SetSourceIdentity
        │        wrapped in RefreshableCredentials (role chaining caps at 1h)
        │
        ├─(2) presign sts:GetCallerIdentity with x-k8s-aws-id in the
        │     signature ─▶ base64url, no padding, "k8s-aws-v1." prefix
        │                  = the EKS bearer token, capped at 15 min,
        │                    regenerated every 10
        │
        └─(3) POST /apis/batch/v1/namespaces/<team>/jobs
```

Hop 1 is conditional. `_has_access_entry()` checks whether the current
identity already has one — an operator's SSO role does, an `obp-*-task` role
does not — and skips the hop when unnecessary. The check matches on role
*name*, because an SSO role's access entry carries an
`/aws-reserved/sso.amazonaws.com/` path that the STS assumed-role ARN does not
expose.

`sts:SetSourceIdentity` is required, not optional: Outerbounds federates its
task role with a source identity set, and without that third action the hop
fails with a message that reads like a missing `AssumeRole` grant.

**Three roles, disjoint permissions:**

| role | who assumes it | can |
|---|---|---|
| `…-ob-submitter` | Outerbounds task roles | `eks:DescribeCluster`, payload bucket RW, CloudWatch read + `StartLiveTail`; `AmazonEKSEditPolicy` scoped to team namespaces |
| `…-ob-runner` | the runner pod, via Pod Identity | payload bucket RW, CloudWatch write. No Kubernetes API access |
| `…-ob-artifact-reader` | Outerbounds task roles | payload bucket read-only, for `RemoteArtifact.load()` from a non-`@remote_step` step whose own role is denied cross-account `s3:GetObject` |

The runner's identity comes from `aws_eks_pod_identity_association` binding
the `remote-step-runner` ServiceAccount in each team namespace. No static
credentials exist anywhere in the chain.

---

## 5. S3 layout

`keys.py` is the sole owner of this schema.

```
s3://pattern-ml-platform/
  <submitter>/<perimeter>/<flow>/<run_id>/
    code/<uuid8>/code.tgz
    specs/<task_id>/<attempt>/spec.json
    inputs/<task_id>/<attempt>/<name>.pkl
    outputs/<task_id>/<attempt>/<name>.pkl
    outputs/<task_id>/<attempt>/output-manifest.json
```

Broadest to narrowest left to right, so each level is independently useful for
`aws s3 ls` or a lifecycle rule. `<submitter>` names the control plane (today
always `outerbounds`) so a second submitter cannot interleave run ids.
`<perimeter>` matters because run ids are only unique within one.
`<task_id>/<attempt>` means a retry writes a fresh attempt instead of
overwriting, leaving the failed one readable for a post-mortem.

The bucket has all four public-access blocks on, no bucket policy, SSE-S3
encryption, versioning disabled, and a rule aborting incomplete multipart
uploads after a day. Anonymous access returns 403 — verified.

An S3 **gateway endpoint** on the private route tables keeps this traffic off
NAT. That is a cost control, not an access control: steps ship GB-scale
pickles and NAT bills per GB.

---

## 6. Artifacts

Outputs are pickled to S3 by the runner and returned to the driver as
`RemoteArtifact` references — kind, size and URI, nothing more. Metaflow
persists the reference.

A downstream `@remote_step` receives the reference in its spec and the runner
fetches the payload directly, so a large intermediate never transits the
driver. That is the zero-copy path.

A downstream **non**-`@remote_step` step calls `.load()`, which assumes the
artifact-reader role and pulls the object. Code that indexes or calls an
artifact directly works because `RemoteArtifact` proxies those operations.

The runner distinguishes outputs from inputs by identity, snapshotting `id()`
of every attribute *before* the body runs. In-place rebinding —
`self.df = transform(self.df)` — is therefore detected as an output.

---

## 7. Queueing and placement

**Kueue** gates admission. The Job is created `suspend: true` with
`kueue.x-k8s.io/queue-name` set; Kueue's webhook unsuspends it once the team's
ClusterQueue has quota. Omitting that label is a correctness bug, not a
missing nicety — Kueue ignores the Job and it runs unqueued, bypassing quota.

Each team gets a namespace, ClusterQueue and LocalQueue. Quotas are declared
per team and borrowable from a shared cohort when others are idle. **Values in
`var.teams` are per flavor** and additive: `cpu_quota=256` grants 256 x86
cores *and* 256 arm64 cores.

ResourceFlavors: `x86-ondemand`, `arm64-ondemand`, `gpu`. Their `nodeLabels`
are stamped onto the admitted pod, which is what steers it to the right
NodePool. Every requested resource must be covered by a flavor —
`ephemeral-storage` included, or admission fails with "couldn't assign flavors
to pod set main".

WorkloadPriorityClasses `low` / `normal` / `high` affect preemption only
within a team's own ClusterQueue, never across teams.

**Karpenter** then provisions a node. Three NodePools, current generation
only, no older-generation fallback:

| pool | families |
|---|---|
| `x86-ondemand` | c8i m8i r8i c8a m8a r8a x8i |
| `arm64-ondemand` | c9g m9g r9g x8g |
| `gpu` | g6e g6 p6-b200 |

The family list is a candidate **set**, not a priority order — Karpenter picks
the cheapest instance satisfying the request. To force a family, put a
nodeSelector on `karpenter.k8s.aws/instance-family`. The x- families exist
because our steps ask in roughly a 16:1 memory-to-core ratio; without them
Karpenter overshoots cores to reach the memory target.

No spot capacity anywhere: there is no SQS interruption queue, so a reclaim
would hard-kill a running step with no graceful drain.

Nodes run Bottlerocket, expire after 24h, and consolidate `WhenEmpty` after
2 minutes. Bottlerocket withholds IMDS from pods — which is why the EBS CSI
driver needs its own Pod Identity role, and why the NVIDIA device plugin must
not be installed separately (its NVIDIA variant ships one).

---

## 8. Execution modes

The decorator asks *what am I about to do*, not *where am I running*.

Two independent dimensions: **which** steps go to EKS, and **where** Metaflow
puts everything else.

The second is not ours — it is whatever the invocation would do without this
decorator at all. And because the driver is itself an ordinary Metaflow task,
it lands in that same place. So one column determines two.

| invocation | steps on EKS | every other task, driver included |
|---|---|---|
| `run` | those decorated | local process |
| `run --with kubernetes` | those decorated | Outerbounds pod |
| `argo-workflows create` + `trigger` | those decorated | Argo pod |
| `run --with remote_step:team=X` | all but `start`/`end` | local process |
| `run --with remote_step:team=X --with kubernetes` | all but `start`/`end` | Outerbounds pod |
| `argo-workflows create --with remote_step:team=X` | all but `start`/`end` | Argo pod |
| `run --with local_step` | none | local process |
| `run --with local_step --with kubernetes` | none | Outerbounds pod |

"those decorated" means the steps carrying `@remote_step` in the flow source.

Note `--with kubernetes` covers **every** step, `start` and `end` included —
Metaflow's `_attach_decorators` has no exclusion for them, so an undecorated
step runs in an Outerbounds pod rather than locally. Only a `--with
remote_step` sweep skips those two, and only because `@remote_step` itself
refuses them.

`--with local_step` wins over everything, including a `--with remote_step`
sweep in the same command: every step goes inert and runs wherever the right
column says.

### Applying it to a whole flow

`--with remote_step:team=<team>` offloads **every** step without decorating
any of them, the same way `--with kubernetes` works:

```bash
run --with remote_step:team=forecasting
argo-workflows create --with remote_step:team=forecasting
```

`start` and `end` are skipped. Hand-writing `@remote_step` on those is still
an error — Metaflow's scheduler owns them and there is nothing to offload —
but a `--with` sweep cannot avoid touching them, so refusing would make the
flag unusable. `_attached_via_with()` tells the two cases apart by looking for
a `--with` spec naming `remote_step` in `sys.argv`.

Each step keeps its own `@resources`, so this is not one blanket size.

This survives into Argo. `argo_workflows.py` re-emits every
non-statically-defined decorator as `--with <spec>` in each step's baked
command, so the pod re-attaches `@remote_step` and reaches the same conclusion
the laptop did. Without that propagation the failure would be quiet and bad:
step bodies running inside Argo pods at driver size, so a 63 GB step would OOM
in 8 GB.

### Where `team` comes from

Resolution order:

1. `team=` on the decorator
2. `--tag ds.domain:<team>` on the run

So `@remote_step()` and `--with remote_step` both work when the run is tagged,
which for flows already tagged by domain removes the same string from every
decorator. The trade-off is that the flow stops being self-contained: without
the tag it fails at flow init, clearly and naming both fixes, but it does fail.
`@remote_step(team=...)` in source always works regardless of how it is
invoked.

Read from `sys.argv` rather than `metaflow.current.tags` because the
requirement is validated at flow init, before `current` is populated — and
because Argo re-emits run tags into each step's command too, so argv agrees in
the pod. Two `ds.domain:` tags naming different teams is an error rather than
a coin flip. The resolved team is checked against the cluster's known teams, so
a typo fails at flow init instead of surfacing as a namespace error mid-run.

**This is not an authorisation check.** A tag is as user-supplied as `team=`;
see finding 1 in [security_review.md](security_review.md).

### Opting out

`--with local_step` is a no-op marker decorator that makes `@remote_step`
inert: siblings are left untouched and the step function is returned
unwrapped, so Metaflow does whatever it would have done anyway. Nothing is
submitted and no quota is consumed. Every affected step says so loudly at
flow init, because a body that ran in the driver's environment rather than the
runner container proves nothing about production.

A marker rather than an attribute because Metaflow *silently ignores* a
`--with` decorator whose name is already on the step. A marker rather than an
environment variable because `--with` travels in `top_level_options` and so
reaches the command built for a remote step, where an env var would not.

---

## 9. The runner container

One multi-arch image (`linux/amd64` + `linux/arm64`) in ECR serves every team;
the pod's `nodeSelector` decides which variant is pulled. The Job sets no
`command`, preserving the image's `tini` entrypoint — a pod-spec `command`
*replaces* the entrypoint rather than appending, which would make Python PID 1
and break signal handling.

Stages, each logged with timing so a failure names its phase:

```
fetch_payload → uv_venv → uv_pip_install → install_runner → fetch_code_pkg
  → load_spec → hydrate_inputs → import_step → user_step → persist_outputs
  → write_manifest
```

Exit codes distinguish setup failure (3, 4) from user-code failure (5), which
is what lets the driver classify the error rather than reporting every failure
identically.

The environment is built per-step by `uv` from the spec's package set, so the
runner matches the flow's declared pypi environment rather than whatever the
image happens to contain. Private git dependencies need `GITHUB_TOKEN`, which
the driver forwards from `@secrets`; Outerbounds integration settings
(`OBP_INTEGRATIONS_URL` and friends) are read from the resolved Metaflow
config and forwarded too, since on a laptop they exist only there.

Job manifest properties worth knowing:

- `suspend: true` — Kueue unsuspends on admission
- `backoffLimit: 0` — retries are Metaflow's job, not the Job controller's;
  a Job-level retry would re-run the body without the driver knowing and write
  to the same output prefix
- `ttlSecondsAfterFinished: 86400` — a failure is inspectable the next morning
- `activeDeadlineSeconds` — a real ceiling, so a hung step cannot hang the
  driver indefinitely
- memory limit equals its request, so a runaway step is OOM-killed rather than
  evicting neighbours; CPU is deliberately unlimited, since throttling a step
  that briefly wants more cores makes it slower for no benefit

---

## 10. Failure model

Every failure is one of a typed set carrying `.retriable`, which Metaflow's
`@retry` honours.

| error | retriable | trigger |
|---|---|---|
| `SizingError` | no | ask no NodePool can satisfy, or an unsupported decorator combination |
| `ConfigError` | no | `config.json` missing or malformed |
| `SubmitError` | no | the API server refused the Job |
| `PendingTimeoutError` | yes | never admitted or never scheduled within the timeout |
| `RunnerError` | yes | container exited non-zero |
| `NodeLostError` | yes | node disappeared under a running pod |
| `ManifestMissingError` | yes | Job succeeded but wrote no manifest |
| `ManifestReferencesMissingError` | yes | manifest names objects that are absent |
| `ArtifactLoadError` | no | a `RemoteArtifact` could not be fetched |
| `KilledByUser` | no | Ctrl-C — the intent is to stop |

The driver traps SIGINT **and** SIGTERM and deletes the Job on the way out, so
an interrupted run releases its Kueue quota instead of orphaning it. It
tolerates transient API errors rather than failing on the first one, and
handles a vanished Job distinctly from an unreachable API.

---

## 11. Infrastructure

Terraform in [`infra/eks/`](../../infra/eks). Remote state in S3.

| component | detail |
|---|---|
| cluster | `pattern-ml-platform`, Kubernetes 1.35, `authentication_mode = "API"` |
| VPC | `10.42.0.0/16`, 3 AZs, private subnets, single NAT, S3 gateway endpoint |
| system node group | 2–4 × m8i.xlarge Bottlerocket, x86, carries Karpenter/CoreDNS/Kueue |
| autoscaling | Karpenter with Pod Identity, three NodePools |
| queueing | Kueue, one ClusterQueue per team in a shared cohort |
| registry | ECR, mutable tags so `:latest` can be re-pointed; production pins a timestamped tag |
| payload bucket | `pattern-ml-platform`, private, SSE-S3 |
| logs | CloudWatch log group `/pattern-ml-platform/steps`, 30-day retention |
| metrics | metrics-server for `kubectl top`. Karpenter and Kueue expose Prometheus endpoints but **nothing scrapes them** |

The API endpoint is public with a CIDR allow-list. Private-only was
investigated and rejected as disproportionate — see the rationale on
`var.enable_public_endpoint`.

Teams are `advertising`, `content`, `forecasting`, `market-intelligence`,
`nlp`, `operations`. Adding one is a change to `var.teams`, not new Terraform.

---

## 12. Observability

The driver streams the runner pod's log into its own stdout, so a step's
output appears in the Metaflow UI as though it ran locally. Reconnects use
`sinceSeconds` so a dropped idle connection does not replay the whole log.

`_MflogPusher` forces a `save_logs` every few seconds, because Metaflow's own
sidecar backs off to a ~30s cadence for long tasks and the UI reads from those
uploads.

Driver tasks record `kubernetes-pod-name` / `-pod-id` / `-node-ip` as task
metadata, which is what lets Outerbounds join its per-task CPU/memory panel to
cluster metrics.

**Not implemented:** log retention for the runner pod itself. `logs.tf`
provisions a CloudWatch group but nothing ships to it, so once a pod is reaped
its log is unrecoverable beyond what the driver streamed. Likewise no metrics
scraper — Karpenter and Kueue metrics are live-only and reset on pod restart.

---

## 13. Known gaps

Feature-by-feature status is in [gaps.md](gaps.md); auth and IAM
findings are in [security_review.md](security_review.md). The ones most likely
to surprise:

- `team=` is an unauthenticated claim — any flow author can name any team's
  namespace and quota.
- Credentials reach the runner as plaintext env in the Job spec.
- `current.is_production`, `current.card`, `current.model`, join-step
  signatures and `self.input` in foreach children are not yet forwarded to the
  runner.
- `@timeout` applies to the driver; the Job's deadline is a separate
  `@remote_step` attribute, so the two can disagree.
