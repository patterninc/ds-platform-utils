# Security review — `@remote_step` on EKS

**Date:** 2026-09-07
**Scope:** the auth chain from an Outerbounds Argo pod to a Kubernetes Job on
`pattern-ml-platform` (209479263910, us-west-2), plus the IAM and cluster
configuration in `infra/eks/`.

Nothing here is known to have been exploited. This is a design review, written
so the findings survive until someone has time to act on them.

---

## Context: how auth works today

Two independent planes. IAM gets a caller **to** the API server; EKS access
entries decide what it may **do** once there. Broad IAM with no access entry
still gets a 403.

```
Argo pod (Outerbounds)
  │  OIDC-federated task role, one of three depending on perimeter:
  │    obp-5p6le9-task        sub = pattern        default
  │    obp-301bcf-task--prod  sub = pattern--prod  prod
  │    obp-301bcf-task        (no sub condition)   any
  │
  ├─(1) sts:AssumeRole ─────────> pattern-ml-platform-ob-submitter
  │       trust needs AssumeRole + TagSession + SetSourceIdentity
  │       creds wrapped in RefreshableCredentials (role chaining caps at 1h)
  │
  ├─(2) presign sts:GetCallerIdentity, x-k8s-aws-id header in the signature
  │       -> base64url, no padding, "k8s-aws-v1." prefix = EKS bearer token
  │
  └─(3) POST /apis/batch/v1/namespaces/<team>/jobs
          API server webhook resolves the token back to an IAM ARN and
          matches aws_eks_access_entry.submitter
```

The runner pod is a separate identity: `serviceAccountName:
remote-step-runner` bound by `aws_eks_pod_identity_association` to
`pattern-ml-platform-ob-runner`. It has S3 and CloudWatch access and never
touches the Kubernetes API.

A third path exists for non-`@remote_step` steps reading a `RemoteArtifact`:
they assume `pattern-ml-platform-ob-artifact-reader` (read-only on the bucket),
because Outerbounds' own pod role has a permissions boundary denying
cross-account `s3:GetObject`.

---

## Findings

| # | Finding | Severity | Fix effort |
|---|---------|----------|-----------|
| 1 | `team=` is an unauthenticated authorization claim | High | Design work |
| 2 | Credentials written as plaintext env in the Job spec | High | Contained |
| 3 | API endpoint open to `0.0.0.0/0` | Medium-High | One variable |
| 4 | A task role is trusted with no `sub` condition | Medium | Confirm + remove |
| 5 | One runner role with whole-bucket access | Medium | IAM condition |
| 6 | `AmazonEKSEditPolicy` is broader than needed | Low-Medium | Custom role |

---

### 1. `team=` is an unauthenticated authorization claim

**Evidence**
- `plugins/remote_step_decorator.py:714` — `team = self.attributes["team"]`,
  passed straight through as the Kubernetes namespace.
- `infra/eks/iam-crossaccount.tf:125` — the submitter's access policy is scoped
  to `namespaces = keys(var.teams)`, i.e. **all** of them.

**Impact**
Nothing validates that the caller is entitled to the namespace it names. Any
Outerbounds user who can run a flow can set `team="nlp"` and:

- consume NLP's Kueue quota, starving their jobs;
- run arbitrary code in NLP's namespace under NLP's runner identity;
- read and write NLP's artifacts in the payload bucket (see finding 5).

Teams are separated by convention, not by a boundary. There is no audit signal
that distinguishes this from legitimate use — the Job looks identical.

**Suggested fix**
Entitlement has to live somewhere the caller cannot set. Options, roughly in
order of effort:

1. Map Outerbounds perimeter → allowed namespaces, and reject at submit time.
   Cheap, but the check runs in the driver, which the caller controls.
2. Split the submitter role per team, each with an access entry scoped to one
   namespace, and pick the role from a trusted signal (the perimeter, or the
   Outerbounds project). Enforced by IAM rather than by our code.
3. A validating admission webhook on the cluster that compares the Job's
   namespace against a claim in the submitting identity's session tags —
   `sts:TagSession` is already in the trust policy.

Option 2 is the one that actually holds, because the enforcement point is
outside the code path the caller controls.

---

### 2. Credentials written as plaintext env in the Job spec

**Evidence**
- `plugins/remote_step_decorator.py:854-863` — `GITHUB_TOKEN` / `GIT_TOKEN` /
  `GH_TOKEN` copied from the driver env into `runner_env`.
- `plugins/remote_step_decorator.py:866-868` — **every** `METAFLOW_*`, `OBP_*`
  and `OUTERBOUNDS_*` variable copied wholesale. This includes
  `METAFLOW_SERVICE_HEADERS`, which carries the Outerbounds Metaflow service
  auth header.
- `submit.py:231-232` — `extra_env` becomes `{"name": k, "value": v}`, a
  literal value in the manifest, not a `secretKeyRef`.

**Impact**
These land in the Job and Pod specs in cleartext. Anyone who can read Jobs or
Pods in the namespace can retrieve them — and per finding 1, that is
effectively any flow author. They also persist in etcd, in `kubectl get job -o
yaml` output pasted into tickets, and in anything that scrapes the API.

The blast radius is not limited to GitHub: `METAFLOW_SERVICE_HEADERS` is a
credential for the Outerbounds control plane, which is a different trust
domain from this cluster.

The decorator does log only `len=` and never the value, which is correct — the
problem is the manifest, not the logging.

**Suggested fix**
Write the sensitive subset to a per-run Kubernetes `Secret` in the team
namespace, reference it with `secretKeyRef`, and set `ownerReferences` to the
Job so it is garbage-collected with it. Keep the non-sensitive `METAFLOW_*`
values as plain env — but allow-list them rather than forwarding by prefix, so
a future `METAFLOW_*` secret does not silently join the manifest.

---

### 3. API endpoint open to `0.0.0.0/0`

**Evidence**
- `infra/eks/variables.tf` — `public_endpoint_allowed_cidrs` defaults to
  `["0.0.0.0/0"]` and has not been narrowed.

**Impact**
The Kubernetes API server accepts connections from any address on the
internet. IAM and access entries still gate authorisation, so this is not
anonymous access — but there is no network-layer control at all. The endpoint
is publicly scannable, any authentication-path CVE becomes remotely reachable,
and a leaked credential is usable immediately from anywhere.

For context, this matches the rest of Pattern — all eight data-platform
clusters are public and none narrows its CIDR list. That makes it normal, not
safe.

**Suggested fix**
Two entries:

```
35.82.100.167/32   obp-301bcf's NAT gateway EIP — where driver pods egress
                   from. Omitting it breaks every submission.
<operator egress>  whoever runs terraform apply and kubectl.
```

Both must be **public** addresses. Pattern's Client VPN is split tunnel, so
traffic to a public endpoint leaves via the operator's own ISP — the VPN client
pool (`10.200.0.0/16`) is never the source address the API server sees. A
stable corporate egress range is preferable to a dynamic ISP address.

Note the default is deliberately wide: getting this wrong locks Terraform out
mid-apply, since the kubernetes and helm providers then cannot reach the
cluster, and recovery is an out-of-band console change.

---

### 4. A task role is trusted with no `sub` condition

**Evidence**
- `infra/eks/variables.tf`, `outerbounds_task_role_arns` — `obp-301bcf-task`
  is documented as having no `sub` condition, i.e. any perimeter.

**Impact**
Any workload in any Outerbounds perimeter running as that role can assume the
submitter role and create Jobs in all team namespaces. If a lower-trust
perimeter is ever added, it inherits that access silently.

**Suggested fix**
Confirm with Outerbounds whether that role is actually used, or whether the two
`sub`-scoped roles (`obp-5p6le9-task`, `obp-301bcf-task--prod`) cover every
perimeter a flow can deploy to. If they do, drop the unscoped one.

---

### 5. One runner role with whole-bucket access

**Evidence**
- `infra/eks/pod-identity.tf:37-50` — `PayloadBucketReadWrite` grants
  read/write on `bucket` and `bucket/*` with no prefix condition.
- The file's own comment says isolation comes from "run-scoped prefixes" —
  nothing enforces that.

**Impact**
A step in any team can read or overwrite any other team's artifacts. Because
runners deserialize pickles, overwriting another team's artifact is remote code
execution in that team's namespace on their next run — a stored attack, not
just a confidentiality issue.

**Suggested fix**
Add an `s3:prefix` / resource-ARN condition keyed on the submitter/perimeter
segment of the key layout (`keys.py` owns that schema). This does not require
per-team roles; it requires the prefix to be derived from something the step
cannot choose freely.

---

### 6. `AmazonEKSEditPolicy` is broader than the driver needs

**Evidence**
- `infra/eks/iam-crossaccount.tf:121` — `AmazonEKSEditPolicy`.
- `k8s.py` — the client implements exactly: `create_job`, `get_job`,
  `delete_job`, `list_job_pods`, `stream_pod_log`, `read_pod_log`,
  `list_events_for`, `get_node`, `server_version`.

**Impact**
`EKSEditPolicy` derives from the built-in `edit` ClusterRole, which
additionally allows **reading Secrets** and **`pods/exec`** in the scoped
namespaces. Neither is used. Combined with finding 2, secret-read is the
mechanism by which a cross-team caller retrieves forwarded credentials.

**Suggested fix**
A custom ClusterRole with just the verbs above, associated in place of the
managed policy.

Side note found while reviewing: `get_node` is cluster-scoped, but the access
policy is namespace-scoped — so that call is likely 403-ing and being swallowed
by `poll.py`'s error tolerance. Worth confirming; it only affects the instance
type shown in log output.

---

## Accepted design trade-offs

Not defects, but they should be stated rather than assumed.

- **Pickle deserialization is RCE by construction.** Whoever can write the
  payload prefix gets code execution in the runner. That is the trust model.
  Finding 5 matters because it widens who "whoever" is.
- **No NetworkPolicies.** Runner pods have unrestricted egress via NAT, which
  is a viable exfiltration path. IMDS is blocked — Bottlerocket withholds it
  from pods — which is the more important half.
- **Bucket versioning disabled** (`s3.tf`). No recovery from deletion or
  tampering. Deliberate, given artifact volume.
- **ECR tags are `MUTABLE`** so `:latest` can be re-pointed each build.
  Production config pins an immutable timestamped tag, but nothing enforces
  that a flow does so.

---

## Suggested order

1. **Finding 3** — one variable, no coordination, closes the open network
   surface. Needs a decision on the operator egress range.
2. **Finding 2** — contained change in the decorator plus a Secret in the Job
   manifest. Highest credential exposure per unit of effort.
3. **Finding 4** — a question to Outerbounds, then possibly a one-line removal.
4. **Finding 6** — mechanical, reduces the blast radius of 1 and 2.
5. **Finding 5** — needs the key layout and the IAM condition designed together.
6. **Finding 1** — the most serious, but needs a decision about where
   entitlement lives before any code changes.
