# The EKS cluster

`pattern-ml-platform` — the compute plane `@remote_step` submits to. This
document covers the infrastructure; [architecture.md](architecture.md) covers
how the decorator uses it.

Terraform: [`infra/eks/`](../../infra/eks), remote state in S3
(`pattern-ml-platform-tfstate-209479263910`, key `eks/terraform.tfstate`,
locked via a lockfile object rather than DynamoDB).

Account **209479263910**, region **us-west-2**. Outerbounds operates its own
VPC and cluster in the same account — `obp-301bcf` and `obp-301bcf-main` — and
neither is ours to touch.

---

## 1. Why a cluster at all

Outerbounds bills per Metaflow task-minute on the *requested* tier. Running a
20-core step there costs the Large-tier rate for its whole duration. Running
it here costs one EC2 instance sized to the request, chosen per step, billed
by AWS.

That trade only works if the cluster is cheap when idle. Hence: two small
always-on nodes, everything else provisioned per step by Karpenter and
released within minutes.

---

## 2. Network

```
VPC  vpc-01600c42db6aa4217   10.42.0.0/16   3 AZs
  private  10.42.0.0/20, 10.42.16.0/20, 10.42.32.0/20   (nodes, API ENIs)
  public   10.42.48.0/24, 10.42.49.0/24, 10.42.50.0/24  (NAT)
  1 × NAT gateway
  S3 gateway endpoint on the private route tables
```

Nodes live in private subnets. One NAT, not three: Karpenter spreads nodes
across AZs for capacity, not availability — losing a node mid-step already
costs the step — so paying triple for zonal NAT redundancy buys nothing.

The **S3 gateway endpoint** is a cost control. Steps ship GB-scale pickles and
NAT bills per GB processed; the endpoint is free and keeps that traffic out of
NAT entirely. It only works because the private route tables are associated
with it — the endpoint alone diverts nothing.

`10.42.0.0/16` was **chosen, not assigned by IPAM**. It is absent from the
per-`/16` list the Client VPN routes, so it collides with nothing currently
reachable, but it is not an allocation. Renumbering later means rebuilding the
VPC and the cluster with it.

---

## 3. Control plane

Kubernetes **1.35**, `authentication_mode = "API"` — no legacy `aws-auth`
ConfigMap; EKS access entries are the only authorisation path.

Endpoint is **public with a CIDR allow-list**, and private access is also
enabled so in-VPC clients reach the API over the private ENIs instead of
hairpinning through NAT.

> `public_endpoint_allowed_cidrs` is still `0.0.0.0/0`. Narrowing it to
> obp-301bcf's NAT EIP plus operator egress is the single outstanding
> hardening item that depends on nobody else — see
> [security_review.md](security_review.md) finding 3.

Private-only was investigated and rejected. It needs a Client VPN route for
our CIDR (the tunnel carries an explicit per-`/16` allow-list), a Transit
Gateway attachment plus association and propagation in the hub account
`922016401078`, and a way to resolve the endpoint — the private hosted zone
EKS creates is owned by `eks.amazonaws.com` and cannot take extra VPC
associations, and the VPN pushes no resolver. All achievable, but a large
dependency chain for a cluster whose only human traffic is debugging. The
rationale lives on `var.enable_public_endpoint`.

**Addons:** CoreDNS, kube-proxy, VPC CNI, EKS Pod Identity Agent, EBS CSI
driver.

VPC CNI runs with **prefix delegation** (`ENABLE_PREFIX_DELEGATION=true`,
`WARM_PREFIX_TARGET=1`), which lifts the per-node pod ceiling from the ENI
limit to `/28` blocks. Without it a large instance is capped at a handful of
pods by its network interfaces.

The EBS CSI controller has its **own Pod Identity role**. This is not
optional: it calls EC2 directly to create and attach volumes, and Bottlerocket
withholds IMDS from pods, so with no role it has no credentials and no
fallback. The failure mode is `ebs-plugin` failing its startup health check
with `no EC2 IMDS role found`, which crashloops the container and hangs the
addon in `CREATING` until Terraform times out.

### A required security group rule

```hcl
node_security_group_additional_rules = {
  kueue_visibility = { protocol = "tcp", from_port = 8082, to_port = 8082, ... }
}
```

Kueue's visibility extension API server listens on 8082. The EKS module's
built-in rules cover the usual webhook ports — 443, 4443, 6443, 8443, 9443,
10250, 10251 — but not this one.

Losing it is far worse than losing the visibility API. The namespace
controller must enumerate every API group before it can finalise a delete, so
one unreachable APIService breaks discovery and hangs **every** namespace
deletion cluster-wide:

```
NamespaceDeletionDiscoveryFailure  True  DiscoveryFailed
  visibility.kueue.x-k8s.io/v1beta2: stale GroupVersion discovery
```

---

## 4. Nodes

### System node group

2–4 × `m8i.xlarge`, Bottlerocket x86, labelled
`pattern-ml-platform.pattern.com/role: system`. Carries Karpenter, CoreDNS,
the Kueue controller, metrics-server and the EBS CSI controller.

**Two nodes minimum, not one:** Karpenter cannot provision the node that runs
Karpenter, so a single system node is an unrecoverable failure.

**`xlarge`, not `large`:** DaemonSets (~145m) + Karpenter (1000m) + CoreDNS
(100m) + Kueue (500m) + EBS CSI (~50m) is ~1795m against a `large`'s ~1800m
allocatable — full, with nothing left for a new addon. `xlarge` lands the same
set near 45%.

**x86, not Graviton**, despite the price difference: keeping the system plane
single-architecture avoids requiring every DaemonSet to stay multi-arch.
Graviton is a per-step opt-in on the workload pools instead. `m7i` is listed
second purely as a capacity fallback.

The node group's name is fixed (`eks-system-node`) rather than the module's
`system-<random>`. The random suffix exists so `create_before_destroy` can
stand two groups up at once; with a fixed name, any replacement-forcing change
fails because AWS rejects the duplicate. Routine changes — instance types,
scaling — are in-place, so this is fine; a ForceNew change needs `name`
changed in the same commit.

### Karpenter

Chart **1.14.1** (an LTS release), two replicas spread across the system nodes
by a topology constraint, credentials via Pod Identity rather than IRSA so the
role survives a cluster rebuild.

Three NodePools, **current generation only, no older-generation fallback**:

| pool | families | notes |
|---|---|---|
| `x86-ondemand` | c8i m8i r8i c8a m8a r8a x8i | gen 8 is newest in us-west-2 |
| `arm64-ondemand` | c9g m9g r9g x8g | Graviton ships ahead of x86 |
| `gpu` | g6e g6 p6-b200 | tainted `nvidia.com/gpu=true:NoSchedule` |

Sizes `nano`/`micro`/`small`/`medium` are excluded — they cannot hold the
runner image plus a multi-GB pickle. Each pool is limited to 2000 CPU and
8000 GiB, which is the real ceiling on spend.

The family list is a candidate **set, not a priority order**. Karpenter picks
the cheapest instance satisfying the request, so listing more families widens
the search and lowers cost; it never expresses preference. To force one, put a
nodeSelector on `karpenter.k8s.aws/instance-family`.

The `x`-families matter: our steps ask in roughly a 16:1 memory-to-core ratio,
and without an x-family option Karpenter overshoots cores to reach the memory
target and we pay for cores the step never uses.

The GPU taint exists because a CPU step scheduled onto a `g6e` would burn
several dollars an hour for nothing. Kueue's `gpu` flavor carries the matching
toleration, so only pods that actually requested a GPU clear it.

**No spot capacity anywhere.** There is no SQS interruption queue, so a
reclaim would hard-kill a running step with no graceful drain. Revisit when
steps checkpoint.

**The trade-off of no fallback:** if every listed family is capacity-short in
every reachable AZ, Karpenter cannot launch and the pod stays Pending until
Kueue's `waitForPodsReady` timeout evicts and requeues it. Low risk for gen
8/9 x86 and Graviton; materially higher for GPU.

Nodes `expireAfter: 24h`, consolidate `WhenEmpty` after 2 minutes, and get a
5-minute termination grace period.

### EC2NodeClass

`bottlerocket@latest` — the alias resolves the NVIDIA variant automatically
when the selected instance has a GPU, so one alias covers both classes.

Bottlerocket rather than AL2023: immutable read-only root, no package manager,
no SSH, atomic two-partition updates with rollback, declarative TOML config
instead of a userData shell script. IMDS is `httpTokens: required`.

The volume split is the part that matters:

| device | role | CPU pool | GPU pool |
|---|---|---|---|
| `/dev/xvda` | OS, immutable | 10 GiB gp3 | 10 GiB gp3 |
| `/dev/xvdb` | images, writable layers, ephemeral | 200 GiB gp3, 6000 IOPS, 500 MB/s | 500 GiB gp3, 10000 IOPS, 1000 MB/s |

`xvdb` holds the runner image, the per-step `uv` venv and any pickle that
spills to disk, so it gets the space and the throughput. gp3's baseline is
3000 IOPS / 125 MB/s, which is well short of what streaming GB-scale artifacts
wants.

**Do not install a separate NVIDIA device plugin.** Bottlerocket's NVIDIA
variant ships one; a second copy crashloops with
`Incompatible strategy detected auto`, because it expects a conventional
container-toolkit layout that a read-only OS does not present.

---

## 5. Queueing

Kueue **0.19.3**, controller pinned to the system nodes for the same reason as
Karpenter: the thing that admits workloads should not depend on a node a
workload-driven autoscaler might reclaim.

> Kueue is pre-1.0 and its API shifts between minors. Treat an upgrade as a
> change that needs the ClusterQueue and ResourceFlavor manifests re-read, not
> a routine bump. The chart's `controllerManagerConfigYaml` **replaces** the
> defaults rather than merging, so everything the default sets has to be
> restated — dropping `health` alone crashloops the controller by removing the
> probes' bind address.

### Flavors

| flavor | node labels | notes |
|---|---|---|
| `x86-ondemand` | pool + `kubernetes.io/arch: amd64` | |
| `arm64-ondemand` | pool + `kubernetes.io/arch: arm64` | |
| `gpu` | pool `gpu` | carries the `NoSchedule` toleration |

A flavor's `nodeLabels` are **stamped onto the admitted pod**, which is how a
Workload ends up on the right NodePool. Every requested resource must be
covered by some flavor — `ephemeral-storage` included, or admission fails with
`couldn't assign flavors to pod set main`.

### Per-team queues

One Namespace, ClusterQueue and LocalQueue (`default`) per entry in
`var.teams`, plus the `remote-step-runner` ServiceAccount. Adding a team is a
variable change, not new Terraform.

All ClusterQueues share the cohort `pattern-ml-platform`, so an idle team's
nominal quota is borrowable. `preemption.reclaimWithinCohort: Any` means a
team under its own nominal quota can reclaim what another borrowed —
borrowing is opportunistic, not a transfer.

| team | cpu (nominal/borrow) | memory | gpu |
|---|---|---|---|
| forecasting | 256 / 512 | 1024Gi / 2048Gi | 0 / 4 |
| content | 256 / 512 | 1024Gi / 2048Gi | 8 / 16 |
| market-intelligence | 256 / 512 | 1024Gi / 2048Gi | 0 / 4 |
| advertising | 128 / 512 | 512Gi / 2048Gi | 0 / 4 |
| operations | 128 / 512 | 512Gi / 2048Gi | 0 / 4 |
| revops | 128 / 512 | 512Gi / 2048Gi | 0 / 4 |
| reference | 128 / 512 | 512Gi / 2048Gi | 0 / 4 |
| demand-generation | 128 / 512 | 512Gi / 2048Gi | 0 / 4 |
| sandbox | 32 / 64 | 128Gi / 256Gi | 0 / 0 |

`content` carries NLP's work, which is why it holds the cluster's only GPU
quota. `sandbox` is where a step lands when nothing names a team — small on
purpose, and with no GPU so an untagged run cannot take a GPU node.

> **These are per flavor and therefore additive.** `cpu_quota=256` grants 256
> x86 cores *and* 256 arm64 cores, so a team's real nominal is double what the
> table reads. Defensible as "256 of each architecture"; halve them if the
> intent is N cores regardless. The NodePool `limits` are the true ceiling
> either way.

`ephemeral-storage` is declared at 10Ti nominal and borrow on both CPU
flavors — effectively unlimited, present only because admission requires
every requested resource to be covered.

Three WorkloadPriorityClasses — `low`, `normal`, `high` — affecting preemption
**only within a team's own ClusterQueue**, never across teams.

`waitForPodsReady` is set with a 15-minute timeout and `blockAdmission:
false`, so a Workload that cannot get its pod running is evicted and requeued
rather than holding quota indefinitely. Note v1beta2 dropped the `enable` key:
the feature is active by the block's presence, and a stray `enable: true`
kills the controller at startup with a strict-decoding error.

---

## 6. Identity

| role | trusted by | grants |
|---|---|---|
| `…-ob-submitter` | the three `obp-*-task` roles | `eks:DescribeCluster`, payload bucket RW, CloudWatch read + `StartLiveTail`; `AmazonEKSEditPolicy` scoped to team namespaces |
| `…-ob-runner` | `pods.eks.amazonaws.com` | payload bucket RW, CloudWatch write |
| `…-ob-artifact-reader` | the three `obp-*-task` roles | payload bucket read-only |
| `…-karpenter-node` | EC2 | SSM + CloudWatch agent; access entry created by the submodule |
| `…-ob-ebs-csi` | `pods.eks.amazonaws.com` | `AmazonEBSCSIDriverPolicy` |

Both cross-account trust policies need **three** actions —
`sts:AssumeRole`, `sts:TagSession`, `sts:SetSourceIdentity`. The third is
required because Outerbounds federates its task role with a source identity
set; without it the hop fails with a message that reads like a missing
`AssumeRole` grant.

`var.outerbounds_task_role_arns` is a list because a pod's task role depends
on which Outerbounds perimeter the flow was deployed to:

```
obp-5p6le9-task        sub = pattern         default perimeter
obp-301bcf-task--prod  sub = pattern--prod   prod perimeter
obp-301bcf-task        (no sub condition)    any perimeter
```

There is **no `access_entries` block on the cluster module**, deliberately.
Karpenter's node role needs an `EC2_LINUX` entry but the submodule creates it
itself; declaring it again produces a dependency cycle, since the cluster
module would need `module.karpenter.node_iam_role_arn` while Karpenter needs
`module.eks.cluster_name`. The submitter's entry is a standalone resource for
the same reason — it only needs the cluster name.

The runner role is **one role associated into every team namespace**, not one
per team. Teams are separated by namespace and quota, not by blast radius on
the bucket. See [security_review.md](security_review.md) finding 5.

---

## 7. Storage, registry, logs

**Payload bucket** `pattern-ml-platform` — all four public-access blocks on,
no bucket policy, SSE-S3, versioning disabled, incomplete multipart uploads
aborted after a day. Anonymous access returns 403.

No lifecycle expiry: objects accumulate until someone sets a retention
policy. A single demand-forecast run writes on the order of 10 GB.

**ECR** `pattern-ml-platform-runner` — one multi-arch image
(`linux/amd64` + `linux/arm64`) for every team; the pod's nodeSelector decides
which variant is pulled. Tags are `MUTABLE` so `:latest` can be re-pointed
each build, with scan-on-push and AES256 encryption. Production pinning
happens at the config level via an immutable `YYYYMMDD-HHMMSS` tag rather than
by locking the repository.

**Logs** — CloudWatch group `/pattern-ml-platform/steps`, 30-day retention.

> **Nothing ships to it.** The group exists and the IAM permissions are in
> place, but no agent forwards runner-pod logs, so once a pod is reaped its log
> is unrecoverable beyond what the driver streamed live.

---

## 8. Observability

**metrics-server** 3.14.0, one replica, pinned to the system nodes, with
`--kubelet-preferred-address-types=InternalIP` — our nodes have no public
addresses and Bottlerocket registers no resolvable hostname, so the chart's
default address types would each be tried and time out first.

One replica is deliberate: an outage costs `kubectl top` and nothing else,
since no HPAs depend on it.

It answers "what is running right now" over a 2–5 minute window. It is not a
store.

> **No metrics scraper.** Karpenter serves `:8080/metrics` (nodes created and
> terminated, provisioning latency, NodeClaim lifecycle, unschedulable pod
> counts) and Kueue serves `:8443/metrics` (pending and admitted Workloads per
> ClusterQueue, admission latency, quota usage per flavor). Nothing polls
> either, so the values are live-only and reset on pod restart. Reachable by
> hand:
>
> ```bash
> kubectl -n kube-system port-forward deploy/karpenter 8080:8080
> curl -s localhost:8080/metrics | grep karpenter_nodes
> ```
>
> kube-prometheus-stack is the intended route — it brings the Operator CRDs,
> after which Karpenter's `serviceMonitor.enabled` and Kueue's
> `enablePrometheus` wire themselves up. Budget ~850m CPU and ~3Gi on the
> system nodes plus a PVC for the TSDB.

---

## 9. Operating it

```bash
aws eks update-kubeconfig --name pattern-ml-platform --region us-west-2 \
  --profile AWSAdministratorAccess-209479263910

kubectl get pods -n forecasting                  # a team's runner pods
kubectl get jobs -n forecasting
kubectl get workloads -n forecasting             # Kueue admission state
kubectl get clusterqueues                        # quota usage
kubectl get nodepools,nodeclaims                 # Karpenter
kubectl top nodes
```

A pod stuck `Pending` is nearly always one of: Kueue has not admitted it
(check `kubectl describe workload`), or Karpenter cannot find capacity (check
`kubectl describe nodeclaim` and the pod's events).

`terraform apply` needs the `aws_profile` variable, supplied through
`terraform.tfvars`, which is gitignored because it names an operator-specific
SSO profile.

---

## 10. Known state

- `public_endpoint_allowed_cidrs` is `0.0.0.0/0`.
- Team quotas are effectively doubled by the per-flavor declaration.
- `10.42.0.0/16` is not an IPAM allocation.
- No log shipping, no metrics scraper.
- No account-level S3 public-access block, and no bucket policy — so no
  TLS-only or VPC-endpoint restriction as defence in depth.
- Two pre-existing Karpenter drift items appear on every plan: an out-of-band
  `QSConfigId-wu54y` tag on the node role, and the controller policy showing
  as `(known after apply)` because the module derives it from data sources.
- GPU pods charge their CPU and memory to the `x86-ondemand` flavor, since
  the `gpu` flavor covers only `nvidia.com/gpu`.
