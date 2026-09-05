# Per-team Kueue objects. Terraform renders one copy of this per entry in
# var.teams (see kueue.tf).
#
# Shape per team:
#   Namespace     <team>               where the team's Jobs land
#   ClusterQueue  <team>               the quota pool
#   LocalQueue    default              namespace-scoped pointer at the pool
#
# Every ClusterQueue joins the same cohort ("pattern-ml-platform"), which is what
# lets an idle team's nominal quota be borrowed by a busy one. borrowingLimit
# caps how far over its own nominal a team can reach, so one runaway flow
# can't starve everyone else.
#
# preemption.reclaimWithinCohort=Any means a team that is under its own
# nominal quota can reclaim capacity that another team borrowed — borrowing
# is opportunistic, never a land grab.

---
apiVersion: v1
kind: Namespace
metadata:
  name: ${team}
  labels:
    pattern-ml-platform.pattern.com/team: ${team}

---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: ${team}
spec:
  namespaceSelector:
    matchLabels:
      pattern-ml-platform.pattern.com/team: ${team}
  cohortName: pattern-ml-platform
  preemption:
    reclaimWithinCohort: Any
    withinClusterQueue: LowerPriority
  # ephemeral-storage is covered here, not omitted, because Kueue requires
  # EVERY resource a pod requests to be provided by some flavor in the queue.
  # Leaving it out does not mean "ignore it" — the Workload is refused with
  #
  #   couldn't assign flavors to pod set main:
  #   resource ephemeral-storage unavailable in ClusterQueue
  #
  # and sits Pending forever. @remote_step always sets an
  # ephemeral-storage request (pods unpack wheels and write temp files), so
  # every step would be unschedulable without this.
  #
  # It is not a per-team tunable like cpu/memory: the real ceiling is the
  # 200Gi data volume on each Karpenter node plus the NodePool's own cpu
  # limit, so a generous fixed figure keeps Kueue satisfied without adding a
  # dimension nobody wants to manage.
  resourceGroups:
    - coveredResources: ["cpu", "memory", "ephemeral-storage"]
      flavors:
        - name: x86-ondemand
          resources:
            - name: cpu
              nominalQuota: "${cpu_quota}"
              borrowingLimit: "${cpu_borrow}"
            - name: memory
              nominalQuota: "${memory_quota}"
              borrowingLimit: "${memory_borrow}"
            - name: ephemeral-storage
              nominalQuota: "10Ti"
              borrowingLimit: "10Ti"
        - name: arm64-ondemand
          resources:
            - name: cpu
              nominalQuota: "${cpu_quota}"
              borrowingLimit: "${cpu_borrow}"
            - name: memory
              nominalQuota: "${memory_quota}"
              borrowingLimit: "${memory_borrow}"
            - name: ephemeral-storage
              nominalQuota: "10Ti"
              borrowingLimit: "10Ti"
    # GPU is its own resource group: a team's CPU quota should never be
    # convertible into GPU capacity, and vice versa.
    - coveredResources: ["nvidia.com/gpu"]
      flavors:
        - name: gpu
          resources:
            - name: nvidia.com/gpu
              nominalQuota: "${gpu_quota}"
              borrowingLimit: "${gpu_borrow}"

---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: default
  namespace: ${team}
spec:
  clusterQueue: ${team}

---
# ServiceAccount the runner pods use. Bound to an IAM role via
# aws_eks_pod_identity_association (see pod-identity.tf), which is what
# gives the pod S3 + CloudWatch access without any static credentials.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: remote-step-runner
  namespace: ${team}
