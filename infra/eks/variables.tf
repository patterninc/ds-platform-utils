# Single environment by design — one cluster serves every team's workloads,
# partitioned by Kueue ClusterQueue rather than by separate infrastructure.

variable "region" {
  description = "AWS region for the cluster."
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS SSO profile for the account hosting the EKS cluster."
  type        = string
}

variable "cluster_version" {
  description = <<-EOT
    EKS control plane version.

    1.35 constraints:
      - cgroup v1 support removed. Bottlerocket 1.35 sets `failCgroupV1:
        false`, so no action needed.
      - Last release supporting containerd 1.x. Bottlerocket ships 2.x.
      - IPVS kube-proxy mode deprecated, removed in 1.36. This cluster uses
        iptables mode.
  EOT
  type        = string
  default     = "1.35"
}

variable "vpc_cidr" {
  description = <<-EOT
    CIDR for the cluster VPC.

    NOT ISSUED BY IPAM — 10.42.0.0/16 was chosen, not assigned. Have
    922016401078 assign a range before this cluster is treated as permanent;
    renumbering later means rebuilding the VPC and the cluster with it.
  EOT
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = <<-EOT
    Availability zones to spread subnets across. Three gives Karpenter room
    to find capacity when one AZ is short on a newer instance family; two
    halves the NAT gateway bill.
  EOT
  type        = number
  default     = 3
}

variable "teams" {
  description = <<-EOT
    Teams that get their own namespace, ClusterQueue and LocalQueue.

    Quotas are per-team nominal capacity; borrow limits cap how far a team
    can reach into the shared cohort when others are idle. Memory values are
    Kubernetes quantities ("512Gi").

    VALUES ARE PER FLAVOR. kueue-team.yaml.tpl declares each quota against
    both the x86-ondemand and arm64-ondemand flavors, and Kueue's per-flavor
    quotas are additive — cpu_quota=256 grants 256 x86 cores AND 256 arm64
    cores. Halve these if the intent is N cores regardless of architecture.
    The NodePool `limits` in karpenter-nodepools.yaml are the real ceiling.
  EOT
  type = map(object({
    cpu_quota     = string
    cpu_borrow    = string
    memory_quota  = string
    memory_borrow = string
    gpu_quota     = string
    gpu_borrow    = string
    # CPU and memory available *on GPU nodes*. Separate from cpu_quota because
    # cpu, memory and gpu share one Kueue resource group: a GPU pod is assigned
    # the `gpu` flavor for everything, so it draws its cpu/memory from here.
    # 0 for a team with no GPU quota, which also stops a CPU-only pod ever
    # being placed on a GPU node.
    gpu_cpu_quota     = string
    gpu_cpu_borrow    = string
    gpu_memory_quota  = string
    gpu_memory_borrow = string
  }))
  default = {
    forecasting = {
      cpu_quota         = "256"
      cpu_borrow        = "512"
      memory_quota      = "1024Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    advertising = {
      cpu_quota         = "128"
      cpu_borrow        = "512"
      memory_quota      = "512Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    market-intelligence = {
      cpu_quota         = "256"
      cpu_borrow        = "512"
      memory_quota      = "1024Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    operations = {
      cpu_quota         = "128"
      cpu_borrow        = "512"
      memory_quota      = "512Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    # NLP is part of content, so this namespace carries that work — hence the
    # cluster's only GPU quota, and a larger CPU/memory allocation than the
    # other domains. Dropping the GPU quota when the nlp namespace went away
    # would leave the NLP flows unschedulable rather than merely cramped.
    content = {
      cpu_quota         = "256"
      cpu_borrow        = "512"
      memory_quota      = "1024Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "8"
      gpu_borrow        = "16"
      gpu_cpu_quota     = "192"
      gpu_cpu_borrow    = "384"
      gpu_memory_quota  = "768Gi"
      gpu_memory_borrow = "1536Gi"
    }
    revops = {
      cpu_quota         = "128"
      cpu_borrow        = "512"
      memory_quota      = "512Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    reference = {
      cpu_quota         = "128"
      cpu_borrow        = "512"
      memory_quota      = "512Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    demand-generation = {
      cpu_quota         = "128"
      cpu_borrow        = "512"
      memory_quota      = "512Gi"
      memory_borrow     = "2048Gi"
      gpu_quota         = "0"
      gpu_borrow        = "4"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
    # Where a step lands when neither @remote_step(team=...) nor
    # `--tag ds.domain:<team>` names one. Ad-hoc and exploratory work, so the
    # quota is deliberately small and borrows little: an untagged run should
    # be able to get a node, but never at the cost of a team's own capacity.
    # No GPU — an untagged run should not silently take a GPU node.
    sandbox = {
      cpu_quota         = "32"
      cpu_borrow        = "64"
      memory_quota      = "128Gi"
      memory_borrow     = "256Gi"
      gpu_quota         = "0"
      gpu_borrow        = "0"
      gpu_cpu_quota     = "0"
      gpu_cpu_borrow    = "0"
      gpu_memory_quota  = "0"
      gpu_memory_borrow = "0"
    }
  }
}

variable "enable_public_endpoint" {
  description = <<-EOT
    Whether the EKS API server gets an internet-facing endpoint.

    Access control comes from var.public_endpoint_allowed_cidrs (network) and
    EKS access entries (authorisation).
  EOT
  type        = bool
  default     = true
}

variable "public_endpoint_allowed_cidrs" {
  description = <<-EOT
    Source CIDRs allowed to reach the public API endpoint. Must be PUBLIC
    addresses — this is the source IP the API server sees.

    Defaults to 0.0.0.0/0 because getting it wrong locks Terraform out
    mid-apply: the cluster exists but the kubernetes/helm providers cannot
    reach it, and recovery is an out-of-band console change. Narrow it.

      35.82.100.167/32   obp-301bcf's NAT gateway EIP — the address
                         Outerbounds driver pods egress from. Omitting it
                         breaks every @remote_step submission.
      <operator egress>  whoever runs terraform apply and kubectl.

    EKS access entries gate authorisation regardless of this list; this only
    controls who can open a connection.
  EOT
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "outerbounds_task_role_arns" {
  description = <<-EOT
    Outerbounds pod task roles allowed to assume the submitter and
    artifact-reader roles.

    A LIST because a pod's task role depends on which Outerbounds perimeter
    the flow was deployed to, and a pod gets exactly one of them. Trusting
    only one means a flow deployed to another perimeter fails at
    sts:AssumeRole.

      obp-5p6le9-task        sub = pattern         default perimeter
      obp-301bcf-task--prod  sub = pattern--prod   prod perimeter
      obp-301bcf-task        (no sub condition)    any perimeter
  EOT
  type        = list(string)
  default = [
    "arn:aws:iam::209479263910:role/obp-5p6le9-task",
    "arn:aws:iam::209479263910:role/obp-301bcf-task",
    "arn:aws:iam::209479263910:role/obp-301bcf-task--prod",
  ]
}

variable "outerbounds_deployment_tag_value" {
  description = "Value for the `outerbounds.com/accessible-by-deployment` tag Outerbounds uses to discover cross-account roles."
  type        = string
  default     = "pattern"
}

variable "karpenter_version" {
  description = <<-EOT
    Karpenter Helm chart version.

    1.14.1 is an LTS release supported until July 2027. Kubernetes 1.35
    requires Karpenter >= 1.9 per the upstream compatibility matrix
    (https://karpenter.sh/docs/upgrading/compatibility/).

    The chart and its CRDs move together — bump both or neither.
  EOT
  type        = string
  default     = "1.14.1"

  validation {
    condition     = tonumber(split(".", var.karpenter_version)[0]) > 1 || tonumber(split(".", var.karpenter_version)[1]) >= 9
    error_message = "Kubernetes 1.35 requires Karpenter >= 1.9."
  }
}

variable "metrics_server_version" {
  description = <<-EOT
    metrics-server Helm chart version (chart 3.14.0 ships app v0.9.0).

    Supplies the resource metrics API that `kubectl top` reads. EKS does not
    install it as an addon, so it is a plain Helm release.
  EOT
  type        = string
  default     = "3.14.0"
}

variable "kueue_version" {
  description = <<-EOT
    Kueue Helm chart version, from oci://registry.k8s.io/kueue/charts/kueue.

    Kueue is pre-1.0 and its API shifts between minors — an upgrade needs the
    ClusterQueue and ResourceFlavor manifests re-read, not a routine bump.
  EOT
  type        = string
  default     = "0.19.3"
}

variable "s3_integration_role_arns" {
  description = <<-EOT
    IAM roles backing Outerbounds S3 integrations that @remote_step steps may
    use, which the runner pod is allowed to assume.

    An Outerbounds S3 integration is a role in the *target bucket's* account
    whose trust policy names the Outerbounds deployment task role
    (arn:aws:iam::209479263910:role/obp-5p6le9-task) and which is tagged
    outerbounds.com/accessible-by-deployment = pattern. That works on an
    Outerbounds pod, which runs as that task role.

    A @remote_step body does not. It runs in our EKS cluster as the runner
    ServiceAccount, whose pod identity role is
    <name>-ob-runner -- so the integration role does not trust it, and the
    runner role has no sts:AssumeRole permission of its own. Both sides have
    to change for a step to reach an integration bucket:

      1. list the integration's role ARN here, which grants the runner
         sts:AssumeRole on exactly that role, and
      2. add the runner role as a second principal on the integration role's
         trust policy, alongside the Outerbounds task role.

    Listed explicitly rather than granted by wildcard-plus-tag-condition: the
    target roles live in other accounts, and an ARN list is auditable from
    this repo without reading tags in an account we may not control.
  EOT
  type = list(string)
  default = [
    # pattern-demand-forecast-models -- written by weekly_flow.publish_artifacts.
    # Registered as the `demand-forecast-models` integration in the prod
    # perimeter, and also assumed directly by ARN from the step body.
    "arn:aws:iam::209479263910:role/ob-demand-forecast-models",
    # market-mix-modeling. mmm-access-role backs the `market-mix-modeling`
    # integration in the default perimeter; the prod-spectrum one is assumed
    # directly by ARN with no registered integration.
    "arn:aws:iam::209479263910:role/mmm-access-role",
    "arn:aws:iam::209479263910:role/prod-spectrum-mmm-access-role",
    # search-term-brand-link. Trusts only obp-5p6le9-task, so it is reachable
    # from the default perimeter only.
    "arn:aws:iam::209479263910:role/search-term-brand-link-access-role",
    # niche-insights. The integration is registered in the default perimeter
    # and points here, but the role does not exist in the account (GetRole ->
    # NoSuchEntity), so that integration is currently broken for ordinary
    # steps too. Listed so it works the moment the role returns; granting
    # AssumeRole on a missing ARN is inert.
    "arn:aws:iam::209479263910:role/niche-insights-access-role",
  ]
}
