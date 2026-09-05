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
  }))
  default = {
    forecasting = {
      cpu_quota     = "256"
      cpu_borrow    = "512"
      memory_quota  = "1024Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "0"
      gpu_borrow    = "4"
    }
    nlp = {
      cpu_quota     = "256"
      cpu_borrow    = "512"
      memory_quota  = "1024Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "8"
      gpu_borrow    = "16"
    }
    advertising = {
      cpu_quota     = "128"
      cpu_borrow    = "512"
      memory_quota  = "512Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "0"
      gpu_borrow    = "4"
    }
    market-intelligence = {
      cpu_quota     = "256"
      cpu_borrow    = "512"
      memory_quota  = "1024Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "0"
      gpu_borrow    = "4"
    }
    operations = {
      cpu_quota     = "128"
      cpu_borrow    = "512"
      memory_quota  = "512Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "0"
      gpu_borrow    = "4"
    }
    content = {
      cpu_quota     = "128"
      cpu_borrow    = "512"
      memory_quota  = "512Gi"
      memory_borrow = "2048Gi"
      gpu_quota     = "0"
      gpu_borrow    = "4"
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
