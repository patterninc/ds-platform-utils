provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = "pattern-ml-platform"
      ManagedBy = "terraform"
    }
  }

  # Tags applied to our resources by tooling outside this state.
  #
  # `QSConfigId-*` comes from an org-managed AWS Systems Manager Quick Setup
  # patch policy, which stamps it on the IAM roles it manages — here the
  # Karpenter node role, which also carries that policy's
  # AWSQuickSetupPatchPolicyBaselineAccess and AmazonSSMPatchAssociation.
  #
  # Without this, every plan wants to strip the tag, because the role is
  # declared inside the Karpenter submodule and there is no resource block of
  # ours to hang `ignore_changes` on. Stripping it is the wrong fix twice
  # over: it may take those nodes out of Quick Setup's scope, and Quick Setup
  # would likely re-apply it and reintroduce the drift on the next plan.
  #
  # Ignoring by prefix rather than by exact key because the id is per-config:
  # a rebuilt or re-pointed Quick Setup config issues a new one.
  ignore_tags {
    key_prefixes = ["QSConfigId-"]
  }
}

# The kubernetes/helm/kubectl providers all authenticate the same way: ask the
# AWS CLI for a short-lived EKS token. Using `exec` rather than a stored token
# means credentials refresh on every apply instead of going stale between runs.
locals {
  eks_exec = {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks", "get-token",
      "--cluster-name", module.eks.cluster_name,
      "--region", var.region,
      "--profile", var.aws_profile,
    ]
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = local.eks_exec.api_version
    command     = local.eks_exec.command
    args        = local.eks_exec.args
  }
}

provider "helm" {
  kubernetes = {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec = {
      api_version = local.eks_exec.api_version
      command     = local.eks_exec.command
      args        = local.eks_exec.args
    }
  }
}

provider "kubectl" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  load_config_file       = false

  exec {
    api_version = local.eks_exec.api_version
    command     = local.eks_exec.command
    args        = local.eks_exec.args
  }
}
