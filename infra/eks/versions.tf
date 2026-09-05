terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.35"
    }
    # alekc/kubectl is the maintained fork of gavinbunney/kubectl. We need it
    # because hashicorp/kubernetes' `kubernetes_manifest` resolves a CRD's
    # schema at *plan* time — so it cannot plan a NodePool or ClusterQueue in
    # the same run that installs the CRDs. `kubectl_manifest` defers to apply
    # time and sidesteps that chicken-and-egg.
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.1"
    }
  }

  # Remote state from day one so the cluster is never owned by one laptop,
  # and so moving applies into GitHub Actions later needs no state migration.
  #
  # The account id is in the bucket name because S3's namespace is global —
  # the unsuffixed `pattern-ml-platform-tfstate` is already taken by another
  # AWS customer.
  backend "s3" {
    bucket  = "pattern-ml-platform-tfstate-209479263910"
    key     = "eks/terraform.tfstate"
    region  = "us-west-2"
    encrypt = true
    # Terraform 1.10+ locks via a lockfile object in the same bucket, so
    # there is no DynamoDB table to provision or pay for.
    use_lockfile = true
  }
}
