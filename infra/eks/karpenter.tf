# Karpenter — node autoscaling.
#
# Order matters here and Terraform will not infer all of it:
#   1. helm_release installs the controller + CRDs
#   2. EC2NodeClass manifests (need the CRDs)
#   3. NodePool manifests (reference the node classes)
# The depends_on chain below encodes that.

resource "helm_release" "karpenter" {
  name             = "karpenter"
  namespace        = "kube-system"
  create_namespace = false

  repository = "oci://public.ecr.aws/karpenter"
  chart      = "karpenter"
  version    = var.karpenter_version

  # The controller must land on the always-on managed node group. If it were
  # allowed onto a Karpenter-provisioned node it could schedule itself onto a
  # node it later decides to consolidate — and take itself down with it.
  values = [yamlencode({
    settings = {
      clusterName = module.eks.cluster_name
      # Empty: no spot capacity in any NodePool, so no interruption queue.
      interruptionQueue = ""
    }

    serviceAccount = {
      # Pod Identity supplies credentials; no IRSA role annotation needed.
      name = "karpenter"
    }

    controller = {
      resources = {
        requests = { cpu = "1", memory = "1Gi" }
        limits   = { memory = "1Gi" }
      }
    }

    nodeSelector = {
      "pattern-ml-platform.pattern.com/role" = "system"
    }

    # Spread the two replicas across the system nodes so a single node
    # replacement does not drop the controller entirely.
    replicas = 2
    topologySpreadConstraints = [{
      maxSkew           = 1
      topologyKey       = "kubernetes.io/hostname"
      whenUnsatisfiable = "DoNotSchedule"
      labelSelector = {
        matchLabels = { "app.kubernetes.io/instance" = "karpenter" }
      }
    }]
  })]

  depends_on = [
    module.eks,
    module.karpenter,
  ]
}

# ---------------------------------------------------------------------------
# EC2NodeClass — the AWS shape of a node (AMI, disks, metadata options)
# ---------------------------------------------------------------------------

data "kubectl_file_documents" "nodeclasses" {
  content = templatefile("${path.module}/karpenter-nodeclasses.yaml.tpl", {
    cluster_name   = module.eks.cluster_name
    node_role_name = module.karpenter.node_iam_role_name
  })
}

resource "kubectl_manifest" "nodeclasses" {
  for_each  = data.kubectl_file_documents.nodeclasses.manifests
  yaml_body = each.value

  # Karpenter validates a node class against live AWS on admission; give the
  # webhook time to come up after the chart install.
  wait = true

  depends_on = [helm_release.karpenter]
}

# ---------------------------------------------------------------------------
# NodePool — the Kubernetes shape of a node (arch, families, limits, taints)
# ---------------------------------------------------------------------------

data "kubectl_file_documents" "nodepools" {
  content = file("${path.module}/karpenter-nodepools.yaml")
}

resource "kubectl_manifest" "nodepools" {
  for_each  = data.kubectl_file_documents.nodepools.manifests
  yaml_body = each.value

  depends_on = [kubectl_manifest.nodeclasses]
}

# ---------------------------------------------------------------------------
# NVIDIA device plugin — DELIBERATELY NOT INSTALLED
#
# Bottlerocket's NVIDIA variant ships the driver, the container toolkit AND
# the Kubernetes device plugin, the last as a host service. A GPU node built
# from `bottlerocket-aws-k8s-1.35-nvidia-*` advertises nvidia.com/gpu on its
# own, with no DaemonSet from us.
#
# Verified on a g6.xlarge launched by the gpu NodePool:
#
#   os-image                       Bottlerocket OS 1.64.0 (aws-k8s-1.35-nvidia)
#   capacity/allocatable  gpu = 1
#   nvidia-smi in-pod              NVIDIA L4, driver 580.159.03, CUDA 13.0
#
# Do NOT install nvidia/k8s-device-plugin alongside this. It cannot run on
# Bottlerocket and crashloops on every GPU node:
#
#   E factory.go:112] Incompatible strategy detected auto
#   E main.go:173] error starting plugins: ... invalid device discovery
#                  strategy
#
# The plugin expects a conventional container-toolkit layout to discover
# devices; Bottlerocket is a read-only OS that does not present one. The
# built-in plugin already advertises the GPU, so a second one adds nothing.
#
# If a future node family needs a plugin that is not built in (a non-
# Bottlerocket AMI, or time-slicing/MIG configuration), install it then —
# and scope it to those nodes only.
# ---------------------------------------------------------------------------
