locals {
  name = "pattern-ml-platform"


  tags = {
    Project   = local.name
    ManagedBy = "terraform"
    Component = "pattern-ml-platform"
  }
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Network
#
# The cluster runs in the account's shared VPC (`local-oregon`) rather than one
# of its own. That VPC was created once by SRE via patterninc/aws_account_setup
# and is ours to operate; it already provides everything a dedicated VPC was
# built to provide, and more:
#
#   - an allocated CIDR. Ours was 10.42.0.0/16, which was chosen rather than
#     issued, so it could have collided with a later allocation.
#   - four private /20s across four AZs (~16k free addresses) versus three.
#     EKS here runs VPC-CNI with prefix delegation, so nodes claim addresses in
#     /28 blocks; headroom is the thing that matters and there is more of it.
#   - a NAT gateway, so we are no longer paying for a second one.
#   - gateway endpoints for S3 and DynamoDB, plus interface endpoints for
#     SageMaker and Bedrock. The S3 one is the cost control a dedicated VPC had
#     to build by hand: step artifacts are GB-scale and NAT bills per GB.
#   - a Transit Gateway attachment, which a dedicated VPC had no route to.
#
# Only OUR resources carry our tags. Nothing here modifies the VPC or its
# subnets, which keep SRE's Owner/Repo/CostCenter tags -- see the subnet
# selection note in karpenter-nodeclasses.yaml.tpl for why Karpenter matches
# subnets by id rather than by adding a tag of ours to someone else's resource.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# EKS control plane
# ---------------------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.25"

  name               = local.name
  kubernetes_version = var.cluster_version

  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  # The CIDR list is the network control here — see
  # var.public_endpoint_allowed_cidrs.
  endpoint_public_access       = var.enable_public_endpoint
  endpoint_public_access_cidrs = var.public_endpoint_allowed_cidrs

  # In-VPC clients reach the API over the private ENIs rather than
  # hairpinning out through NAT.
  endpoint_private_access = true

  # API_AND_CONFIG_MAP would keep the legacy aws-auth ConfigMap alive. IAM
  # access entries are the only auth path here.
  authentication_mode = "API"

  # The identity running terraform gets cluster-admin so the kubernetes and
  # helm providers can reach the API on the first apply.
  enable_cluster_creator_admin_permissions = true

  addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni = {
      before_compute = true
      configuration_values = jsonencode({
        env = {
          # Prefix delegation lifts the per-node pod ceiling from the ENI
          # limit to /28 blocks, so a large instance is not capped at a
          # handful of pods by its network interfaces.
          ENABLE_PREFIX_DELEGATION = "true"
          WARM_PREFIX_TARGET       = "1"
        }
      })
    }
    eks-pod-identity-agent = { before_compute = true }
    # No aws-ebs-csi-driver on purpose. Nothing here claims a volume: a step's
    # scratch space is the container's writable layer on the node's data
    # volume, metered as `ephemeral-storage` (see @remote_step's
    # `ephemeral_gb`), and anything that has to outlive the pod goes to S3.
    #
    # An EBS PVC would be the wrong tool anyway — attach/detach adds tens of
    # seconds to a pod whose whole life may be a minute, and it pins the pod
    # to one AZ, which fights Karpenter's instance selection.
    #
    # Running the driver with nothing to provision cost a controller
    # Deployment plus a DaemonSet pod on every ephemeral node. If a future
    # workload does need a volume, re-adding this also needs a StorageClass on
    # `ebs.csi.aws.com`: the only class EKS ships is `gp2` on the *in-tree*
    # `kubernetes.io/aws-ebs` provisioner, which no longer provisions, so a PVC
    # against the default class sits in Pending rather than failing.
  }

  # REQUIRED. Kueue's visibility extension API server listens on 8082, and
  # the module's built-in node rules cover the usual webhook ports (443,
  # 4443, 6443, 8443, 9443, 10250, 10251) but not this one.
  #
  # Without it the failure is far worse than losing the visibility API: the
  # namespace controller must enumerate every API group to finalise a delete,
  # so one unreachable APIService breaks discovery and hangs EVERY namespace
  # deletion in Terminating, cluster-wide:
  #
  #   NamespaceDeletionDiscoveryFailure  True  DiscoveryFailed
  #     visibility.kueue.x-k8s.io/v1beta2: stale GroupVersion discovery
  node_security_group_additional_rules = {
    kueue_visibility = {
      description                   = "Kubernetes API server to Kueue visibility extension apiserver"
      protocol                      = "tcp"
      from_port                     = 8082
      to_port                       = 8082
      type                          = "ingress"
      source_cluster_security_group = true
    }
  }

  # A small always-on managed node group carries the cluster's own control
  # plane workloads — Karpenter itself, CoreDNS, Kueue's controller. Karpenter
  # cannot schedule the node that runs Karpenter, so this bootstraps the loop.
  # Everything else lands on Karpenter-provisioned nodes.
  eks_managed_node_groups = {
    # Do not rename this map key — it is the terraform address, and changing
    # it destroys and recreates the node group with no ordering guarantee.
    # Rename via `name` instead.
    system = {
      # Fixed name, not the module's default "system-<random>". The random
      # suffix exists so the submodule's create_before_destroy can stand two
      # node groups up at once; with a fixed name, any ForceNew change
      # (ami_type, subnet_ids, capacity_type) fails because AWS rejects the
      # duplicate name. Routine changes — instance_types, scaling — are
      # in-place, so this is fine; if a ForceNew change is ever needed,
      # change `name` in the same commit.
      name            = "eks-system-node"
      use_name_prefix = false

      ami_type = "BOTTLEROCKET_x86_64"
      # xlarge, not large: DaemonSets (~145m) + Karpenter (1000m) + CoreDNS
      # (100m) + Kueue controller (500m) + EBS CSI (~50m) is ~1795m against a
      # `large`'s ~1800m allocatable — full, with nothing left for a new
      # addon. xlarge lands the same set near 45%.
      #
      # x86 rather than Graviton: keeping the system plane single-arch avoids
      # requiring every DaemonSet to stay multi-arch. Graviton is a per-step
      # opt-in on the workload NodePools instead.
      #
      # m7i is a capacity fallback only, for when m8i is short in an AZ.
      instance_types = ["m8i.xlarge", "m7i.xlarge"]

      min_size = 2
      max_size = 4
      # Two, not one: Karpenter cannot provision the node that runs
      # Karpenter, so a single system node is an unrecoverable failure.
      desired_size = 2

      labels = {
        "pattern-ml-platform.pattern.com/role" = "system"
      }
    }
  }

  # No access_entries block here on purpose.
  #
  # Karpenter-launched nodes do need an EC2_LINUX access entry for their
  # node IAM role, but the karpenter submodule creates it itself
  # (create_access_entry defaults to true). Declaring it here as well
  # produces a dependency cycle: this module would need
  # module.karpenter.node_iam_role_arn, while module.karpenter needs
  # module.eks.cluster_name.
  #
  # The cross-account submitter entry lives in iam-crossaccount.tf as a
  # standalone aws_eks_access_entry for the same reason — it only needs the
  # cluster name, so it can depend on this module without the reverse.

  # Plain tags: no karpenter.sh/discovery.
  #
  # Karpenter selects both subnets and security groups by id now, so the tag
  # selects nothing. Removing it is not just tidiness -- the tag is what caused
  # the problem it used to serve. `tags` here propagates to the cluster, and EKS
  # propagates cluster tags onto the eks-cluster-sg-* it creates, so a
  # tag-based securityGroupSelectorTerms matched that group too. It carries an
  # all-protocol self-referencing ingress rule, which gave every Karpenter node
  # unrestricted access to every other one and to the control-plane ENIs,
  # straight past the scoped node SG. With the tag gone, reintroducing tag
  # selection cannot silently re-create that.
  tags = local.tags
}

# ---------------------------------------------------------------------------
# Karpenter controller IAM + node role
#
# The submodule wires the parts that are easy to get subtly wrong: the
# controller's Pod Identity association, the node IAM role and instance
# profile, and the access entry that lets launched nodes register.
# ---------------------------------------------------------------------------

module "karpenter" {
  source  = "terraform-aws-modules/eks/aws//modules/karpenter"
  version = "~> 21.25"

  cluster_name = module.eks.cluster_name

  # Pod Identity instead of IRSA: the trust policy is generic
  # (pods.eks.amazonaws.com) rather than pinned to this cluster's OIDC
  # issuer, so the role survives a cluster rebuild unchanged.
  create_pod_identity_association = true

  node_iam_role_use_name_prefix = false
  node_iam_role_name            = "${local.name}-karpenter-node"

  # Bottlerocket pulls its AMI id from SSM; the controller needs read access
  # to those parameters to resolve `alias: bottlerocket@latest`.
  ami_id_ssm_parameter_arns = [
    "arn:aws:ssm:${var.region}::parameter/aws/service/bottlerocket/*"
  ]

  # No spot capacity in any NodePool, so there is no interruption queue to
  # drain. Skipping it drops an SQS queue and four EventBridge rules.
  # Re-enable alongside any move to spot.
  enable_spot_termination = false

  node_iam_role_additional_policies = {
    # Nodes pull the runner image and write step logs.
    AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    CloudWatchAgentServerPolicy  = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  }

  tags = local.tags
}
