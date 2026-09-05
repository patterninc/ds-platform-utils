locals {
  name = "pattern-ml-platform"

  # Karpenter's EC2NodeClass finds subnets and security groups by tag rather
  # than by id, so the same manifest works across accounts and rebuilds.
  # Everything Karpenter is allowed to launch into carries this tag.
  discovery_tag = { "karpenter.sh/discovery" = local.name }

  tags = {
    Project   = local.name
    ManagedBy = "terraform"
    Component = "pattern-ml-platform"
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Network
#
# Nodes live in private subnets and reach S3/ECR through a NAT gateway, with
# a gateway endpoint short-circuiting S3 so the multi-GB pickle traffic never
# touches NAT (which bills per GB and would dominate the cost of a run).
# ---------------------------------------------------------------------------

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.7"

  name = local.name
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, var.az_count)
  private_subnets = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 8, i + 48)]

  enable_nat_gateway = true
  # One NAT for the whole VPC. Karpenter spreads nodes across AZs for capacity,
  # not for HA — a node dying mid-step already costs us the step, so paying 3x
  # for zonal NAT redundancy buys nothing here.
  single_nat_gateway = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  private_subnet_tags = merge(local.discovery_tag, {
    "kubernetes.io/role/internal-elb" = 1
  })

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }

  tags = local.tags
}

# S3 gateway endpoint — a cost control, not a convenience. Steps ship
# GB-scale pickles through S3, and NAT bills $0.045/GB. A gateway endpoint is
# free and keeps that traffic off NAT entirely.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"

  # Associating the private route tables is what actually diverts traffic —
  # without this the endpoint exists but nothing uses it.
  route_table_ids = module.vpc.private_route_table_ids

  tags = merge(local.tags, {
    Name = "${local.name}-s3"
  })
}

# ---------------------------------------------------------------------------
# EKS control plane
# ---------------------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.25"

  name               = local.name
  kubernetes_version = var.cluster_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

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
    aws-ebs-csi-driver = {
      # The controller calls EC2 to create and attach volumes, so it needs a
      # role. Omitting this leaves it with no credentials and no IMDS to fall
      # back to (Bottlerocket withholds IMDS from pods), which crashloops
      # `ebs-plugin` and hangs the addon in CREATING until terraform times
      # out. See ebs-csi.tf.
      pod_identity_association = [{
        role_arn        = aws_iam_role.ebs_csi.arn
        service_account = "ebs-csi-controller-sa"
      }]
    }
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

  tags = merge(local.tags, local.discovery_tag)
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
