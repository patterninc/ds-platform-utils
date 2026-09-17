# EC2NodeClass — the AWS-side shape of nodes Karpenter launches.
#
# Terraform renders this template (see karpenter.tf) so the cluster name and
# node role come from real resource attributes rather than being pinned by
# hand.
#
# Bottlerocket, not AL2023:
#   - immutable read-only root, no package manager, no SSH
#   - atomic two-partition updates with rollback
#   - minimal attack surface (only containerd + kubelet + the API daemon)
#   - config is declarative TOML instead of a shell userData script
#
# The volume split matters for us. Bottlerocket separates the OS disk from
# the data disk:
#   /dev/xvda  OS       small, immutable, never fills up
#   /dev/xvdb  data     container images + writable layers + ephemeral
#                       storage — this is what holds our runner image, the
#                       per-job uv venv, and any multi-GB pickle that spills
#                       to disk, so it gets the space and the IOPS.
#
# Two classes:
#   default  Bottlerocket standard      — every CPU NodePool
#   gpu      Bottlerocket NVIDIA variant — Karpenter resolves the nvidia
#            flavor automatically when the selected instance type has a GPU,
#            so the same alias covers both. Drivers, the container toolkit
#            AND the device plugin all ship in the image; do not install a
#            separate nvidia-device-plugin (see karpenter.tf).

---
apiVersion: karpenter.k8s.aws/v1
kind: EC2NodeClass
metadata:
  name: default
spec:
  # Alias pins the family but lets AWS roll patch versions; Karpenter
  # re-resolves on each launch so nodes stay current without an AMI id
  # in git.
  amiSelectorTerms:
    - alias: bottlerocket@latest

  role: ${node_role_name}

  # Subnets by id, not by tag. The cluster runs in the account's shared VPC,
  # whose subnets are declared in patterninc/aws_account_setup -- a
  # karpenter.sh/discovery tag added out of band would be stripped the next time
  # that repo is applied, and Karpenter would then quietly find no subnets and
  # launch nothing. The failure is silent and arrives via an unrelated deploy,
  # which is the worst combination. Ids cannot drift.
  #
  subnetSelectorTerms:
%{ for id in subnet_ids ~}
    - id: ${id}
%{ endfor ~}

  # The node security group by id, not by tag.
  #
  # Matching on karpenter.sh/discovery attached THREE groups to every Karpenter
  # node where the managed node group gets one: the scoped node SG, the module's
  # cluster SG, and EKS's own eks-cluster-sg-*. That last one carries an
  # all-protocol, all-port, self-referencing ingress rule, so a node running
  # user pickle code could reach every other Karpenter node on any port and the
  # control-plane ENIs -- straight past the node SG rules that exist to prevent
  # exactly that.
  #
  # The tag lands on the cluster SG because `tags` on module.eks includes the
  # discovery tag and EKS propagates cluster tags to the SG it creates. An id
  # cannot be propagated onto something else.
  securityGroupSelectorTerms:
    - id: ${node_sg_id}

  blockDeviceMappings:
    # OS volume — Bottlerocket's root is read-only and tiny by design.
    - deviceName: /dev/xvda
      ebs:
        volumeSize: 10Gi
        volumeType: gp3
        deleteOnTermination: true
        encrypted: true
    # Data volume — container images, writable layers, ephemeral storage.
    # gp3 baseline is 3000 IOPS / 125 MB/s; we stream GB-scale pickles
    # through this disk, so buy throughput.
    - deviceName: /dev/xvdb
      ebs:
        volumeSize: 200Gi
        volumeType: gp3
        iops: 6000
        throughput: 500
        deleteOnTermination: true
        encrypted: true

  # Bottlerocket takes TOML, not a shell script.
  userData: |
    [settings.kubernetes]
    "max-pods" = 110

    [settings.kernel]
    lockdown = "integrity"

  # IMDSv2 required, hop limit 1 so a compromised pod can't reach instance
  # metadata through the container network.
  metadataOptions:
    httpEndpoint: enabled
    httpProtocolIPv6: disabled
    httpPutResponseHopLimit: 1
    httpTokens: required

  tags:
    ManagedBy: karpenter
    Cluster: ${cluster_name}
    Workload: pattern-ml-platform

---
apiVersion: karpenter.k8s.aws/v1
kind: EC2NodeClass
metadata:
  name: gpu
spec:
  amiSelectorTerms:
    - alias: bottlerocket@latest

  role: ${node_role_name}

  # Subnets by id, not by tag. The cluster runs in the account's shared VPC,
  # whose subnets are declared in patterninc/aws_account_setup -- a
  # karpenter.sh/discovery tag added out of band would be stripped the next time
  # that repo is applied, and Karpenter would then quietly find no subnets and
  # launch nothing. The failure is silent and arrives via an unrelated deploy,
  # which is the worst combination. Ids cannot drift.
  #
  subnetSelectorTerms:
%{ for id in subnet_ids ~}
    - id: ${id}
%{ endfor ~}

  # The node security group by id, not by tag.
  #
  # Matching on karpenter.sh/discovery attached THREE groups to every Karpenter
  # node where the managed node group gets one: the scoped node SG, the module's
  # cluster SG, and EKS's own eks-cluster-sg-*. That last one carries an
  # all-protocol, all-port, self-referencing ingress rule, so a node running
  # user pickle code could reach every other Karpenter node on any port and the
  # control-plane ENIs -- straight past the node SG rules that exist to prevent
  # exactly that.
  #
  # The tag lands on the cluster SG because `tags` on module.eks includes the
  # discovery tag and EKS propagates cluster tags to the SG it creates. An id
  # cannot be propagated onto something else.
  securityGroupSelectorTerms:
    - id: ${node_sg_id}

  blockDeviceMappings:
    - deviceName: /dev/xvda
      ebs:
        volumeSize: 10Gi
        volumeType: gp3
        deleteOnTermination: true
        encrypted: true
    # Small, because containerd and ephemeral storage live on the instance
    # store instead -- see instanceStorePolicy below. This only has to hold
    # what Bottlerocket itself puts on the data volume.
    - deviceName: /dev/xvdb
      ebs:
        volumeSize: 100Gi
        volumeType: gp3
        iops: 6000
        throughput: 500
        deleteOnTermination: true
        encrypted: true

  # Every GPU instance type this pool may launch ships local NVMe, and it is
  # already paid for in the instance price:
  #
  #   g6.xlarge        250 GB      g6e.2xlarge      450 GB
  #   g6.2xlarge       450 GB      g6e.12xlarge   3,800 GB
  #   g6.48xlarge    7,520 GB      p6-b200.48xlarge  30,400 GB
  #
  # Verified with describe-instance-types; every g6/g6e/p6-b200 size returns
  # InstanceStorageSupported=true. Without this the disks sat idle while we
  # paid separately for a 500 GB gp3 at 10,000 provisioned IOPS -- for the one
  # workload that most wants local scratch, since datasets, checkpoints and
  # weights all stage through it.
  #
  # RAID0 so multi-disk types present one filesystem. Instance store is wiped
  # on stop/terminate, which costs nothing here: these nodes are ephemeral and
  # the volumes are already deleteOnTermination.
  instanceStorePolicy: RAID0

  userData: |
    [settings.kubernetes]
    "max-pods" = 110

    [settings.kernel]
    lockdown = "integrity"

  metadataOptions:
    httpEndpoint: enabled
    httpProtocolIPv6: disabled
    httpPutResponseHopLimit: 1
    httpTokens: required

  tags:
    ManagedBy: karpenter
    Cluster: ${cluster_name}
    Workload: pattern-ml-platform-gpu
