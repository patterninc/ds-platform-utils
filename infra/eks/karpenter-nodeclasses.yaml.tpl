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

  subnetSelectorTerms:
    - tags:
        karpenter.sh/discovery: ${cluster_name}

  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: ${cluster_name}

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

  subnetSelectorTerms:
    - tags:
        karpenter.sh/discovery: ${cluster_name}

  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: ${cluster_name}

  blockDeviceMappings:
    - deviceName: /dev/xvda
      ebs:
        volumeSize: 10Gi
        volumeType: gp3
        deleteOnTermination: true
        encrypted: true
    # Bigger and faster than the CPU class: GPU images carry CUDA + cuDNN,
    # and model weights land here before they reach device memory.
    - deviceName: /dev/xvdb
      ebs:
        volumeSize: 500Gi
        volumeType: gp3
        iops: 10000
        throughput: 1000
        deleteOnTermination: true
        encrypted: true

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
