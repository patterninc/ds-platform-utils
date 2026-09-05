# IAM for the EBS CSI driver.
#
# The driver's controller calls EC2 directly — CreateVolume, AttachVolume,
# DescribeAvailabilityZones — so it needs credentials of its own. Without
# them the `ebs-plugin` container fails its startup health check:
#
#   Failed health check (verify network connection and IAM credentials):
#   dry-run EC2 API call failed: ... DescribeAvailabilityZones,
#   get credentials: ... no EC2 IMDS role found
#
# and takes the liveness probe down with it, so the addon never reaches
# ACTIVE and terraform blocks on the wait until it times out.
#
# The IMDS fallback in that error is not a usable path here: Bottlerocket
# does not expose the instance metadata service to pods, which is the
# behaviour we want. Credentials have to be granted explicitly.
#
# Pod Identity rather than IRSA, matching aws_iam_role.runner — the trust
# policy names the generic service principal instead of this cluster's OIDC
# issuer, so the role survives a cluster rebuild.

data "aws_iam_policy_document" "ebs_csi_assume" {
  statement {
    actions = [
      "sts:AssumeRole",
      "sts:TagSession",
    ]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${local.name}-ob-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_assume.json
  tags               = local.tags
}

# The AWS-managed policy is the whole permission set the driver needs, and it
# is scoped by tag: the mutating actions require volumes carrying
# `ebs.csi.aws.com/cluster` or `CSIVolumeName`, which the driver stamps on
# what it creates. It cannot touch a volume it did not make — including the
# node root and data volumes.
resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
