# Pod Identity for the runner pods.
#
# Chosen over IRSA because the trust policy is generic — it names
# pods.eks.amazonaws.com rather than this cluster's OIDC issuer — so the same
# role survives a cluster rebuild, and one role can be associated with many
# clusters without editing its trust policy.
#
# One role, associated into every team namespace. Teams are separated by Kueue
# quota and namespace, not by blast radius on the payload bucket: every step
# reads and writes the same bucket under run-scoped prefixes, so per-team roles
# would add ceremony without adding isolation.

data "aws_iam_policy_document" "runner_assume" {
  statement {
    actions = [
      "sts:AssumeRole",
      # TagSession lets EKS stamp the association's namespace and service
      # account onto the session, which is what makes the CloudTrail record
      # of an S3 call attributable to a team.
      "sts:TagSession",
    ]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "runner" {
  name               = "${local.name}-ob-runner"
  assume_role_policy = data.aws_iam_policy_document.runner_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "runner_permissions" {
  statement {
    sid = "PayloadBucketReadWrite"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      aws_s3_bucket.payload.arn,
      "${aws_s3_bucket.payload.arn}/*",
    ]
  }

  statement {
    sid = "StepLogs"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["${aws_cloudwatch_log_group.steps.arn}:*"]
  }
}

resource "aws_iam_role_policy" "runner" {
  name   = "runner-permissions"
  role   = aws_iam_role.runner.id
  policy = data.aws_iam_policy_document.runner_permissions.json
}

# Bind the role to the runner ServiceAccount in each team's namespace. The
# namespace and ServiceAccount are created by kueue-team.yaml.tpl, so this
# depends on those manifests having landed.
resource "aws_eks_pod_identity_association" "runner" {
  for_each = var.teams

  cluster_name    = module.eks.cluster_name
  namespace       = each.key
  service_account = "remote-step-runner"
  role_arn        = aws_iam_role.runner.arn

  tags = local.tags

  depends_on = [kubectl_manifest.kueue_teams]
}
