# Cross-account access for Outerbounds.
#
# Two distinct paths, both starting from the Outerbounds pod task role:
#
#   submitter        the @remote_step driver creates Jobs against this
#                    cluster's API. Needs eks:DescribeCluster to find the
#                    endpoint, plus an EKS access entry scoped to the team
#                    namespaces.
#
#   artifact-reader  a *non*-@remote_step step running on Outerbounds reaches
#                    through a RemoteArtifact proxy and pulls the pickle from
#                    our bucket. Outerbounds' own pod role has a permissions
#                    boundary that denies cross-account s3:GetObject, so the
#                    ref carries this role's ARN and RemoteArtifact.load()
#                    assumes it.
#
# Both are assumed by Outerbounds' pod role, so neither needs static keys.

# ---------------------------------------------------------------------------
# Submitter — creates Jobs
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "submitter_assume" {
  statement {
    # sts:SetSourceIdentity is required, not optional. Outerbounds federates
    # its pod task role with a source identity set, and a session carrying one
    # can only assume another role if that role's trust policy permits the
    # identity to propagate. Without it the hop fails with
    #
    #   assumed-role/obp-...-task is not authorized to perform:
    #   sts:SetSourceIdentity on resource: .../pattern-ml-platform-ob-submitter
    #
    # which reads like a missing AssumeRole grant but is not one.
    #
    # sts:TagSession is kept so the association's namespace and service
    # account can be stamped onto the session, which is what makes a
    # CloudTrail record attributable to a team.
    actions = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]
    principals {
      type        = "AWS"
      identifiers = var.outerbounds_task_role_arns
    }
  }
}

resource "aws_iam_role" "submitter" {
  name               = "${local.name}-ob-submitter"
  assume_role_policy = data.aws_iam_policy_document.submitter_assume.json

  tags = merge(local.tags, {
    "outerbounds.com/accessible-by-deployment" = var.outerbounds_deployment_tag_value
  })
}

data "aws_iam_policy_document" "submitter_permissions" {
  statement {
    sid = "DiscoverCluster"
    # The driver needs the API endpoint and CA bundle before it can build a
    # kubernetes client. The token itself comes from STS, not from IAM.
    actions   = ["eks:DescribeCluster"]
    resources = [module.eks.cluster_arn]
  }

  statement {
    sid = "PayloadBucketReadWrite"
    # The driver writes spec.json and the code package, and reads the output
    # manifest once the step finishes.
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
    sid = "StreamStepLogs"
    # Driver tails the running step's logs back into the Metaflow UI.
    actions = [
      "logs:GetLogEvents",
      "logs:FilterLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["${aws_cloudwatch_log_group.steps.arn}:*"]
  }

  statement {
    sid       = "StartLiveTail"
    actions   = ["logs:StartLiveTail"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "submitter" {
  name   = "submitter-permissions"
  role   = aws_iam_role.submitter.id
  policy = data.aws_iam_policy_document.submitter_permissions.json
}

# Kubernetes-side authorisation. IAM gets the driver to the API server;
# this entry decides what it may do once there — namespaced edit rights on
# the team namespaces, nothing cluster-wide.
resource "aws_eks_access_entry" "submitter" {
  cluster_name  = module.eks.cluster_name
  principal_arn = aws_iam_role.submitter.arn
  type          = "STANDARD"

  tags = local.tags
}

resource "aws_eks_access_policy_association" "submitter" {
  cluster_name  = module.eks.cluster_name
  principal_arn = aws_iam_role.submitter.arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"

  access_scope {
    type       = "namespace"
    namespaces = keys(var.teams)
  }

  depends_on = [aws_eks_access_entry.submitter]
}

# ---------------------------------------------------------------------------
# Artifact reader — lazy RemoteArtifact loads from Outerbounds pods
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ob_artifact_reader_assume" {
  statement {
    # Same three actions as the submitter, for the same reasons: this role is
    # assumed from the same Outerbounds sessions, so it needs the same
    # permissions to propagate a source identity and session tags.
    actions = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]
    principals {
      type        = "AWS"
      identifiers = var.outerbounds_task_role_arns
    }
  }
}

resource "aws_iam_role" "ob_artifact_reader" {
  name               = "${local.name}-ob-artifact-reader"
  assume_role_policy = data.aws_iam_policy_document.ob_artifact_reader_assume.json

  # Outerbounds discovers cross-account roles by this tag.
  tags = merge(local.tags, {
    "outerbounds.com/accessible-by-deployment" = var.outerbounds_deployment_tag_value
  })
}

data "aws_iam_policy_document" "ob_artifact_reader_permissions" {
  statement {
    # Read-only: a downstream step materialises an artifact, it never writes
    # one back through this path.
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.payload.arn,
      "${aws_s3_bucket.payload.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "ob_artifact_reader" {
  name   = "s3-payload-read"
  role   = aws_iam_role.ob_artifact_reader.id
  policy = data.aws_iam_policy_document.ob_artifact_reader_permissions.json
}
