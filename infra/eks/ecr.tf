# Runner image repository.
#
# One multi-arch image (linux/amd64 + linux/arm64) serves every team and both
# NodePool architectures — the pod's nodeSelector decides which variant gets
# pulled. See container/build_and_push.sh.

resource "aws_ecr_repository" "runner" {
  name = "${local.name}-runner"

  # MUTABLE so `:latest` can be re-pointed on each build. Production pinning
  # happens at the config level (runner_image carries an immutable
  # YYYYMMDD-HHMMSS tag) rather than by locking the repository.
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = local.tags
}

resource "aws_ecr_lifecycle_policy" "runner" {
  repository = aws_ecr_repository.runner.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep the 20 most recent images; older builds are recoverable from git."
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = { type = "expire" }
      },
    ]
  })
}

# The Karpenter node role needs to pull from this repository. The managed
# AmazonEC2ContainerRegistryReadOnly policy the module attaches covers it,
# so no repository policy is required for same-account pulls.
