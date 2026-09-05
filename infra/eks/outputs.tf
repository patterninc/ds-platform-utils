# The `remote_step_config` output is the contract between this Terraform and
# the Python extension. Write it to the package's environments directory:
#
#   terraform output -json remote_step_config \
#     > ../../../ds-platform-utils/src/metaflow_extensions/remote_step/environments/default.json
#
# The extension reads it at flow-init time to resolve the cluster, bucket,
# image and role ARNs — nothing about the target environment is hardcoded in
# the decorator.

output "remote_step_config" {
  description = "JSON block consumed by metaflow_extensions.remote_step.config"

  value = {
    region       = var.region
    cluster_name = module.eks.cluster_name
    # Endpoint is discoverable via eks:DescribeCluster, but shipping it saves
    # the driver an API call on every step.
    cluster_endpoint = module.eks.cluster_endpoint

    payload_bucket = aws_s3_bucket.payload.id
    runner_image   = "${aws_ecr_repository.runner.repository_url}:latest"
    log_group      = aws_cloudwatch_log_group.steps.name

    # Namespace == team name. A step's team picks the namespace it lands in.
    # Every team namespace has a LocalQueue by this name.
    local_queue = "default"
    # ServiceAccount the runner pods run as; bound to the runner IAM role
    # through Pod Identity.
    service_account = "remote-step-runner"

    # Assumed by the Outerbounds driver pod to reach the cluster API.
    submitter_role_arn = aws_iam_role.submitter.arn
    # Baked into every RemoteArtifact ref so a downstream non-@remote_step
    # consumer on an Outerbounds pod can hop into this account to read it.
    artifact_read_role_arn = aws_iam_role.ob_artifact_reader.arn

    teams = keys(var.teams)
  }
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "configure_kubectl" {
  description = "Command to point kubectl at this cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region} --profile ${var.aws_profile}"
}

output "ecr_login" {
  description = "Command to authenticate docker against the runner repository."
  value       = "aws ecr get-login-password --region ${var.region} --profile ${var.aws_profile} | docker login --username AWS --password-stdin ${split("/", aws_ecr_repository.runner.repository_url)[0]}"
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}
