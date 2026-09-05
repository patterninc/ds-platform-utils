# Step logs.
#
# Runner pods write stdout/stderr here via the CloudWatch agent on the node,
# and the driver tails the stream back into the Metaflow UI with
# logs:StartLiveTail — sub-second delivery once CloudWatch has ingested,
# which is what keeps the Outerbounds log view close to live.

resource "aws_cloudwatch_log_group" "steps" {
  name = "/${local.name}/steps"

  # 30 days. Long enough to debug last month's failed run, short enough that
  # a chatty training loop does not accumulate indefinitely.
  retention_in_days = 30

  tags = local.tags
}
