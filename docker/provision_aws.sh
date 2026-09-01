#!/usr/bin/env bash
#
# Create everything @remote_step needs in an AWS account.
#
# Written because the sandbox account gets wiped periodically and rebuilding it by hand loses an
# afternoon -- twice now. Every step is idempotent, so running it against a partly-provisioned
# account fills the gaps rather than failing.
#
#   AWS_PROFILE=sandbox ./docker/provision_aws.sh
#
# What it cannot do is create IAM roles where you lack iam:CreateRole -- data-science-prod is the
# case in point. There the roles are a platform request; see EXTERNAL_COMPUTE.md for the policies.
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="sagemaker-${REGION}-${ACCOUNT}"
# The Outerbounds task role that Metaflow pods run as. It may assume any role tagged for the
# deployment, which is how a pod reaches this account at all.
POD_ROLE="${POD_ROLE:-arn:aws:iam::209479263910:role/obp-5p6le9-task}"

say() { printf '  %s\n' "$*"; }

# Trust policies carry sts:SetSourceIdentity alongside sts:AssumeRole. Outerbounds stamps a source
# identity onto the pod's credentials and it propagates through every assumption in the chain, so a
# role missing it fails with "Could not assume role" -- twice, at two different links, before this
# was understood.
ecs_trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":["sts:AssumeRole","sts:SetSourceIdentity"]}]}'
sagemaker_trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sagemaker.amazonaws.com"},"Action":["sts:AssumeRole","sts:SetSourceIdentity"]}]}'
pod_trust="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"${POD_ROLE}\"},\"Action\":[\"sts:AssumeRole\",\"sts:SetSourceIdentity\"]}]}"

ensure_role() {  # name, trust
    if aws iam get-role --role-name "$1" >/dev/null 2>&1; then
        aws iam update-assume-role-policy --role-name "$1" --policy-document "$2"
        say "role $1 (trust refreshed)"
    else
        aws iam create-role --role-name "$1" --assume-role-policy-document "$2" >/dev/null
        say "role $1 created"
    fi
}

say "account ${ACCOUNT}, region ${REGION}"

# --- payload bucket --------------------------------------------------------------------------
# The sagemaker-* name is load-bearing: AmazonSageMakerFullAccess grants S3 on *sagemaker* and
# GetSecretValue on AmazonSageMaker-*, so the execution role needs no policy of its own.
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    say "bucket $BUCKET exists"
else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
        --create-bucket-configuration "LocationConstraint=${REGION}" >/dev/null
    say "bucket $BUCKET created"
fi

# --- container image -------------------------------------------------------------------------
aws ecr describe-repositories --repository-names remote-step-runtime >/dev/null 2>&1 \
    || { aws ecr create-repository --repository-name remote-step-runtime >/dev/null; say "ecr repo created"; }
say "ecr repo remote-step-runtime ready (build and push separately)"

# --- SageMaker -------------------------------------------------------------------------------
ensure_role RemoteStepSageMakerExecutionRole "$sagemaker_trust"
aws iam attach-role-policy --role-name RemoteStepSageMakerExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonSageMakerFullAccess
say "  + AmazonSageMakerFullAccess"

# --- Batch on Fargate Spot -------------------------------------------------------------------
ensure_role RemoteStepBatchExecutionRole "$ecs_trust"
aws iam attach-role-policy --role-name RemoteStepBatchExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
say "  + AmazonECSTaskExecutionRolePolicy"

ensure_role RemoteStepBatchJobRole "$ecs_trust"
aws iam put-role-policy --role-name RemoteStepBatchJobRole --policy-name RemoteStepBatchJob \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\"],
     \"Resource\":[\"arn:aws:s3:::${BUCKET}\",\"arn:aws:s3:::${BUCKET}/*\"]},
    {\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:GetSecretValue\"],
     \"Resource\":\"arn:aws:secretsmanager:${REGION}:${ACCOUNT}:secret:AmazonSageMaker-remote-step-*\"}]}"
say "  + S3 and secret access"

# --- the role a Metaflow pod assumes to reach this account ------------------------------------
ensure_role RemoteStepSubmitterRole "$pod_trust"
# Without this tag every assume is denied: the pod's own policy allows sts:AssumeRole only on
# roles carrying it. It is the mechanism Outerbounds provides, and needs nothing from them.
aws iam tag-role --role-name RemoteStepSubmitterRole \
    --tags Key=outerbounds.com/accessible-by-deployment,Value=pattern
say "  + accessible-by-deployment tag"
aws iam put-role-policy --role-name RemoteStepSubmitterRole --policy-name RemoteStepSubmit \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
    {\"Sid\":\"SageMakerJobs\",\"Effect\":\"Allow\",\"Action\":[\"sagemaker:CreateTrainingJob\",
      \"sagemaker:DescribeTrainingJob\",\"sagemaker:StopTrainingJob\",\"sagemaker:CreateProcessingJob\",
      \"sagemaker:DescribeProcessingJob\",\"sagemaker:AddTags\"],\"Resource\":\"*\"},
    {\"Sid\":\"BatchJobs\",\"Effect\":\"Allow\",\"Action\":[\"batch:SubmitJob\",\"batch:DescribeJobs\",
      \"batch:TerminateJob\"],\"Resource\":\"*\"},
    {\"Sid\":\"PassSageMakerRole\",\"Effect\":\"Allow\",\"Action\":\"iam:PassRole\",
     \"Resource\":\"arn:aws:iam::${ACCOUNT}:role/RemoteStepSageMakerExecutionRole\",
     \"Condition\":{\"StringEquals\":{\"iam:PassedToService\":\"sagemaker.amazonaws.com\"}}},
    {\"Sid\":\"PassBatchRoles\",\"Effect\":\"Allow\",\"Action\":\"iam:PassRole\",
     \"Resource\":[\"arn:aws:iam::${ACCOUNT}:role/RemoteStepBatchExecutionRole\",
                   \"arn:aws:iam::${ACCOUNT}:role/RemoteStepBatchJobRole\"],
     \"Condition\":{\"StringEquals\":{\"iam:PassedToService\":\"ecs-tasks.amazonaws.com\"}}},
    {\"Sid\":\"StagePayloads\",\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\",
      \"s3:ListBucket\",\"s3:DeleteObject\"],
     \"Resource\":[\"arn:aws:s3:::${BUCKET}\",\"arn:aws:s3:::${BUCKET}/*\"]},
    {\"Sid\":\"ReadJobLogs\",\"Effect\":\"Allow\",\"Action\":[\"logs:DescribeLogStreams\",
      \"logs:GetLogEvents\"],
     \"Resource\":[\"arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/sagemaker/*\",
                   \"arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/batch/*\"]},
    {\"Sid\":\"EphemeralSnowflakeSecret\",\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:CreateSecret\",
      \"secretsmanager:DeleteSecret\"],
     \"Resource\":\"arn:aws:secretsmanager:${REGION}:${ACCOUNT}:secret:AmazonSageMaker-remote-step-*\"}]}"
say "  + submit permissions"

# --- networking for Fargate --------------------------------------------------------------------
VPC="$(aws ec2 describe-vpcs --query 'Vpcs[0].VpcId' --output text)"
SUBNETS="$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=${VPC}" \
    --query 'Subnets[?MapPublicIpOnLaunch==`true`].SubnetId' --output text | tr '\t' ',')"

# A dedicated security group, not the default one. The shared default had its egress rule stripped,
# so tasks could not reach ECR and failed with ResourceInitializationError -- and loosening a
# shared, deliberately-locked-down group to fix our own job would be the wrong repair.
SG="$(aws ec2 describe-security-groups --filters "Name=group-name,Values=remote-step-batch" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [ -z "$SG" ] || [ "$SG" = "None" ]; then
    SG="$(aws ec2 create-security-group --group-name remote-step-batch \
        --description "Egress for remote_step Batch jobs (ECR, S3, Secrets Manager)" \
        --vpc-id "$VPC" --query GroupId --output text)"
    # A new security group already carries an allow-all egress rule, so this is usually a no-op
    # and AWS rejects it as a duplicate. Added anyway for the case where someone has stripped it --
    # which is exactly what happened to the shared default group, and why we use our own.
    aws ec2 authorize-security-group-egress --group-id "$SG" \
        --ip-permissions 'IpProtocol=-1,IpRanges=[{CidrIp=0.0.0.0/0}]' >/dev/null 2>&1 || true
fi
say "security group $SG in $VPC"

aws iam create-service-linked-role --aws-service-name batch.amazonaws.com >/dev/null 2>&1 || true

if aws batch describe-compute-environments --compute-environments remote-step-fargate-spot \
     --query 'computeEnvironments[0].computeEnvironmentArn' --output text 2>/dev/null | grep -q arn; then
    say "compute environment exists"
else
    aws batch create-compute-environment --compute-environment-name remote-step-fargate-spot \
        --type MANAGED --state ENABLED \
        --compute-resources "type=FARGATE_SPOT,maxvCpus=64,subnets=${SUBNETS},securityGroupIds=${SG}" >/dev/null
    say "compute environment creating..."
    for _ in $(seq 1 40); do
        [ "$(aws batch describe-compute-environments --compute-environments remote-step-fargate-spot \
            --query 'computeEnvironments[0].status' --output text)" = "VALID" ] && break
        sleep 5
    done
    say "compute environment VALID"
fi

aws batch describe-job-queues --job-queues remote-step-spot \
    --query 'jobQueues[0].jobQueueArn' --output text 2>/dev/null | grep -q arn \
  || { aws batch create-job-queue --job-queue-name remote-step-spot --state ENABLED --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=remote-step-fargate-spot" >/dev/null
       say "job queue created"; }

# One job definition for every step. Sizing, command and environment are container overrides at
# submit time, so no revision accumulates -- and the image cannot be overridden per job, which is
# why it is pinned here.
aws batch register-job-definition --job-definition-name remote-step --type container \
  --platform-capabilities FARGATE \
  --container-properties "{
    \"image\": \"${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/remote-step-runtime:py311\",
    \"executionRoleArn\": \"arn:aws:iam::${ACCOUNT}:role/RemoteStepBatchExecutionRole\",
    \"jobRoleArn\": \"arn:aws:iam::${ACCOUNT}:role/RemoteStepBatchJobRole\",
    \"resourceRequirements\": [{\"type\":\"VCPU\",\"value\":\"1\"},{\"type\":\"MEMORY\",\"value\":\"2048\"}],
    \"networkConfiguration\": {\"assignPublicIp\": \"ENABLED\"},
    \"runtimePlatform\": {\"cpuArchitecture\":\"X86_64\",\"operatingSystemFamily\":\"LINUX\"},
    \"command\": [\"python3\",\"-u\",\"-c\",\"print('overridden at submit time')\"]}" \
  --query '[jobDefinitionName,revision]' --output text | sed 's/^/  job definition /'

say "done. Build and push the image next:"
say "  docker buildx build --platform linux/amd64 --build-arg DS_PLATFORM_UTILS_REF=<ref> \\"
say "    -t ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/remote-step-runtime:py311 --push docker/"
