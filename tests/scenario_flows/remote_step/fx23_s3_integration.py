"""Can a @remote_step body reach an Outerbounds S3 integration bucket?

Read-only on purpose: this touches a production bucket, so it lists and heads
but never writes.

Before the fix both sides denied it -- the integration role did not trust the
runner, and the runner had no sts:AssumeRole permission at all.
"""

from metaflow import FlowSpec, remote_step, resources, step

ROLE = "arn:aws:iam::209479263910:role/ob-demand-forecast-models"
BUCKET = "pattern-demand-forecast-models"


class S3IntegFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.reach_integration)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def reach_integration(self):
        import boto3

        who = boto3.client("sts").get_caller_identity()
        print(f"[flow] pod identity: {who['Arn']}")

        creds = boto3.client("sts").assume_role(
            RoleArn=ROLE, RoleSessionName="remote-step-integration-check"
        )["Credentials"]
        print("[flow] assumed the integration role")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
        resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=5)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        print(f"[flow] listed {BUCKET}: {resp.get('KeyCount', 0)} key(s)")
        for k in keys:
            print(f"[flow]   {k}")
        self.reached = True
        self.keys_seen = len(keys)
        self.next(self.end)

    @step
    def end(self):
        print(f"[flow] integration reachable={self.reached} keys={self.keys_seen}")


if __name__ == "__main__":
    S3IntegFlow()
