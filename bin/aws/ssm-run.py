#!/usr/bin/env python3
"""Runs a shell command as ubuntu on the controller through Systems Manager Run Command (HTTPS only, no SSH).

Usage: bin/aws/ssm-run.py [--instance-id i-...] [--bucket B] [--region R] [--timeout SECONDS] 'command'
  CONTROLLER_ID, BUCKET and AWS_REGION are the defaults for the first three; credentials come from boto3's usual
  chain (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).

Prints the command's stdout and stderr and exits with its exit code (124 on timeout, 130 if cancelled). The
inline output of a command is cut at 24,000 characters, so output goes to the bucket as well and is read back from
there when it may have been cut.
"""
import argparse
import os
import shlex
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

OUTPUT_PREFIX = "ssm-output"
# Run Command cuts inline output at 24,000 characters; the bucket copy is read from well below that, so a cut is never missed
READ_BACK_AT = 20000
TERMINAL = {"Success", "Failed", "Cancelled", "TimedOut"}
# Slack on top of --timeout for the agent to pick the command up before the client gives up waiting
START_GRACE_S = 300
MAX_EXECUTION_TIMEOUT_S = 172800
S3_RETRIES = 12


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Run a shell command as ubuntu on the controller via SSM.")
    parser.add_argument("command", help="shell command, run with bash -c as user ubuntu")
    parser.add_argument("--instance-id", default=os.environ.get("CONTROLLER_ID"), help="default: $CONTROLLER_ID")
    parser.add_argument("--bucket", default=os.environ.get("BUCKET"), help="output bucket, default: $BUCKET")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION"), help="default: $AWS_REGION")
    parser.add_argument("--timeout", type=int, default=7200, help="seconds the command may run (default 7200)")
    parser.add_argument("--poll", type=int, default=5, help="seconds between status checks (default 5)")
    args = parser.parse_args(argv)
    for option, value in (("--instance-id/CONTROLLER_ID", args.instance_id), ("--bucket/BUCKET", args.bucket),
                          ("--region/AWS_REGION", args.region)):
        if not value:
            parser.error(f"{option} is required")
    if not 30 <= args.timeout <= MAX_EXECUTION_TIMEOUT_S:
        parser.error(f"--timeout must be between 30 and {MAX_EXECUTION_TIMEOUT_S} seconds")
    return args


def send(ssm, args):
    # AWS-RunShellScript runs as root; both timeouts default to minutes-to-an-hour and would cut a long run:
    # TimeoutSeconds bounds delivery of the command, executionTimeout its run.
    response = ssm.send_command(
        InstanceIds=[args.instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": ["exec sudo -u ubuntu -H bash -c " + shlex.quote(args.command)],
            "executionTimeout": [str(args.timeout)],
        },
        TimeoutSeconds=args.timeout,
        OutputS3BucketName=args.bucket,
        OutputS3KeyPrefix=OUTPUT_PREFIX,
        Comment="mqtt-benchmark ssm-run",
    )
    return response["Command"]["CommandId"]


def wait(ssm, command_id, args, clock, sleep):
    deadline = clock() + args.timeout + START_GRACE_S
    started = clock()
    next_note = started + 60
    invocation = None
    while invocation is None:
        try:
            current = ssm.get_command_invocation(CommandId=command_id, InstanceId=args.instance_id)
        except ClientError as error:
            # the invocation is not visible for a moment after send_command
            if error.response["Error"]["Code"] != "InvocationDoesNotExist":
                raise
            current = None
        if current is not None and current["Status"] in TERMINAL:
            invocation = current
        elif clock() >= deadline:
            break
        else:
            if clock() >= next_note:
                print(f"[ssm-run] still running after {int(clock() - started)} s", file=sys.stderr, flush=True)
                next_note += 60
            sleep(args.poll)
    return invocation


def read_output_object(s3, bucket, command_id, name, sleep):
    prefix = f"{OUTPUT_PREFIX}/{command_id}/"
    text = None
    for attempt in range(S3_RETRIES):
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys += [item["Key"] for item in page.get("Contents", []) if item["Key"].endswith("/" + name)]
        if keys:
            text = s3.get_object(Bucket=bucket, Key=keys[0])["Body"].read().decode("utf-8", errors="replace")
            break
        if attempt + 1 < S3_RETRIES:
            sleep(5)
    return text


def full_output(invocation, s3, bucket, command_id, sleep):
    result = []
    for field, name in (("StandardOutputContent", "stdout"), ("StandardErrorContent", "stderr")):
        inline = invocation.get(field, "")
        if len(inline) < READ_BACK_AT:
            result.append(inline)
        else:
            stored = read_output_object(s3, bucket, command_id, name, sleep)
            if stored is None:
                print(f"[ssm-run] {name} may be cut: the copy in s3://{bucket}/{OUTPUT_PREFIX}/{command_id}/ was not found",
                      file=sys.stderr)
            result.append(inline if stored is None else stored)
    return result


def exit_code(invocation):
    status = invocation["Status"]
    code = invocation.get("ResponseCode", -1)
    if status == "Success":
        result = 0
    elif status == "Failed":
        result = code if code > 0 else 1
    elif status == "TimedOut":
        result = 124
    else:
        result = 130
    return result


def main(argv, ssm=None, s3=None, clock=time.monotonic, sleep=time.sleep):
    args = parse_args(argv)
    config = Config(retries={"max_attempts": 10, "mode": "standard"})
    ssm = ssm or boto3.client("ssm", region_name=args.region, config=config)
    s3 = s3 or boto3.client("s3", region_name=args.region, config=config)
    command_id = send(ssm, args)
    print(f"[ssm-run] command {command_id} on {args.instance_id}", file=sys.stderr, flush=True)
    invocation = wait(ssm, command_id, args, clock, sleep)
    if invocation is None:
        print(f"[ssm-run] no result after {args.timeout + START_GRACE_S} s, cancelling {command_id}", file=sys.stderr)
        try:
            ssm.cancel_command(CommandId=command_id, InstanceIds=[args.instance_id])
        except ClientError as error:
            print(f"[ssm-run] cancel failed: {error}", file=sys.stderr)
        result = 124
    else:
        out, err = full_output(invocation, s3, args.bucket, command_id, sleep)
        sys.stdout.write(out)
        sys.stdout.flush()
        sys.stderr.write(err)
        sys.stderr.flush()
        if invocation["Status"] != "Success":
            print(f"[ssm-run] status {invocation['Status']}", file=sys.stderr)
        result = exit_code(invocation)
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
