#!/usr/bin/env python3
"""EBS and instance-level EBS metrics from CloudWatch for one run, best effort (needs cloudwatch:GetMetricStatistics).

Usage: ebs-cloudwatch.py <out.json> <start_epoch_ms> <end_epoch_ms> <instance-id> [<volume-id>...]
Writes {"volumes": {vol: {metric: [[iso time, stat], ...]}}, "instance": {metric: [...]}} with 60 s periods. CloudWatch publishes
EBS metrics a minute or two late, so the last minutes of a run may be missing when this runs right after it ends.
"""
import datetime
import json
import subprocess
import sys

out, start_ms, end_ms, instance, volumes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5:]
iso = lambda ms: datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
VOLUME = (('VolumeWriteBytes', 'Sum'), ('VolumeReadBytes', 'Sum'), ('VolumeWriteOps', 'Sum'), ('VolumeReadOps', 'Sum'),
          ('VolumeTotalWriteTime', 'Sum'), ('VolumeTotalReadTime', 'Sum'), ('VolumeIdleTime', 'Sum'), ('VolumeQueueLength', 'Average'))
INSTANCE = (('EBSWriteBytes', 'Sum'), ('EBSReadBytes', 'Sum'), ('EBSWriteOps', 'Sum'), ('EBSReadOps', 'Sum'),
            ('EBSIOBalance%', 'Average'), ('EBSByteBalance%', 'Average'))


def metric(namespace, dim, value, name, stat):
    cmd = ['aws', 'cloudwatch', 'get-metric-statistics', '--namespace', namespace, '--metric-name', name, '--dimensions',
           f'Name={dim},Value={value}', '--start-time', iso(start_ms), '--end-time', iso(end_ms), '--period', '60',
           '--statistics', stat, '--output', 'json']
    try:
        points = json.loads(subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=True).stdout)['Datapoints']
        return sorted([p['Timestamp'], p[stat]] for p in points)
    except Exception as ex:   # no permission, no aws cli, throttled: record why and go on
        return f'unavailable: {type(ex).__name__}'


result = {'volumes': {v: {n: metric('AWS/EBS', 'VolumeId', v, n, s) for n, s in VOLUME} for v in volumes},
          'instance': {n: metric('AWS/EC2', 'InstanceId', instance, n, s) for n, s in INSTANCE}}
json.dump(result, open(out, 'w'))
