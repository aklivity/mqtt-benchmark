#!/usr/bin/env python3
"""Rates in summary.json come from the clients' own read times and from the slope of the Kafka offset series."""
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def build_run(directory, window_s=722, read_gap_ms=(721000, 721500)):
    os.makedirs(os.path.join(directory, 'metrics'))
    t0 = 1_000_000_000_000
    json.dump({'run_id': 'r', 'mode': 'pubsub', 'window_s': window_s, 'broker': {}, 'broker_cpu': 0, 'broker_mem': 0,
               'kafka_cpu': 0, 'broker_tcp_connections': 0, 'broker_restarts': 0, 'broker_errors': 0, 'connected': 0,
               'subscribed': 0, 'k0': 0, 'k1': 0}, open(os.path.join(directory, 'run-values.json'), 'w'))
    open(os.path.join(directory, 'events.txt'), 'w').write(f'{t0} t0\n{t0 + 720000} t1\n')
    for host, gap in zip(('0', '1'), read_gap_ms):
        for label, ms, count in (('t0', t0, 0), ('t1', t0 + gap, 50000 * gap // 1000)):
            open(os.path.join(directory, 'metrics', f'pub-{host}-{label}.txt'), 'w').write(f'pub {count}\n# read_ms {ms}\n')
    rows = ''.join(f'{t0 + s * 10000}\t{t0 + s * 10000 + 200}\tmqtt-devices\t{p}\t{int((s * 10000 + 100) * 6.25)}\n'
                   for s in range(-2, 75) for p in range(16))
    data = rows.encode()
    with tarfile.open(os.path.join(directory, 'recorder-kafka.tgz'), 'w:gz') as tar:
        info = tarfile.TarInfo('rec/offsets/offsets.tsv')
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


class SummarizeTest(unittest.TestCase):
    def test_client_rate_uses_read_times(self):
        with tempfile.TemporaryDirectory() as d:
            build_run(d)
            scenario = os.path.join(d, 's.env')
            open(scenario, 'w').write('NAME=s\nPUBLISHERS=100000\nSUBSCRIBERS=0\nQOS=1\nPAYLOAD_BYTES=1024\nKAFKA_TOPIC=mqtt-devices\n')
            out = subprocess.run([sys.executable, '-I', os.path.join(HERE, 'summarize.py'), d, scenario], check=True, capture_output=True, text=True)
            summary = json.loads(out.stdout)
            self.assertEqual(summary['sent_msg_rate'], 100000.0)
            self.assertNotEqual(summary['sent_msg_rate_coarse'], summary['sent_msg_rate'])
            self.assertAlmostEqual(summary['kafka_msg_rate'], 100000.0, delta=1500)


if __name__ == '__main__':
    unittest.main()
