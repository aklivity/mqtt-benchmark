#!/usr/bin/env python3
"""Builds results/<run-id>/summary.json from what bin/run-scenario.sh collected.

Inputs in the run directory: run-values.json, metrics/{pub,sub}-<host>-{t0,t1}.txt (emqtt-bench Prometheus pages
at the start and end of the measurement window), puback-probe.json (optional). The scenario file gives the targets.
Usage: summarize.py <run dir> <scenario .env>
"""
import glob, json, os, re, sys

run_dir, scenario_file = sys.argv[1], sys.argv[2]
values = json.load(open(os.path.join(run_dir, 'run-values.json')))

scenario = {}


def read_scenario(path):
    """KEY=VALUE lines of a scenario file, following `source <file>` (paths relative to the repo root)"""
    for line in open(path):
        src = re.match(r"\s*(?:source|\.)\s+(\S+)", line)
        if src:
            read_scenario(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', src.group(1)))
            continue
        m = re.match(r"\s*([A-Z_][A-Z0-9_]*)=('([^']*)'|\"([^\"]*)\"|([^#\s]*))", line)
        if m:
            scenario[m.group(1)] = next(g for g in m.groups()[2:] if g is not None)


read_scenario(scenario_file)
num = lambda k, d=0: float(scenario.get(k, d) or d)


def parse(path):
    """Prometheus text page -> {metric name with labels: value}"""
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        if line.startswith('#') or not line.strip():
            continue
        name, _, val = line.rstrip().rpartition(' ')
        # exact integers where possible: emqtt-bench's e2e_latency_sum can sit near -1.4e20, where a float can't
        # represent the few-thousand-ms difference between two snapshots
        try:
            out[name] = int(val)
        except ValueError:
            try:
                out[name] = float(val)
            except ValueError:
                pass
    return out


def totals(role, label):
    """Sum the pages of every client host for one role and snapshot."""
    acc = {}
    for path in glob.glob(os.path.join(run_dir, 'metrics', f'{role}-*-{label}.txt')):
        for k, v in parse(path).items():
            acc[k] = acc.get(k, 0) + v
    return acc


def delta(role, name):
    return totals(role, 't1').get(name, 0) - totals(role, 't0').get(name, 0)


def histogram(role, base, use_delta=True):
    """avg and bucket upper bounds for p50/p90/p99 of an emqtt-bench histogram (ms)."""
    t1, t0 = totals(role, 't1'), (totals(role, 't0') if use_delta else {})
    get = lambda k: t1.get(k, 0) - t0.get(k, 0)
    count, total = get(f'{base}_count'), get(f'{base}_sum')
    buckets = sorted(((float('inf') if le == '+Inf' else float(le), get(k))
                      for k in t1 for le in re.findall(rf'^{base}_bucket{{le="([^"]+)"}}$', k)))
    if not count:
        return None

    def pct(q):
        for le, c in buckets:
            if c >= q * count:
                return None if le == float('inf') else le
    return {'count': int(count), 'avg_ms': round(total / count, 2),
            'p50_le_ms': pct(.5), 'p90_le_ms': pct(.9), 'p99_le_ms': pct(.99)}


def n(v):
    return None if v in (None, '', 'null') else round(float(v), 2)


window = values['window_s']
summary = {'run_id': values['run_id'], 'scenario': scenario.get('NAME'), 'mode': values['mode'], 'window_s': window}
summary['broker'] = values['broker']
host = {'broker_cpu_pct_avg': n(values['broker_cpu']), 'broker_mem_used_gib_avg': n(values['broker_mem']),
        'kafka_cpu_pct_avg': n(values['kafka_cpu']), 'broker_tcp_connections_at_end': n(values['broker_tcp_connections']),
        'broker_state_at_end': values.get('broker_state'), 'broker_restarts': values['broker_restarts'],
        # Zilla: AgentTerminationException lines; EMQX: log lines at error level or above
        'broker_errors_logged': values['broker_errors']}

if values['mode'] == 'conn':
    target = int(num('CONNECTIONS'))
    t1 = totals('pub', 't1')
    summary['connections'] = {
        'target': target, 'conn_rate_target': num('CONN_RATE'),
        'connected': int(t1.get('connect_succ', 0)), 'connect_fail': int(t1.get('connect_fail', 0)),
        'success_pct': round(100 * t1.get('connect_succ', 0) / target, 3) if target else None,
        'seconds_to_connect_all': values.get('connect_s'),
        'connect_rate_achieved': round(t1.get('connect_succ', 0) / values['connect_s'], 1) if values.get('connect_s') else None,
        'held_at_end_broker': n(values['broker_tcp_connections']),
        # emqtt-bench's own histograms, under their own names. Neither is a clean CONNECT -> CONNACK time:
        # handshake_duration read 0.07 ms on AWS (below a network round trip), and connect_duration includes
        # emqtt-bench's pacing at the connect rate.
        'emqtt_bench_handshake_duration': histogram('pub', 'mqtt_client_handshake_duration', use_delta=False),
        'emqtt_bench_connect_duration': histogram('pub', 'mqtt_client_connect_duration', use_delta=False),
    }
else:
    try:
        probe = json.load(open(os.path.join(run_dir, 'puback-probe.json')))
    except Exception:
        probe = {}
    probe_msgs = probe.get('acked', 0) or 0
    kafka_records = values['k1'] - values['k0'] - probe_msgs
    deliveries = num('DELIVERIES_PER_MSG', 1)
    recv = delta('sub', 'recv')
    sent = delta('pub', 'pub')
    summary.update({
        'target': {'publishers': int(num('PUBLISHERS')), 'subscribers': int(num('SUBSCRIBERS')), 'qos': int(num('QOS')),
                   'sub_qos': int(num('SUB_QOS', 1)), 'payload_bytes': int(num('PAYLOAD_BYTES')),
                   'pub_msg_rate': round(num('PUBLISHERS') * 1000 / num('PUBLISH_INTERVAL_MS', 1000)),
                   'sub_msg_rate': round(num('PUBLISHERS') * 1000 / num('PUBLISH_INTERVAL_MS', 1000) * deliveries)
                   if num('SUBSCRIBERS') else 0},
        'connections': {'publishers_connected': values['connected'], 'subscribers_subscribed': values['subscribed'],
                        'pub_connect_fail': int(totals('pub', 't1').get('connect_fail', 0)),
                        'sub_connect_fail': int(totals('sub', 't1').get('connect_fail', 0)),
                        'sub_fail': int(totals('sub', 't1').get('sub_fail', 0))},
        'sent_msg_rate': round(delta('pub', 'pub') / window, 1),
        'send_failures': int(delta('pub', 'pub_fail')),
        'kafka_msg_rate': round(kafka_records / window, 1),
        'received_msg_rate': round(recv / window, 1),
        # received vs what the publishers sent in the same window, times subscribers per message (1,000 for fan-out)
        'delivery_pct': round(100 * recv / (sent * deliveries), 2) if sent and num('SUBSCRIBERS') else None,
        'e2e_latency': histogram('sub', 'e2e_latency'),
        'publish_response_ms': {k: probe.get(k) for k in ('avg_ms', 'p50_ms', 'p90_ms', 'p99_ms', 'p999_ms', 'max_ms')} if probe else None,
        'probe': {k: probe.get(k) for k in ('connections', 'sent', 'acked', 'timed_out', 'connect_failed', 'disconnected')} if probe else None,
    })



def emqx_action(label):
    try:
        return json.load(open(os.path.join(run_dir, 'metrics', f'emqx-action-{label}.json')))['metrics']
    except Exception:
        return None


# EMQX only: its Kafka action's own counters over the window, as EMQ compared them with the Kafka offsets
a0, a1 = emqx_action('t0'), emqx_action('t1')
if a0 and a1:
    summary['emqx_kafka_action'] = {
        **{f'{k}_rate': round((a1.get(k, 0) - a0.get(k, 0)) / window, 1) for k in ('matched', 'success', 'failed', 'dropped')},
        'queuing_at_end': a1.get('queuing'), 'inflight_at_end': a1.get('inflight'),
        'dropped_total': a1.get('dropped'), 'failed_total': a1.get('failed')}

summary['hosts'] = host
print(json.dumps(summary, indent=2))
