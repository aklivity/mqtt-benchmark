#!/usr/bin/env python3
"""Publish response time probe: time from PUBLISH until the broker completes it, on a sample of connections.

emqtt-bench does not report publish latency (its publish_latency counter stays at 0), so the benchmark runs this
probe next to the load. Each probe connection publishes once per second (like a benchmark client) and times every
publish until the broker completes it:
  QoS 1  PUBLISH -> PUBACK
  QoS 2  PUBLISH -> PUBREC, PUBREL -> PUBCOMP (the whole exchange; the time to PUBREC is reported too)
Each connection has one publish in flight at a time, like emqtt-bench with INFLIGHT=1. No dependencies; MQTT 3.1.1
over plain TCP.

Usage: puback-probe.py <host> <port> <connections> <duration_s> <run_id> [payload_bytes] [qos]
Writes a JSON summary to stdout: qos, sent, acked (completed), timed_out, connect_failed, disconnected,
avg/p50/p90/p99/p999/max in ms, and for QoS 2 the same percentiles up to PUBREC under "pubrec".
"""
import asyncio, json, struct, sys, time

HOST, PORT, CONNS, DURATION, RUN = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), sys.argv[5]
PAYLOAD = b'x' * (int(sys.argv[6]) if len(sys.argv) > 6 else 1024)
QOS = int(sys.argv[7]) if len(sys.argv) > 7 else 1
assert QOS in (1, 2), 'qos must be 1 or 2'
ACK_TIMEOUT = 10.0   # for the whole exchange
PUBACK, PUBREC, PUBCOMP = 4, 5, 7
samples, pubrec_samples = [], []
stats = {'sent': 0, 'acked': 0, 'timed_out': 0, 'connect_failed': 0, 'disconnected': 0}


def varint(n):
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def utf8(s):
    b = s.encode()
    return struct.pack('>H', len(b)) + b


def packet(first, body):
    return bytes([first]) + varint(len(body)) + body


async def read_packet(reader):
    first = (await reader.readexactly(1))[0]
    mult, n = 1, 0
    while True:
        b = (await reader.readexactly(1))[0]
        n += (b & 0x7f) * mult
        mult *= 128
        if not b & 0x80:
            break
    return first, await reader.readexactly(n)


async def expect(reader, ptype, pid, deadline):
    """Read until a packet of type ptype for packet ID pid arrives; anything else (PINGRESP etc.) is skipped."""
    while True:
        first, body = await asyncio.wait_for(read_packet(reader), max(0.0, deadline - time.monotonic()))
        if first >> 4 == ptype and struct.unpack('>H', body[:2])[0] == pid:
            return


async def client(i, start_at):
    await asyncio.sleep(max(0.0, start_at - time.monotonic()))
    cid = f'{RUN}-probe-{i}'
    topic = f'device/{RUN}-probe-{i}'
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(HOST, PORT), 10)
        connect = utf8('MQTT') + bytes([4, 0x02]) + struct.pack('>H', 300) + utf8(cid)   # clean session, keepalive 300
        writer.write(packet(0x10, connect))
        first, body = await asyncio.wait_for(read_packet(reader), 10)
        if first >> 4 != 2 or body[1] != 0:
            raise RuntimeError(f'CONNACK {body[1] if len(body) > 1 else "?"}')
    except Exception:
        stats['connect_failed'] += 1
        return
    end = time.monotonic() + DURATION
    pid = 0
    try:
        while time.monotonic() < end:
            tick = time.monotonic()
            deadline = tick + ACK_TIMEOUT
            pid = pid % 65535 + 1
            writer.write(packet(0x30 | QOS << 1, utf8(topic) + struct.pack('>H', pid) + PAYLOAD))
            await writer.drain()
            stats['sent'] += 1
            try:
                if QOS == 1:
                    await expect(reader, PUBACK, pid, deadline)
                else:
                    await expect(reader, PUBREC, pid, deadline)
                    pubrec_samples.append((time.monotonic() - tick) * 1000)
                    writer.write(packet(0x62, struct.pack('>H', pid)))                       # PUBREL
                    await writer.drain()
                    await expect(reader, PUBCOMP, pid, deadline)
                samples.append((time.monotonic() - tick) * 1000)
                stats['acked'] += 1
            except asyncio.TimeoutError:
                # The exchange is abandoned. A late PUBACK/PUBREC/PUBCOMP for this packet ID is skipped by expect(),
                # since the next publish uses the next packet ID.
                stats['timed_out'] += 1
            await asyncio.sleep(max(0.0, 1.0 - (time.monotonic() - tick)))
        writer.write(packet(0xE0, b''))
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        stats['disconnected'] += 1
    finally:
        writer.close()


def pct(sorted_ms, q):
    return round(sorted_ms[min(len(sorted_ms) - 1, int(q * len(sorted_ms)))], 2) if sorted_ms else None


def percentiles(ms):
    s = sorted(ms)
    return dict(avg_ms=round(sum(s) / len(s), 2) if s else None,
                p50_ms=pct(s, .50), p90_ms=pct(s, .90), p99_ms=pct(s, .99), p999_ms=pct(s, .999),
                max_ms=round(s[-1], 2) if s else None)


async def main():
    t0 = time.monotonic()
    # spread the probe connections over the first second so their publishes don't all coincide
    await asyncio.gather(*(client(i, t0 + i / max(CONNS, 1)) for i in range(CONNS)))
    out = dict(stats, qos=QOS, connections=CONNS, duration_s=DURATION, **percentiles(samples))
    if QOS == 2:
        out['pubrec'] = percentiles(pubrec_samples)
    print(json.dumps(out))


asyncio.run(main())
