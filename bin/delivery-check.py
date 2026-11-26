#!/usr/bin/env python3
"""Per-client delivery check: which subscriber received which messages, and which connections the broker closed.

emqtt-bench reports only totals per host, so a subscription that never receives is invisible in them. This tool runs
one point-to-point test with its own clients and accounts for every one of them:
  1. COUNT subscribers connect at CONN_RATE per second, each subscribes to <prefix>/<i> and records the SUBACK result.
  2. COUNT publishers connect at CONN_RATE per second, and each publishes MESSAGES messages to <prefix>/<i>, one per second,
     starting as soon as it is connected (like emqtt-bench with -w false). Each publish waits for its acknowledgement.
  3. After the last publish it waits GRACE seconds, then reports.
Every connection that ends before the test does is recorded with the time since the start and what it was doing.

Usage: delivery-check.py <host> <port> <count> [--qos 1] [--version 4|5] [--clean true|false] [--session-expiry 3600]
                         [--messages 20] [--rate 500] [--prefix device] [--grace 5] [--run-id delivery]
Prints a JSON report: totals, the subscribers that received fewer messages than were published to them (with the number
received), failed SUBACKs, connections that closed early (kind, index, seconds since start, phase) and publishes without
acknowledgement. No dependencies; plain TCP, MQTT 3.1.1 or 5, QoS 0 or 1.
"""
import argparse
import asyncio
import json
import struct
import time

parser = argparse.ArgumentParser()
parser.add_argument('host')
parser.add_argument('port', type=int)
parser.add_argument('count', type=int)
parser.add_argument('--qos', type=int, default=1, choices=(0, 1))
parser.add_argument('--version', type=int, default=4, choices=(4, 5))
parser.add_argument('--clean', default='true')
parser.add_argument('--session-expiry', type=int, default=0)
parser.add_argument('--messages', type=int, default=20)
parser.add_argument('--rate', type=float, default=500.0)
parser.add_argument('--prefix', default='device')
parser.add_argument('--grace', type=float, default=5.0)
parser.add_argument('--run-id', default='delivery')
args = parser.parse_args()

CONNACK, PUBLISH, PUBACK, SUBACK = 2, 3, 4, 9
start = time.time()
received = {}          # subscriber index -> messages received
suback = {}            # subscriber index -> return codes
early_close = []       # connections that ended before the test did
unacked = []           # publishes without a PUBACK in time
connect_failed = []
done = False


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


def connect_packet(client_id):
    flags = 0x02 if args.clean == 'true' else 0x00
    props = b''
    if args.version == 5:
        props = b'\x11' + struct.pack('>I', args.session_expiry) if args.session_expiry else b''
        props = varint(len(props)) + props
    body = utf8('MQTT') + bytes([args.version, flags]) + struct.pack('>H', 300) + props + utf8(client_id)
    return packet(0x10, body)


def skip_properties(body, pos):
    n, mult = 0, 1
    while True:
        b = body[pos]
        pos += 1
        n += (b & 0x7f) * mult
        mult *= 128
        if not b & 0x80:
            break
    return pos + n


async def connect(kind, i):
    client_id = f'{args.run_id}-{kind}-{i}'
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(args.host, args.port), 10)
        writer.write(connect_packet(client_id))
        first, body = await asyncio.wait_for(read_packet(reader), 10)
        if first >> 4 != CONNACK or body[1] != 0:
            raise ConnectionError(f'connack {first >> 4} rc={body[1] if len(body) > 1 else None}')
    except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError) as e:
        connect_failed.append({'kind': kind, 'index': i, 'error': type(e).__name__ + ' ' + str(e)})
        return None
    return reader, writer


def closed(kind, i, phase, e):
    if not done:
        early_close.append({'kind': kind, 'index': i, 'at_s': round(time.time() - start, 2), 'phase': phase,
                            'error': type(e).__name__})


async def subscriber(i):
    conn = await connect('sub', i)
    if not conn:
        return
    reader, writer = conn
    received[i] = 0
    phase = 'subscribe'
    try:
        props = b'\x00' if args.version == 5 else b''
        writer.write(packet(0x82, struct.pack('>H', 1) + props + utf8(f'{args.prefix}/{i}') + bytes([args.qos])))
        while True:
            first, body = await read_packet(reader)
            ptype = first >> 4
            if ptype == SUBACK:
                pos = 2 + (skip_properties(body, 2) - 2 if args.version == 5 else 0)
                suback[i] = list(body[pos:])
                phase = 'receive'
            elif ptype == PUBLISH:
                received[i] += 1
                if (first >> 1) & 3:
                    tl = struct.unpack('>H', body[:2])[0]
                    writer.write(packet(0x40, body[2 + tl:4 + tl]))
    except (OSError, asyncio.IncompleteReadError, asyncio.CancelledError) as e:
        if not isinstance(e, asyncio.CancelledError):
            closed('sub', i, phase, e)


async def publisher(i):
    conn = await connect('pub', i)
    if not conn:
        return
    reader, writer = conn
    phase = 'publish'
    try:
        for n in range(args.messages):
            topic = utf8(f'{args.prefix}/{i}')
            pid = n % 65535 + 1
            body = topic + (struct.pack('>H', pid) if args.qos else b'') + (b'\x00' if args.version == 5 else b'') + b'x' * 16
            writer.write(packet(0x30 | (args.qos << 1), body))
            if args.qos:
                try:
                    while True:
                        first, _ = await asyncio.wait_for(read_packet(reader), 10)
                        if first >> 4 == PUBACK:
                            break
                except asyncio.TimeoutError:
                    unacked.append({'index': i, 'message': n})
            await asyncio.sleep(1.0)
        writer.close()
    except (OSError, asyncio.IncompleteReadError) as e:
        closed('pub', i, phase, e)


async def ramp(factory, tasks):
    for i in range(1, args.count + 1):
        tasks.append(asyncio.create_task(factory(i)))
        await asyncio.sleep(1.0 / args.rate)


async def main():
    global done
    subs, pubs = [], []
    await ramp(subscriber, subs)
    await asyncio.sleep(2.0)
    await ramp(publisher, pubs)
    await asyncio.gather(*pubs)
    await asyncio.sleep(args.grace)
    done = True
    for t in subs:
        t.cancel()
    await asyncio.gather(*subs, return_exceptions=True)
    short = {i: n for i, n in sorted(received.items()) if n < args.messages}
    report = {
        'subscribers': len(received), 'publishers': len(pubs), 'messages_per_publisher': args.messages,
        'received_total': sum(received.values()), 'expected_total': args.count * args.messages,
        'subscribers_with_no_messages': sorted(i for i, n in received.items() if n == 0),
        'subscribers_with_fewer_messages': short,
        'suback_not_granted': {i: rc for i, rc in suback.items() if rc != [args.qos]},
        'subscribers_without_suback': sorted(set(received) - set(suback)),
        'connect_failed': connect_failed, 'closed_early': early_close, 'unacked_publishes': unacked,
    }
    print(json.dumps(report, indent=1))


asyncio.run(main())
