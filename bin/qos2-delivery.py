#!/usr/bin/env python3
"""QoS 2 publishers for bin/verify-delivery.sh (QOS=2). MQTT 5 over plain TCP, no dependencies.

  exact <host> <port> <run_id> <connections> <messages>
      Each connection publishes <messages> numbered 1 KB QoS 2 messages to device/<run_id>/x/<n>, one exchange at a
      time: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP before the next PUBLISH. Payloads start with a 4-digit sequence
      number. Prints "completed <n>" (PUBCOMP with a success reason) and "failed <n>".

  reconnect <host> <port> <run_id> <iterations>
      Interrupted QoS 2 flows. Each flow publishes one message to its own topic, device/<run_id>/r/<case>/<i>, drops
      the TCP connection (RST, no DISCONNECT) at one point of the exchange, reconnects with the same client ID
      (clean start 0, session expiry 300 s) and finishes the exchange as MQTT 5 section 4.4 requires:
        after-pubrec   PUBLISH, PUBREC, drop. Reconnect, PUBREL, PUBCOMP.
        before-pubrec  PUBLISH, drop before reading PUBREC. Reconnect, PUBLISH again (DUP, same packet ID), PUBREC,
                       PUBREL, PUBCOMP.
        after-pubrel   PUBLISH, PUBREC, PUBREL, drop before PUBCOMP. Reconnect, PUBREL, PUBCOMP.
      Prints one line per flow: "<case> <topic> <result>". The result starts with "ok" when the exchange completed
      (after-pubrel also accepts PUBCOMP 146 "Packet Identifier not found", see flow()), and ends with
      ",session_present=0" when the broker didn't resume the session on reconnect. Otherwise it says what failed.

Either way the caller then counts the records in Kafka: every completed message must be there exactly once.

One exchange at a time per connection is deliberate: with several QoS 2 PUBLISHes in flight on one connection
(for example mosquitto_pub -l), Zilla's mqtt-kafka proxy stops its engine (noted in aklivity/zilla#3309).
"""
import socket, struct, sys, threading, time

TIMEOUT = 30
PUBREC, PUBCOMP, CONNACK, DISCONNECT = 5, 7, 2, 14


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


def connect_pkt(cid, clean_start):
    props = bytes([0x11]) + struct.pack('>I', 300)            # session expiry interval 300 s
    vh = utf8('MQTT') + bytes([5, 0x02 if clean_start else 0x00]) + struct.pack('>H', 60) + varint(len(props)) + props
    return packet(0x10, vh + utf8(cid))


def publish_pkt(topic, pid, payload, dup=False):
    return packet(0x34 | (0x08 if dup else 0), utf8(topic) + struct.pack('>H', pid) + varint(0) + payload)


def pubrel_pkt(pid):
    return packet(0x62, struct.pack('>H', pid))               # reason omitted = success


def recv_exactly(s, n):
    buf = b''
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError('connection closed')
        buf += chunk
    return buf


def read_packet(s):
    first = recv_exactly(s, 1)[0]
    mult, n = 1, 0
    while True:
        b = recv_exactly(s, 1)[0]
        n += (b & 0x7f) * mult
        mult *= 128
        if not b & 0x80:
            break
    return first, recv_exactly(s, n)


def expect(s, ptype):
    """Read packets until one of type ptype arrives and return its body. A DISCONNECT from the broker raises."""
    while True:
        first, body = read_packet(s)
        if first >> 4 == ptype:
            return body
        if first >> 4 == DISCONNECT:
            raise RuntimeError(f'DISCONNECT_{body[0] if body else 0}')


def reason(body):
    return body[2] if len(body) > 2 else 0                    # after the 2-byte packet ID; omitted = 0


def open_session(host, port, cid, clean_start):
    s = socket.create_connection((host, port), timeout=TIMEOUT)
    s.sendall(connect_pkt(cid, clean_start))
    body = expect(s, CONNACK)
    return s, body[0] & 1, body[1]                            # session present, reason code


def hard_close(s):
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))   # RST, no DISCONNECT
    s.close()


def exact(host, port, run, connections, messages):
    counts = {'completed': 0, 'failed': 0}
    lock = threading.Lock()

    def one(c):
        done = 0
        try:
            s, _, rc = open_session(host, port, f'{run}-x-{c}', True)
            if rc != 0:
                raise RuntimeError(f'CONNACK_{rc}')
            topic = f'device/{run}/x/{c}'
            for n in range(1, messages + 1):
                pid = (n - 1) % 65535 + 1
                payload = f'{n:04d}-'.encode().ljust(1024, b'x')
                s.sendall(publish_pkt(topic, pid, payload))
                if reason(expect(s, PUBREC)) >= 0x80:          # 16 "no matching subscribers" is a success
                    continue
                s.sendall(pubrel_pkt(pid))
                if reason(expect(s, PUBCOMP)) == 0:
                    done += 1
            s.sendall(packet(0xE0, b'\x00\x00'))
            s.close()
        except Exception as e:
            print(f'connection {c}: {type(e).__name__} {e} after {done} messages', file=sys.stderr)
        with lock:
            counts['completed'] += done
            counts['failed'] += messages - done

    threads = [threading.Thread(target=one, args=(c,)) for c in range(1, connections + 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"completed {counts['completed']}")
    print(f"failed {counts['failed']}")


def flow(host, port, run, case, i):
    cid = f'{run}-r-{case}-{i}'
    topic = f'device/{run}/r/{case}/{i}'
    payload, pid = f'{run}-{case}-{i}'.encode(), 7
    s, _, rc = open_session(host, port, cid, True)
    if rc != 0:
        return topic, f'connack1={rc}'
    s.sendall(publish_pkt(topic, pid, payload))
    if case == 'before-pubrec':
        time.sleep(0.05)
        hard_close(s)
    else:
        if (r := reason(expect(s, PUBREC))) >= 0x80:
            hard_close(s)
            return topic, f'pubrec1={r}'
        if case == 'after-pubrel':
            s.sendall(pubrel_pkt(pid))
            time.sleep(0.05)
        hard_close(s)
    time.sleep(0.5)
    s, session_present, rc = open_session(host, port, cid, False)
    if rc != 0:
        return topic, f'connack2={rc}'
    note = '' if session_present else ',session_present=0'
    if case == 'before-pubrec':
        s.sendall(publish_pkt(topic, pid, payload, dup=True))
        if (r := reason(expect(s, PUBREC))) >= 0x80:
            hard_close(s)
            return topic, f'pubrec2={r}{note}'
    s.sendall(pubrel_pkt(pid))
    r = reason(expect(s, PUBCOMP))
    s.sendall(packet(0xE0, b'\x00\x00'))                      # DISCONNECT, normal
    s.close()
    if r == 0:
        return topic, 'ok' + note
    if r == 0x92 and case == 'after-pubrel':
        # "Packet Identifier not found": the broker had completed the exchange when the first PUBREL arrived, and
        # its PUBCOMP was lost with the connection. A valid answer to the resent PUBREL (MQTT 5 section 4.3.3).
        return topic, 'ok-pubcomp-146' + note
    return topic, f'pubcomp={r}{note}'


def reconnect(host, port, run, iterations):
    for case in ('after-pubrec', 'before-pubrec', 'after-pubrel'):
        for i in range(1, iterations + 1):
            try:
                topic, result = flow(host, port, run, case, i)
            except Exception as e:
                topic, result = f'device/{run}/r/{case}/{i}', f'error:{type(e).__name__}:{e}'.replace(' ', '_')
            print(case, topic, result, flush=True)


if __name__ == '__main__':
    mode, host, port, run = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
    if mode == 'exact':
        exact(host, port, run, int(sys.argv[5]), int(sys.argv[6]))
    elif mode == 'reconnect':
        reconnect(host, port, run, int(sys.argv[5]))
    else:
        sys.exit(f'unknown mode {mode}')
