#!/usr/bin/env python3
"""
load_test.py — Concurrent load test for the TCP echo server.

Protocol: each message is a 4-byte big-endian length prefix + payload.

Usage:
    python3 tests/load_test.py --host 127.0.0.1 --port 8080 \
                               --clients 200 --messages 500

Outputs:
    - Total throughput (messages/second)
    - p50, p95, p99 latencies in milliseconds
    - Zero errors (or error count if any)

These numbers go directly into your resume bullet point.
"""

import argparse
import socket
import struct
import threading
import time
import statistics
import sys


def send_framed(sock: socket.socket, payload: bytes) -> None:
    """Send 4-byte big-endian length prefix followed by payload."""
    header = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


def recv_framed(sock: socket.socket) -> bytes:
    """Receive one framed message, handling partial reads."""
    raw_len = _recv_exact(sock, 4)
    msg_len = struct.unpack(">I", raw_len)[0]
    return _recv_exact(sock, msg_len)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from sock, blocking until all arrive."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection prematurely")
        buf += chunk
    return buf


def worker(host: str, port: int, num_messages: int,
           latencies: list, errors: list) -> None:
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            payload = b"Hello from load-test client! " * 4   # ~116 bytes
            for _ in range(num_messages):
                t0 = time.perf_counter()
                send_framed(s, payload)
                reply = recv_framed(s)
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000)   # ms
                if reply != payload:
                    errors.append("echo mismatch")
    except Exception as e:
        errors.append(str(e))


def main():
    parser = argparse.ArgumentParser(description="TCP echo server load tester")
    parser.add_argument("--host",     default="127.0.0.1")
    parser.add_argument("--port",     type=int, default=8080)
    parser.add_argument("--clients",  type=int, default=100)
    parser.add_argument("--messages", type=int, default=200)
    args = parser.parse_args()

    print(f"[load_test] {args.clients} clients × {args.messages} messages/client")
    print(f"            target: {args.host}:{args.port}")
    print(f"            total messages: {args.clients * args.messages:,}")
    print()

    all_latencies: list[float] = []
    all_errors: list[str] = []
    per_thread_latencies = [[] for _ in range(args.clients)]
    per_thread_errors    = [[] for _ in range(args.clients)]

    threads = []
    t_start = time.perf_counter()

    for i in range(args.clients):
        t = threading.Thread(
            target=worker,
            args=(args.host, args.port, args.messages,
                  per_thread_latencies[i], per_thread_errors[i]),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    t_end = time.perf_counter()
    elapsed = t_end - t_start

    for lats in per_thread_latencies:
        all_latencies.extend(lats)
    for errs in per_thread_errors:
        all_errors.extend(errs)

    total_sent    = args.clients * args.messages
    total_success = len(all_latencies)
    throughput    = total_success / elapsed

    print("=== Results ===")
    print(f"  Elapsed:        {elapsed:.2f} s")
    print(f"  Messages sent:  {total_sent:,}")
    print(f"  Successful:     {total_success:,}")
    print(f"  Errors:         {len(all_errors)}")
    print(f"  Throughput:     {throughput:,.0f} msgs/sec")

    if all_latencies:
        all_latencies.sort()
        p50  = statistics.median(all_latencies)
        p95  = all_latencies[int(len(all_latencies) * 0.95)]
        p99  = all_latencies[int(len(all_latencies) * 0.99)]
        mean = statistics.mean(all_latencies)
        print(f"  Latency mean:   {mean:.2f} ms")
        print(f"  Latency p50:    {p50:.2f} ms")
        print(f"  Latency p95:    {p95:.2f} ms")
        print(f"  Latency p99:    {p99:.2f} ms")
        print()
        print("--- Copy these into your resume ---")
        print(f"  Handled {args.clients} concurrent connections "
              f"at {throughput:,.0f} msgs/sec with p99 latency of {p99:.1f} ms")

    if all_errors:
        print(f"\nErrors ({len(all_errors)} total):")
        for e in all_errors[:10]:
            print(f"  {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
