#!/usr/bin/env python3
"""
client_test.py — Functional correctness test for the TCP echo server.

Verifies:
  1. Single client echo (small payload)
  2. Single client echo (large payload near MAX_MSG_BYTES)
  3. Partial read handling (send header and body in separate calls)
  4. Multiple sequential messages on one connection
  5. Multiple clients concurrently
  6. Graceful disconnect (server does not crash)

Run with the server already started:
    ./src/server 8080 &
    python3 tests/client_test.py
"""

import socket
import struct
import threading
import time
import sys


HOST = "127.0.0.1"
PORT = 8080
PASS = 0
FAIL = 0


def send_framed(sock, payload: bytes):
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def recv_framed(sock) -> bytes:
    raw = _recv_exact(sock, 4)
    n   = struct.unpack(">I", raw)[0]
    return _recv_exact(sock, n)


def _recv_exact(sock, n) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed early")
        buf += chunk
    return buf


def check(name: str, condition: bool):
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
    print(f"  [{status}] {name}")


def test_basic_echo():
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        payload = b"Hello, Qualcomm!"
        send_framed(s, payload)
        reply = recv_framed(s)
        check("basic echo", reply == payload)


def test_large_payload():
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        payload = b"X" * (256 * 1024)   # 256 KB
        send_framed(s, payload)
        reply = recv_framed(s)
        check("large payload (256 KB)", reply == payload)


def test_partial_send():
    """Send header and body in two separate TCP segments."""
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        payload = b"partial send test"
        header  = struct.pack(">I", len(payload))
        s.sendall(header)
        time.sleep(0.05)          # simulate delayed second segment
        s.sendall(payload)
        reply = recv_framed(s)
        check("partial send (split TCP segments)", reply == payload)


def test_multiple_messages():
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        ok = True
        for i in range(100):
            payload = f"message-{i}".encode()
            send_framed(s, payload)
            reply = recv_framed(s)
            if reply != payload:
                ok = False
                break
        check("100 sequential messages on one connection", ok)


def test_concurrent_clients():
    errors = []

    def one_client():
        try:
            with socket.create_connection((HOST, PORT), timeout=5) as s:
                for i in range(50):
                    payload = f"concurrent-{i}".encode()
                    send_framed(s, payload)
                    reply = recv_framed(s)
                    if reply != payload:
                        errors.append("mismatch")
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=one_client) for _ in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("50 concurrent clients, 50 msgs each (2500 total)", len(errors) == 0)


def test_graceful_disconnect():
    """Server must not crash when client disconnects mid-session."""
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        send_framed(s, b"first message")
        recv_framed(s)
        # Abruptly close (RST)
    # Give the server 100ms to process the disconnect
    time.sleep(0.1)
    # Try another client immediately
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        payload = b"after disconnect"
        send_framed(s, payload)
        reply = recv_framed(s)
        check("server stable after abrupt client disconnect", reply == payload)


def main():
    print(f"Running correctness tests against {HOST}:{PORT}\n")
    tests = [
        test_basic_echo,
        test_large_payload,
        test_partial_send,
        test_multiple_messages,
        test_concurrent_clients,
        test_graceful_disconnect,
    ]
    for test in tests:
        name = test.__name__.replace("test_", "").replace("_", " ")
        print(f"[{name}]")
        try:
            test()
        except Exception as e:
            global FAIL
            FAIL += 1
            print(f"  [FAIL] exception: {e}")

    print(f"\n{'='*40}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
