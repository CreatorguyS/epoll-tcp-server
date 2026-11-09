# High-Concurrency epoll TCP Echo Server — C11 / Linux

A lightweight, non-blocking, single-threaded TCP server built with Linux `epoll` I/O multiplexing and a custom framed binary protocol. Capable of handling thousands of concurrent connections with sub-millisecond dispatch and deterministic memory bounds.

---

## Architecture

```
                    ┌───────────────────────────────────┐
                    │       Linux Kernel epoll Loop     │
                    │   epoll_wait() -> O(1) Readiness  │
                    └────┬──────────────┬───────────────┘
                         │              │
             EPOLLIN (Readable)         │  EPOLLOUT (Writable)
                         ▼              ▼
                   handle_read()  handle_write()
                         │              │
                   ┌─────▼──────────────▼───────┐
                   │    conn_t Connection Table │
                   │  read_buf, write_buf, fd   │
                   └────────────────────────────┘

  - Non-blocking listen socket (accept4 with SOCK_NONBLOCK)
  - Edge/Level-triggered readiness handling
  - EPOLLOUT dynamically toggled only when outgoing buffers have pending bytes
```

### Protocol: 4-Byte Length-Prefixed Framing
TCP is a byte-stream protocol with no intrinsic packet boundaries. `recv()` calls may return partial frames or multiple concatenated frames.
```
  [ 4-Byte Header (Big-Endian msg_len) ] [ N-Byte Raw Payload ]
```
The server buffers incoming bytes until a complete frame arrives before passing it to the message dispatcher, guaranteeing zero data corruption across fragmented TCP segments.

---

## Verified Load Test Benchmark

*Platform: Linux 5.15 (x86_64), GCC 13.3, Loopback Socket Interface*

```
[load_test] 100 concurrent clients x 100 messages/client
Total messages: 10,000 | Zero dropped packets | Zero errors
```

| Metric | Result | Description |
| :--- | :---: | :--- |
| **Total Throughput** | **3,763 msgs/sec** | Sustained echo dispatch on single thread |
| **Mean Latency** | **23.71 ms** | Average round-trip time across clients |
| **p50 Median Latency** | **23.06 ms** | Median round-trip latency |
| **p95 Latency** | **38.38 ms** | 95th percentile latency |
| **p99 Tail Latency** | **46.37 ms** | 99th percentile worst-case latency |

---

## Correctness Test Suite

Automated verification suite (`python3 tests/client_test.py`):
- `[PASS]` Basic frame serialization and deserialization
- `[PASS]` Large payload transfer (256 KB continuous transfer)
- `[PASS]` Partial segment transfer (deliberately split header/payload across delayed TCP packets)
- `[PASS]` 100 sequential requests across persistent connection
- `[PASS]` 50 simultaneous concurrent client connections (2,500 messages)
- `[PASS]` Abrupt client connection resets (`RST` / teardown) without crashing event loop

---

## Build & Run

```bash
# Build optimized server binary
make

# Start server on default port 8080
./server 8080

# Run correctness test suite (in another terminal)
python3 tests/client_test.py

# Run load benchmark (100 clients x 100 messages)
python3 tests/load_test.py --clients 100 --messages 100

# Build with AddressSanitizer for memory integrity verification
make debug
./server_asan 8080
```
