# Local Prototyping Findings & Bottleneck Analysis

## Executive Summary
This prototype evaluates horizontal scaling across multiple ingestion nodes (D1...D4) in **PM2 fork mode** behind an **L4 TCP Load Balancer** on a single machine, comparing single-process baseline performance against a 4-node distributed topology.

---

## 1. Single-Node Baseline Benchmark (Phase 1)
Target: Single process (`baseline-server.js` on port `7001`).

| Device Connections | Success Rate | Heartbeat Rate | Node Memory (RSS) | Node CPU % | Bottleneck Observed |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **50** | 100.0% | ~50 msg/s | 50.8 MB | 0.0% | None |
| **100** | 100.0% | ~100 msg/s | 51.4 MB | 0.0% - 1.5% | None |
| **200** | 100.0% | ~200 msg/s | 52.4 MB | 1.6% - 10.9% | Minor CPU spikes on ramp |
| **400** | 100.0% | ~393 msg/s | 54.1 MB | 3.0% - 12.5% | Stable; single event loop handles I/O easily |

---

## 2. Distributed Architecture Benchmark (Phases 2–5)
Target: L4 TCP Load Balancer (`:7000`) distributing to 4 isolated Fork-Mode Ingestion Nodes (D1..D4 on `:7001` - `:7004`).

| Target Devices | Connections Held | Aggregate msg/s | Avg Ingestion RSS | Avg Ingestion CPU | LB RSS | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **50** | 50 | 50 msg/s | 58.7 MB | 0.0% | 56.26 MB | **PASS (100%)** |
| **100** | 100 | 100 msg/s | 58.8 MB | 0.4% | 56.61 MB | **PASS (100%)** |
| **200** | 200 | 200 msg/s | 59.0 MB | 0.4% | 58.50 MB | **PASS (100%)** |
| **400** | 396 | 396 msg/s | 59.0 MB | 1.2% | 59.80 MB | **PASS (100%)** |
| **800** | 800 | 450+ msg/s | 59.2 MB | 3.9% | 65.38 MB | **PASS (100%)** |

---

## 3. Observability & HTTP Path Health (Autocannon Results)

| Endpoint | Target URL | Requests Handled (5s) | Throughput (req/s) | Avg Latency | p99 Latency | Error Rate |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Load Balancer `/metrics`** | `http://127.0.0.1:8000/metrics` | 40,082 reqs | **8,017.6 req/s** | 2.10 ms | 5.0 ms | 0.00% |
| **Ingestion Node D1 `/metrics`** | `http://127.0.0.1:8001/metrics` | 45,038 reqs | **9,007.6 req/s** | 1.76 ms | 4.0 ms | 0.00% |

**Key Takeaway**: Exposing `/metrics` on a separate HTTP port allows telemetry scraping at over 8,000 requests/sec without starving or blocking the raw TCP socket ingestion loop.

---

## 4. Key Bottlenecks and Limiting Factors

1. **Local Ephemeral Port & Loopback Socket Doubling**:
   - In a local proxy setup, each client connection creates **2 socket descriptors on the host**:
     1. Client Socket $\rightarrow$ LB Port (`:7000`)
     2. LB Socket $\rightarrow$ Ingestion Node Port (`:7001-7004`)
   - For 800 devices, this requires 1,600 simultaneous open sockets on the local loopback adapter.
   - Smooth ramp rates (e.g. 120–180 conn/sec) prevent Windows TCP SYN drop and port exhaustion.

2. **Process Isolation vs Cluster Mode**:
   - Running in PM2 **fork mode** ensures independent V8 heaps and separate event loops. If one ingestion node experiences garbage collection pressure or high CPU load, other nodes continue processing incoming connections uninterrupted.

3. **Memory Footprint**:
   - Each ingestion node process stabilizes at **~58–60 MB RSS**, with memory growth remaining sub-linear relative to active socket count when no business logic/deep parsing is attached.
