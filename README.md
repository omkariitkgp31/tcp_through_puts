# IoT Telemetry Ingestion Pipeline — Phase 1: TCP Load Distribution

A high-performance, purely local prototype demonstrating **L4 TCP connection distribution, socket stickiness, process isolation via PM2 fork mode, and real-time observability** across horizontally-scaled ingestion nodes.

---

## 1. Architecture Diagram

```
+-------------------------------------------------------------------------+
|                  Simulated IoT Devices (50 -> 800+ sockets)              |
|        (Node.js raw TCP runner streaming periodic JSON heartbeats)      |
+------------------------------------+------------------------------------+
                                     |
                                     | Raw TCP Connections
                                     v
+-------------------------------------------------------------------------+
|                       L4 TCP Load Balancer                              |
|           (TCP Port :7000 | HTTP Observability Port :8000)              |
|  - Strategy: Round-Robin / Least-Connections                            |
|  - Connection Stickiness: Full duplex socket-to-backend socket piping   |
|  - Active Health Probing: 2s interval against backend /health API       |
+------------------+-----------------+-----------------+------------------+
                   |                 |                 |                  |
                   v                 v                 v                  v
+--------------------+ +--------------------+ +--------------------+ +--------------------+
| Ingestion Node D1  | | Ingestion Node D2  | | Ingestion Node D3  | | Ingestion Node D4  |
| (PM2 Fork Mode)    | | (PM2 Fork Mode)    | | (PM2 Fork Mode)    | | (PM2 Fork Mode)    |
| TCP:  :7001        | | TCP:  :7002        | | TCP:  :7003        | | TCP:  :7004        |
| HTTP: :8001/metrics| | HTTP: :8002/metrics| | HTTP: :8003/metrics| | HTTP: :8004/metrics|
+--------------------+ +--------------------+ +--------------------+ +--------------------+

=============================================================================================
                  DELIBERATELY OUT OF SCOPE IN THIS PHASE (Phase 1)
=============================================================================================
  [X] Kafka / Message Queues   --> Kept out to isolate raw socket ingestion performance.
  [X] PostgreSQL / ClickHouse  --> Storage persistence is handled in downstream phases.
  [X] Redis / Caching Layer    --> State is kept in-memory to prevent external network I/O.
  [X] Deep Payload Parsing     --> Avoids CPU deserialization overhead during socket baseline.
=============================================================================================
```

---

## 2. Load Balancer Tradeoff: Custom Node.js TCP Proxy vs. HAProxy

| Characteristic | Custom Node.js TCP LB (`lb/tcp-load-balancer.js`) | HAProxy (TCP Mode) |
| :--- | :--- | :--- |
| **Local Portability** | **100% Pure JS (Zero external binary installs)**. Works out-of-the-box across Windows, macOS, and Linux. | Requires separate HAProxy binary or Docker container setup (adds overhead on Windows). |
| **Observability** | Exposes custom JSON `/metrics` matching node schema. | Requires Prometheus exporter or HAProxy stats socket parsing. |
| **Performance** | Excellent for local prototyping & testing thousands of concurrent connections. | C-based kernel-level epoll/kqueue event loop; optimal for multi-100k production setups. |
| **Production Recommendation** | Recommended for local dev/integration testing and custom routing. | Recommended for production L4 perimeter routing. |

---

## 3. Quick Start & Setup

### Prerequisites
- Node.js `v18+` (Tested on `v24.12.0`)
- npm `v9+`

### Installation
Clone the repository and install dependencies:
```bash
git clone <repo-url>
cd tcp_through_put
npm install
```

---

## 4. Running the Pipeline

### Step 1: Start Ingestion Nodes (D1..D4 in Fork Mode)
Start the 4 ingestion nodes as separate OS processes:
```bash
# Using PM2:
npx pm2 start ecosystem.config.js --no-daemon

# (Or in standard background PM2 mode: npx pm2 start ecosystem.config.js)
```

### Step 2: Start the L4 TCP Load Balancer
In a new terminal window:
```bash
npm run lb
# Or: node lb/tcp-load-balancer.js --port 7000 --http-port 8000
```

### Step 3: Run the IoT Device Simulator
In another terminal window:
```bash
# Simulate 400 concurrent devices for 15 seconds:
npm run simulate -- --connections 400 --duration 15
```

---

## 5. Automated Test Suite & Benchmarks

### 1. Automated Ramping Test Suite (50 -> 100 -> 200 -> 400 -> 800 Devices)
Runs the automated multi-stage load test and prints a consolidated ASCII findings table:
```bash
npm run test:load
```

### 2. Autocannon HTTP `/metrics` Path Benchmark
Validates that the observability path handles high request loads without blocking the event loop:
```bash
npm run test:metrics
```

---

## 6. How to Interpret Metrics & Identify Ceilings

### What "Good" Looks Like
- **Connection Success Rate**: `100.00%` (0 failed / 0 refused).
- **Even Distribution**: Each node holds roughly $\frac{1}{N}$ of total active connections (e.g. ~100 connections per node on a 400-device run).
- **Predictable Memory**: Ingestion nodes stay flat around 50–60 MB RSS.
- **Low Latency on Metrics**: Autocannon reports `< 5 ms` p99 latency on HTTP `/metrics`.

### Signs of Approaching Local Machine Ceilings
1. **Connection Refused / Timeouts (`ECONNREFUSED` / `ETIMEDOUT`)**:
   - Cause: OS ephemeral port exhaustion or hitting local socket descriptor limits (`ulimit -n` on Linux/macOS or `MaxUserPort` on Windows).
2. **Rising CPU & Event-Loop Lag**:
   - Cause: Single-threaded event loop saturation when processing tens of thousands of rapid telemetry heartbeats without batching.
3. **RSS Memory Ramp**:
   - Cause: Buffer accumulation if TCP socket write queues back up.

---

## 7. Scaling and Failover Testing

### How to Scale to D5 and D6
1. Open `ecosystem.config.js` and add apps `ingestion-d5` (TCP :7005 / HTTP :8005) and `ingestion-d6` (TCP :7006 / HTTP :8006).
2. In `lb/tcp-load-balancer.js`, append `{ id: 'D5', host: '127.0.0.1', tcpPort: 7005, httpPort: 8005 }` to the `backends` array.
3. Restart PM2 and the LB.

### How to Test Failover
1. While simulated traffic is running, stop one node in PM2:
   ```bash
   npx pm2 stop ingestion-d2
   ```
2. The Load Balancer detects the failure within 2 seconds via its `/health` check and logs:
   `[LB Health] Backend D2 unreachable, marking UNHEALTHY`
3. All **new** incoming device connections are automatically routed to remaining healthy nodes (D1, D3, D4) without interrupting the overall service.
4. Restart the node (`npx pm2 restart ingestion-d2`) and observe it automatically rejoining the pool:
   `[LB Health] Backend D2 recovered and marked HEALTHY`

---

## 8. Known Limitations of Phase 1 Prototype

1. **Single-Machine Resource Sharing**: Both the simulator, the load balancer, and all 4 ingestion nodes run on the same physical CPU/loopback interface, sharing OS network stack queues and local ports.
2. **No Data Persistence**: Ingestion nodes simply accept and acknowledge raw sockets without saving payloads to Kafka/database.
3. **No TLS / Authentication**: Devices connect over plain TCP without mTLS or JWT handshakes.
