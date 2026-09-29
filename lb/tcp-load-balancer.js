#!/usr/bin/env node
/**
 * lb/tcp-load-balancer.js - L4 TCP Load Balancer
 * 
 * Accepts raw TCP connections on a single entry point (e.g. :7000) and distributes
 * them across healthy Ingestion Nodes (D1..Dn) using Round-Robin or Least-Connections.
 * 
 * Guarantees connection-level stickiness: Once a socket is assigned to a node,
 * it is directly piped to that node for the entire life of the socket.
 * 
 * Supports Multi-Process LB via Node cluster module (SO_REUSEPORT emulation):
 *   LB_WORKERS=1 (default): Single process handling both TCP and HTTP.
 *   LB_WORKERS > 1: Primary process runs HTTP :8000 metrics aggregation and backend health checks.
 *                   N worker processes bind TCP :7000 and handle full-duplex TCP routing.
 */

const net = require('net');
const http = require('http');
const cluster = require('cluster');

// Ensure round-robin connection distribution across cluster workers (default on Linux, required on Windows)
cluster.schedulingPolicy = cluster.SCHED_RR;

// Helper to parse CLI args
const args = process.argv.slice(2);
function getArg(flag, defaultVal) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultVal;
}

const TCP_PORT = parseInt(getArg('--port', process.env.LB_TCP_PORT || '7000'), 10);
const HTTP_PORT = parseInt(getArg('--http-port', process.env.LB_HTTP_PORT || '8000'), 10);
const HOST = getArg('--host', process.env.HOST || '0.0.0.0');
const ALGO = getArg('--algo', process.env.ALGO || 'round-robin'); // 'round-robin' or 'least-connections'
const LB_WORKERS = parseInt(getArg('--lb-workers', process.env.LB_WORKERS || '1'), 10);

// Backend Ingestion Nodes Pool
function createInitialBackends() {
  return [
    { id: 'D1', host: '127.0.0.1', tcpPort: 7001, httpPort: 8001, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
    { id: 'D2', host: '127.0.0.1', tcpPort: 7002, httpPort: 8002, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
    { id: 'D3', host: '127.0.0.1', tcpPort: 7003, httpPort: 8003, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
    { id: 'D4', host: '127.0.0.1', tcpPort: 7004, httpPort: 8004, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null }
  ];
}

// =============================================================================
// MULTI-PROCESS PRIMARY PROCESS (LB_WORKERS > 1 && cluster.isPrimary)
// =============================================================================
if (LB_WORKERS > 1 && cluster.isPrimary) {
  console.log(`=======================================================`);
  console.log(`⚖️  L4 TCP Multi-Process Load Balancer (Primary PID: ${process.pid})`);
  console.log(`   TCP Entry Point    : tcp://${HOST}:${TCP_PORT}`);
  console.log(`   Worker Processes   : ${LB_WORKERS}`);
  console.log(`   Algorithm          : ${ALGO}`);
  console.log(`   HTTP Metrics Port  : http://${HOST}:${HTTP_PORT}/metrics`);
  console.log(`=======================================================`);

  const backends = createInitialBackends();
  const workerMetricsMap = new Map();

  // CPU calculation state for primary
  let lastCpuUsage = process.cpuUsage();
  let lastCpuTime = Date.now();
  let currentCpuPercent = '0.0';

  setInterval(() => {
    const cpuNow = process.cpuUsage();
    const timeNow = Date.now();
    const userDiff = cpuNow.user - lastCpuUsage.user;
    const sysDiff = cpuNow.system - lastCpuUsage.system;
    const timeDiff = (timeNow - lastCpuTime) * 1000;
    if (timeDiff > 0) {
      currentCpuPercent = (((userDiff + sysDiff) / timeDiff) * 100).toFixed(1);
    }
    lastCpuUsage = cpuNow;
    lastCpuTime = timeNow;
  }, 1000);

  // Health Checker
  function checkBackendHealth(backend) {
    const req = http.get({
      host: backend.host,
      port: backend.httpPort,
      path: '/health',
      timeout: 1500
    }, (res) => {
      backend.lastCheck = Date.now();
      if (res.statusCode === 200) {
        if (!backend.healthy) {
          console.log(`[LB Health] Backend ${backend.id} recovered and marked HEALTHY`);
        }
        backend.healthy = true;
      } else {
        if (backend.healthy) {
          console.warn(`[LB Health] Backend ${backend.id} returned status ${res.statusCode}, marking UNHEALTHY`);
        }
        backend.healthy = false;
      }
      res.resume();
    });

    req.on('timeout', () => {
      req.destroy();
      backend.healthy = false;
    });

    req.on('error', () => {
      backend.healthy = false;
    });
  }

  function runHealthChecks() {
    backends.forEach(checkBackendHealth);
    // Broadcast health status to all cluster workers
    const healthStatus = backends.map(b => ({ id: b.id, healthy: b.healthy }));
    for (const id in cluster.workers) {
      try {
        cluster.workers[id].send({ type: 'HEALTH_UPDATE', backends: healthStatus });
      } catch {}
    }
  }

  runHealthChecks();
  const healthInterval = setInterval(runHealthChecks, 2000);

  // Fork worker processes
  for (let i = 0; i < LB_WORKERS; i++) {
    const worker = cluster.fork({ LB_WORKER_NUM: i + 1 });
    worker.on('message', (msg) => {
      if (msg.type === 'METRICS_UPDATE') {
        workerMetricsMap.set(msg.pid, msg);
      }
    });
  }

  cluster.on('fork', (worker) => {
    worker.on('message', (msg) => {
      if (msg.type === 'METRICS_UPDATE') {
        workerMetricsMap.set(msg.pid, msg);
      }
    });
  });

  cluster.on('exit', (worker, code, signal) => {
    console.warn(`[LB Primary] Worker ${worker.process.pid} exited (${signal || code}). Spawning replacement...`);
    workerMetricsMap.delete(worker.process.pid);
    const newWorker = cluster.fork();
    newWorker.on('message', (msg) => {
      if (msg.type === 'METRICS_UPDATE') {
        workerMetricsMap.set(msg.pid, msg);
      }
    });
  });

  // HTTP Metrics & Health Server on Primary
  const httpServer = http.createServer((req, res) => {
    const url = req.url.split('?')[0];

    if (url === '/health') {
      const healthyCount = backends.filter(b => b.healthy).length;
      const status = healthyCount > 0 ? 200 : 503;
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: healthyCount > 0 ? 'ok' : 'degraded',
        healthyBackends: healthyCount,
        totalBackends: backends.length,
        lbWorkers: LB_WORKERS
      }));
      return;
    }

    if (url === '/metrics') {
      const workerMetrics = Array.from(workerMetricsMap.values());
      const primaryMem = process.memoryUsage();

      const totalActive = workerMetrics.reduce((sum, w) => sum + (w.activeIncomingConns || 0), 0);
      const totalConns = workerMetrics.reduce((sum, w) => sum + (w.totalIncomingConns || 0), 0);
      const totalBytes = workerMetrics.reduce((sum, w) => sum + (w.totalBytesBridged || 0), 0);
      const totalCpu = workerMetrics.reduce((sum, w) => sum + (w.cpuPercent || 0), 0) + parseFloat(currentCpuPercent);
      const totalRss = workerMetrics.reduce((sum, w) => sum + (w.rssMB || 0), 0) + parseFloat((primaryMem.rss / (1024 * 1024)).toFixed(2));
      const totalHeap = workerMetrics.reduce((sum, w) => sum + (w.heapUsedMB || 0), 0) + parseFloat((primaryMem.heapUsed / (1024 * 1024)).toFixed(2));

      const backendMap = backends.map(b => {
        const active = workerMetrics.reduce((sum, w) => {
          const wb = w.backends?.find(x => x.id === b.id);
          return sum + (wb ? wb.activeConns : 0);
        }, 0);
        const total = workerMetrics.reduce((sum, w) => {
          const wb = w.backends?.find(x => x.id === b.id);
          return sum + (wb ? wb.totalConns : 0);
        }, 0);
        return {
          id: b.id,
          endpoint: `${b.host}:${b.tcpPort}`,
          healthy: b.healthy,
          activeConnections: active,
          totalConnections: total
        };
      });

      const metrics = {
        service: 'tcp-load-balancer',
        algorithm: ALGO,
        tcpPort: TCP_PORT,
        httpPort: HTTP_PORT,
        lbWorkers: LB_WORKERS,
        activeClientConnections: totalActive,
        totalClientConnections: totalConns,
        totalBytesBridgedMB: parseFloat((totalBytes / (1024 * 1024)).toFixed(2)),
        backends: backendMap,
        memory: {
          rssMB: parseFloat(totalRss.toFixed(2)),
          heapUsedMB: parseFloat(totalHeap.toFixed(2))
        },
        cpuPercent: parseFloat(totalCpu.toFixed(1)),
        pid: process.pid,
        uptimeSeconds: Math.floor(process.uptime()),
        workers: workerMetrics.map(w => ({
          pid: w.pid,
          activeClientConnections: w.activeIncomingConns,
          totalClientConnections: w.totalIncomingConns,
          cpuPercent: w.cpuPercent,
          rssMB: w.rssMB
        }))
      };

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store'
      });
      res.end(JSON.stringify(metrics, null, 2));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  });

  httpServer.listen(HTTP_PORT, HOST, () => {
    console.log(`[LB Primary] HTTP Metrics Aggregator listening on http://${HOST}:${HTTP_PORT}/metrics`);
  });

  // Periodic Terminal Status Monitor
  setInterval(() => {
    const workerMetrics = Array.from(workerMetricsMap.values());
    const totalActive = workerMetrics.reduce((sum, w) => sum + (w.activeIncomingConns || 0), 0);
    const totalConns = workerMetrics.reduce((sum, w) => sum + (w.totalIncomingConns || 0), 0);
    if (totalActive > 0 || totalConns > 0) {
      const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
      const workerDist = workerMetrics.map(w => `PID${w.pid}:${w.activeIncomingConns}`).join(' | ');
      const distStr = backends.map(b => {
        const active = workerMetrics.reduce((sum, w) => {
          const wb = w.backends?.find(x => x.id === b.id);
          return sum + (wb ? wb.activeConns : 0);
        }, 0);
        return `${b.id}(${b.healthy ? '✓' : '✗'}): ${active}`;
      }).join(' | ');
      console.log(`[${timestamp}] [LB Multi :${TCP_PORT}] Total Active: ${totalActive.toString().padStart(4)} | Workers -> [${workerDist}] | Nodes -> [${distStr}]`);
    }
  }, 1000);

  // Graceful shutdown
  function shutdown() {
    console.log(`\nShutting down Multi-Process Load Balancer...`);
    clearInterval(healthInterval);
    httpServer.close(() => {
      for (const id in cluster.workers) {
        try {
          cluster.workers[id].kill();
        } catch {}
      }
      console.log(`Multi-Process Load Balancer offline. Bye!`);
      process.exit(0);
    });
    setTimeout(() => process.exit(0), 2000).unref();
  }

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

} else {
  // =============================================================================
  // SINGLE-PROCESS MODE (LB_WORKERS <= 1) OR CLUSTER WORKER PROCESS
  // =============================================================================
  const backends = createInitialBackends();

  let roundRobinIndex = 0;
  let totalIncomingConns = 0;
  let activeIncomingConns = 0;
  let totalBytesBridged = 0;

  // CPU calculation state
  let lastCpuUsage = process.cpuUsage();
  let lastCpuTime = Date.now();
  let currentCpuPercent = '0.0';

  setInterval(() => {
    const cpuNow = process.cpuUsage();
    const timeNow = Date.now();
    const userDiff = cpuNow.user - lastCpuUsage.user;
    const sysDiff = cpuNow.system - lastCpuUsage.system;
    const timeDiff = (timeNow - lastCpuTime) * 1000;
    if (timeDiff > 0) {
      currentCpuPercent = (((userDiff + sysDiff) / timeDiff) * 100).toFixed(1);
    }
    lastCpuUsage = cpuNow;
    lastCpuTime = timeNow;
  }, 1000);

  // IPC Health Updates from Primary in multi-process mode
  if (cluster.isWorker) {
    process.on('message', (msg) => {
      if (msg.type === 'HEALTH_UPDATE') {
        msg.backends.forEach(hb => {
          const b = backends.find(x => x.id === hb.id);
          if (b) b.healthy = hb.healthy;
        });
      }
    });

    // Report metrics to primary every 500ms
    setInterval(() => {
      const mem = process.memoryUsage();
      if (process.send) {
        process.send({
          type: 'METRICS_UPDATE',
          pid: process.pid,
          activeIncomingConns,
          totalIncomingConns,
          totalBytesBridged,
          backends: backends.map(b => ({
            id: b.id,
            activeConns: b.activeConns,
            totalConns: b.totalConns
          })),
          cpuPercent: parseFloat(currentCpuPercent),
          rssMB: parseFloat((mem.rss / (1024 * 1024)).toFixed(2)),
          heapUsedMB: parseFloat((mem.heapUsed / (1024 * 1024)).toFixed(2))
        });
      }
    }, 500);
  }

  // Health Checker (Used when single-process)
  let healthInterval = null;
  if (!cluster.isWorker) {
    function checkBackendHealth(backend) {
      const req = http.get({
        host: backend.host,
        port: backend.httpPort,
        path: '/health',
        timeout: 1500
      }, (res) => {
        backend.lastCheck = Date.now();
        if (res.statusCode === 200) {
          if (!backend.healthy) {
            console.log(`[LB Health] Backend ${backend.id} recovered and marked HEALTHY`);
          }
          backend.healthy = true;
        } else {
          if (backend.healthy) {
            console.warn(`[LB Health] Backend ${backend.id} returned status ${res.statusCode}, marking UNHEALTHY`);
          }
          backend.healthy = false;
        }
        res.resume();
      });

      req.on('timeout', () => {
        req.destroy();
        if (backend.healthy) {
          console.warn(`[LB Health] Backend ${backend.id} health check TIMEOUT, marking UNHEALTHY`);
        }
        backend.healthy = false;
      });

      req.on('error', (err) => {
        if (backend.healthy) {
          console.warn(`[LB Health] Backend ${backend.id} unreachable (${err.message}), marking UNHEALTHY`);
        }
        backend.healthy = false;
      });
    }

    function runHealthChecks() {
      backends.forEach(checkBackendHealth);
    }

    runHealthChecks();
    healthInterval = setInterval(runHealthChecks, 2000);
  }

  // Node Selection Strategy
  function getNextBackend() {
    const healthyBackends = backends.filter(b => b.healthy);
    if (healthyBackends.length === 0) {
      return null;
    }

    if (ALGO === 'least-connections') {
      let best = healthyBackends[0];
      for (let i = 1; i < healthyBackends.length; i++) {
        if (healthyBackends[i].activeConns < best.activeConns) {
          best = healthyBackends[i];
        }
      }
      return best;
    }

    // Default: Round-Robin among healthy nodes
    const selected = healthyBackends[roundRobinIndex % healthyBackends.length];
    roundRobinIndex = (roundRobinIndex + 1) % healthyBackends.length;
    return selected;
  }

  // L4 TCP Proxy Server
  const tcpServer = net.createServer({ noDelay: true, keepAlive: true }, (clientSocket) => {
    totalIncomingConns++;
    activeIncomingConns++;

    clientSocket.setKeepAlive(true, 10000);
    clientSocket.setNoDelay(true);

    const targetBackend = getNextBackend();

    if (!targetBackend) {
      console.error(`[LB PID:${process.pid}] No healthy backends available to handle connection! Dropping.`);
      clientSocket.destroy();
      activeIncomingConns = Math.max(0, activeIncomingConns - 1);
      return;
    }

    targetBackend.activeConns++;
    targetBackend.totalConns++;

    // Explicit logging for sticky routing & worker PID verification
    if (totalIncomingConns <= 10 || totalIncomingConns % 100 === 0) {
      console.log(`[LB Worker PID:${process.pid}] Accepted connection #${totalIncomingConns} -> routed to backend ${targetBackend.id}`);
    }

    // Dedicated upstream connection to backend
    const backendSocket = net.connect({
      host: targetBackend.host,
      port: targetBackend.tcpPort,
      noDelay: true,
      keepAlive: true
    });

    // Full duplex pipe with connection stickiness
    clientSocket.pipe(backendSocket);
    backendSocket.pipe(clientSocket);

    // Track throughput
    clientSocket.on('data', (chunk) => {
      totalBytesBridged += chunk.length;
    });

    let isCleanedUp = false;
    function cleanup() {
      if (isCleanedUp) return;
      isCleanedUp = true;
      targetBackend.activeConns = Math.max(0, targetBackend.activeConns - 1);
      activeIncomingConns = Math.max(0, activeIncomingConns - 1);
    }

    clientSocket.on('error', (err) => {
      if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') {
        console.error(`[LB Client Socket Error PID:${process.pid}] ${err.message}`);
      }
      backendSocket.destroy();
      cleanup();
    });

    backendSocket.on('error', (err) => {
      if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') {
        console.error(`[LB Backend ${targetBackend.id} Socket Error PID:${process.pid}] ${err.message}`);
      }
      clientSocket.destroy();
      cleanup();
    });

    clientSocket.on('close', cleanup);
    backendSocket.on('close', cleanup);
  });

  tcpServer.on('error', (err) => {
    console.error(`[LB TCP Server Error PID:${process.pid}] ${err.message}`);
    process.exit(1);
  });

  // Single-process HTTP Metrics & Health Server (only if NOT a cluster worker)
  let httpServer = null;
  if (!cluster.isWorker) {
    httpServer = http.createServer((req, res) => {
      const url = req.url.split('?')[0];

      if (url === '/health') {
        const healthyCount = backends.filter(b => b.healthy).length;
        const status = healthyCount > 0 ? 200 : 503;
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: healthyCount > 0 ? 'ok' : 'degraded',
          healthyBackends: healthyCount,
          totalBackends: backends.length
        }));
        return;
      }

      if (url === '/metrics') {
        const mem = process.memoryUsage();
        const metrics = {
          service: 'tcp-load-balancer',
          algorithm: ALGO,
          tcpPort: TCP_PORT,
          httpPort: HTTP_PORT,
          activeClientConnections: activeIncomingConns,
          totalClientConnections: totalIncomingConns,
          totalBytesBridgedMB: parseFloat((totalBytesBridged / (1024 * 1024)).toFixed(2)),
          backends: backends.map(b => ({
            id: b.id,
            endpoint: `${b.host}:${b.tcpPort}`,
            healthy: b.healthy,
            activeConnections: b.activeConns,
            totalConnections: b.totalConns
          })),
          memory: {
            rssMB: parseFloat((mem.rss / (1024 * 1024)).toFixed(2)),
            heapUsedMB: parseFloat((mem.heapUsed / (1024 * 1024)).toFixed(2))
          },
          cpuPercent: parseFloat(currentCpuPercent),
          pid: process.pid,
          uptimeSeconds: Math.floor(process.uptime())
        };

        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache, no-store'
        });
        res.end(JSON.stringify(metrics, null, 2));
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not Found' }));
    });

    // Periodic Terminal Status Monitor
    setInterval(() => {
      if (activeIncomingConns > 0 || totalIncomingConns > 0) {
        const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
        const distStr = backends.map(b => `${b.id}(${b.healthy ? '✓' : '✗'}): ${b.activeConns}`).join(' | ');
        console.log(`[${timestamp}] [LB :${TCP_PORT}] Total Active: ${activeIncomingConns.toString().padStart(4)} | Nodes -> [${distStr}]`);
      }
    }, 1000);
  }

  // Start listeners
  tcpServer.listen(TCP_PORT, HOST, () => {
    if (cluster.isWorker) {
      console.log(`[LB Worker PID:${process.pid}] Listening on tcp://${HOST}:${TCP_PORT}`);
    } else if (httpServer) {
      httpServer.listen(HTTP_PORT, HOST, () => {
        console.log(`=======================================================`);
        console.log(`⚖️  L4 TCP Load Balancer Online (Single-Process PID: ${process.pid})`);
        console.log(`   TCP Entry Point    : tcp://${HOST}:${TCP_PORT}`);
        console.log(`   Algorithm          : ${ALGO}`);
        console.log(`   HTTP Metrics Port  : http://${HOST}:${HTTP_PORT}/metrics`);
        console.log(`   Backends Monitored : ${backends.map(b => `${b.id} (:700${b.id[1]})`).join(', ')}`);
        console.log(`=======================================================`);
      });
    }
  });

  // Graceful shutdown
  function shutdown() {
    console.log(`\nShutting down Load Balancer PID ${process.pid}...`);
    if (healthInterval) clearInterval(healthInterval);
    tcpServer.close(() => {
      if (httpServer) {
        httpServer.close(() => {
          console.log(`Load Balancer offline. Bye!`);
          process.exit(0);
        });
      } else {
        process.exit(0);
      }
    });
    setTimeout(() => process.exit(0), 2000).unref();
  }

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
