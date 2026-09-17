#!/usr/bin/env node
/**
 * lb/tcp-load-balancer.js - L4 TCP Load Balancer
 * 
 * Accepts raw TCP connections on a single entry point (e.g. :7000) and distributes
 * them across healthy Ingestion Nodes (D1..Dn) using Round-Robin or Least-Connections.
 * 
 * Guarantees connection-level stickiness: Once a socket is assigned to a node,
 * it is directly piped to that node for the entire life of the socket.
 */

const net = require('net');
const http = require('http');

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

// Backend Ingestion Nodes Pool
const backends = [
  { id: 'D1', host: '127.0.0.1', tcpPort: 7001, httpPort: 8001, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
  { id: 'D2', host: '127.0.0.1', tcpPort: 7002, httpPort: 8002, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
  { id: 'D3', host: '127.0.0.1', tcpPort: 7003, httpPort: 8003, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null },
  { id: 'D4', host: '127.0.0.1', tcpPort: 7004, httpPort: 8004, healthy: true, activeConns: 0, totalConns: 0, lastCheck: null }
];

let roundRobinIndex = 0;
let totalIncomingConns = 0;
let activeIncomingConns = 0;
let totalBytesBridged = 0;

// -------------------------------------------------------------
// 1. Health Checker
// -------------------------------------------------------------
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

// Initial health check + interval
runHealthChecks();
const healthInterval = setInterval(runHealthChecks, 2000);

// -------------------------------------------------------------
// 2. Node Selection Strategy
// -------------------------------------------------------------
function getNextBackend() {
  const healthyBackends = backends.filter(b => b.healthy);
  if (healthyBackends.length === 0) {
    return null;
  }

  if (ALGO === 'least-connections') {
    // Pick healthy backend with smallest activeConns
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

// -------------------------------------------------------------
// 3. L4 TCP Proxy Server
// -------------------------------------------------------------
const tcpServer = net.createServer({ noDelay: true, keepAlive: true }, (clientSocket) => {
  totalIncomingConns++;
  activeIncomingConns++;

  clientSocket.setKeepAlive(true, 10000);
  clientSocket.setNoDelay(true);

  const targetBackend = getNextBackend();

  if (!targetBackend) {
    console.error(`[LB] No healthy backends available to handle connection! Dropping.`);
    clientSocket.destroy();
    activeIncomingConns = Math.max(0, activeIncomingConns - 1);
    return;
  }

  targetBackend.activeConns++;
  targetBackend.totalConns++;

  // Open dedicated upstream connection to selected backend
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
      console.error(`[LB Client Socket Error] ${err.message}`);
    }
    backendSocket.destroy();
    cleanup();
  });

  backendSocket.on('error', (err) => {
    if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') {
      console.error(`[LB Backend ${targetBackend.id} Socket Error] ${err.message}`);
    }
    clientSocket.destroy();
    cleanup();
  });

  clientSocket.on('close', cleanup);
  backendSocket.on('close', cleanup);
});

tcpServer.on('error', (err) => {
  console.error(`[LB TCP Server Error] ${err.message}`);
  process.exit(1);
});

// -------------------------------------------------------------
// 4. HTTP Metrics & Health Endpoint
// -------------------------------------------------------------
const httpServer = http.createServer((req, res) => {
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

// -------------------------------------------------------------
// 5. Periodic Terminal Status Monitor
// -------------------------------------------------------------
setInterval(() => {
  if (activeIncomingConns > 0 || totalIncomingConns > 0) {
    const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
    const distStr = backends.map(b => `${b.id}(${b.healthy ? '✓' : '✗'}): ${b.activeConns}`).join(' | ');
    console.log(`[${timestamp}] [LB :${TCP_PORT}] Total Active: ${activeIncomingConns.toString().padStart(4)} | Nodes -> [${distStr}]`);
  }
}, 1000);

// Start listeners
tcpServer.listen(TCP_PORT, HOST, () => {
  httpServer.listen(HTTP_PORT, HOST, () => {
    console.log(`=======================================================`);
    console.log(`⚖️  L4 TCP Load Balancer Online`);
    console.log(`   TCP Entry Point    : tcp://${HOST}:${TCP_PORT}`);
    console.log(`   Algorithm          : ${ALGO}`);
    console.log(`   HTTP Metrics Port  : http://${HOST}:${HTTP_PORT}/metrics`);
    console.log(`   Backends Monitored : ${backends.map(b => `${b.id} (:700${b.id[1]})`).join(', ')}`);
    console.log(`=======================================================`);
  });
});

// Graceful shutdown
function shutdown() {
  console.log(`\nShutting down Load Balancer...`);
  clearInterval(healthInterval);
  tcpServer.close(() => {
    httpServer.close(() => {
      console.log(`Load Balancer offline. Bye!`);
      process.exit(0);
    });
  });
  setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
