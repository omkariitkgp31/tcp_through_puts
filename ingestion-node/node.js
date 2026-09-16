#!/usr/bin/env node
/**
 * ingestion-node/node.js - Reusable Ingestion Node Module
 * 
 * Runs an ingestion node process handling raw TCP socket connections on a designated
 * TCP port, and exposing a lightweight HTTP /metrics and /health endpoint on an HTTP port.
 */

const net = require('net');
const http = require('http');

// Helper to extract CLI arguments or fallback to env vars
const args = process.argv.slice(2);
function getArg(flag, defaultVal) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultVal;
}

const NODE_ID = getArg('--node-id', process.env.NODE_ID || 'D1');
const TCP_PORT = parseInt(getArg('--tcp-port', process.env.TCP_PORT || '7001'), 10);
const HTTP_PORT = parseInt(getArg('--http-port', process.env.HTTP_PORT || '8001'), 10);
const HOST = getArg('--host', process.env.HOST || '0.0.0.0');

// Metrics state
let activeConnections = 0;
let totalConnections = 0;
let totalMessages = 0;
let totalBytes = 0;

let messagesInLastSecond = 0;
let bytesInLastSecond = 0;
let currentMsgRate = 0;
let currentByteRate = 0;

// CPU calculation state
let lastCpuUsage = process.cpuUsage();
let lastCpuTime = Date.now();
let currentCpuPercent = '0.0';

// -------------------------------------------------------------
// 1. Raw TCP Server
// -------------------------------------------------------------
const tcpServer = net.createServer({ noDelay: true, keepAlive: true }, (socket) => {
  activeConnections++;
  totalConnections++;

  socket.setKeepAlive(true, 10000);
  socket.setNoDelay(true);

  socket.on('data', (chunk) => {
    totalBytes += chunk.length;
    bytesInLastSecond += chunk.length;

    // Count newline-delimited messages or fallback to 1
    let count = 0;
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === 10) count++; // \n
    }
    const msgs = count > 0 ? count : 1;
    totalMessages += msgs;
    messagesInLastSecond += msgs;
  });

  socket.on('error', (err) => {
    if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') {
      console.error(`[${NODE_ID} TCP] Socket error: ${err.message}`);
    }
  });

  socket.on('close', () => {
    activeConnections = Math.max(0, activeConnections - 1);
  });
});

tcpServer.on('error', (err) => {
  console.error(`[${NODE_ID} TCP Server] Error: ${err.message}`);
  process.exit(1);
});

// -------------------------------------------------------------
// 2. HTTP Metrics & Health Server
// -------------------------------------------------------------
const httpServer = http.createServer((req, res) => {
  const url = req.url.split('?')[0];

  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      nodeId: NODE_ID,
      tcpPort: TCP_PORT,
      uptimeSeconds: process.uptime()
    }));
    return;
  }

  if (url === '/metrics') {
    const mem = process.memoryUsage();
    const metricsData = {
      nodeId: NODE_ID,
      pid: process.pid,
      tcpPort: TCP_PORT,
      httpPort: HTTP_PORT,
      activeConnections,
      totalConnections,
      messagesPerSec: currentMsgRate,
      bytesPerSec: currentByteRate,
      totalMessages,
      totalBytes,
      memory: {
        rssMB: parseFloat((mem.rss / (1024 * 1024)).toFixed(2)),
        heapUsedMB: parseFloat((mem.heapUsed / (1024 * 1024)).toFixed(2)),
        heapTotalMB: parseFloat((mem.heapTotal / (1024 * 1024)).toFixed(2)),
        externalMB: parseFloat((mem.external / (1024 * 1024)).toFixed(2))
      },
      cpuPercent: parseFloat(currentCpuPercent),
      uptimeSeconds: Math.floor(process.uptime())
    };

    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-cache, no-store'
    });
    res.end(JSON.stringify(metricsData, null, 2));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found', endpoints: ['/health', '/metrics'] }));
});

httpServer.on('error', (err) => {
  console.error(`[${NODE_ID} HTTP Server] Error: ${err.message}`);
});

// -------------------------------------------------------------
// 3. Periodic Telemetry Calculation & Logging
// -------------------------------------------------------------
setInterval(() => {
  currentMsgRate = messagesInLastSecond;
  currentByteRate = bytesInLastSecond;
  messagesInLastSecond = 0;
  bytesInLastSecond = 0;

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

  // Log heartbeat stats every 5s if active or regularly
  const mem = process.memoryUsage();
  const rssMB = (mem.rss / (1024 * 1024)).toFixed(1);
  const kbSec = (currentByteRate / 1024).toFixed(1);

  if (activeConnections > 0 || totalConnections > 0) {
    const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
    console.log(
      `[${timestamp}] [Node ${NODE_ID}] Active Sockets: ${activeConnections.toString().padStart(4)} | ` +
      `Rate: ${currentMsgRate.toString().padStart(5)} msg/s (${kbSec.padStart(5)} KB/s) | ` +
      `RSS: ${rssMB.padStart(5)} MB | CPU: ${currentCpuPercent.padStart(4)}%`
    );
  }
}, 1000);

// Start listeners
tcpServer.listen(TCP_PORT, HOST, () => {
  httpServer.listen(HTTP_PORT, HOST, () => {
    console.log(`=======================================================`);
    console.log(`📡 Ingestion Node [${NODE_ID}] Online`);
    console.log(`   TCP Ingestion Port : tcp://${HOST}:${TCP_PORT}`);
    console.log(`   HTTP Metrics Port  : http://${HOST}:${HTTP_PORT}/metrics`);
    console.log(`   HTTP Health Port   : http://${HOST}:${HTTP_PORT}/health`);
    console.log(`   Process PID        : ${process.pid}`);
    console.log(`=======================================================`);
  });
});

// Graceful shutdown
function shutdown() {
  console.log(`\nShutting down Node ${NODE_ID}...`);
  tcpServer.close(() => {
    httpServer.close(() => {
      console.log(`Node ${NODE_ID} offline. Bye!`);
      process.exit(0);
    });
  });
  setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
