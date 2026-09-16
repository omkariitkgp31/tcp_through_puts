#!/usr/bin/env node
/**
 * baseline-server.js - Baseline Single-Node TCP Ingestion Server
 * 
 * Accepts raw TCP connections, keeps them alive, and measures message throughput
 * and socket stability without any downstream business logic.
 */

const net = require('net');

// Parse CLI flags or env vars
const args = process.argv.slice(2);
function getArg(flag, defaultVal) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultVal;
}

const PORT = parseInt(getArg('--port', process.env.PORT || '7001'), 10);
const HOST = getArg('--host', process.env.HOST || '0.0.0.0');

let activeConnections = 0;
let totalConnections = 0;
let totalMessages = 0;
let totalBytes = 0;

let messagesInLastSecond = 0;
let bytesInLastSecond = 0;
let currentMsgRate = 0;
let currentByteRate = 0;

// Track CPU usage
let lastCpuUsage = process.cpuUsage();
let lastCpuTime = Date.now();
let currentCpuPercent = 0;

const server = net.createServer({ noDelay: true, keepAlive: true }, (socket) => {
  activeConnections++;
  totalConnections++;

  socket.setKeepAlive(true, 10000);
  socket.setNoDelay(true);

  // Ingestion path: count messages and bytes without deep parsing
  socket.on('data', (chunk) => {
    totalBytes += chunk.length;
    bytesInLastSecond += chunk.length;

    // Fast count: each newline or heartbeat packet counts as 1 message
    let count = 0;
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === 10) count++; // \n
    }
    const msgs = count > 0 ? count : 1;
    totalMessages += msgs;
    messagesInLastSecond += msgs;
  });

  socket.on('error', (err) => {
    // Suppress connection reset noise in high-volume testing
    if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') {
      console.error(`[Baseline] Socket error: ${err.message}`);
    }
  });

  socket.on('close', () => {
    activeConnections = Math.max(0, activeConnections - 1);
  });
});

server.on('error', (err) => {
  console.error(`[Baseline] Server error: ${err.message}`);
  process.exit(1);
});

// Periodic metrics logging
setInterval(() => {
  currentMsgRate = messagesInLastSecond;
  currentByteRate = bytesInLastSecond;
  messagesInLastSecond = 0;
  bytesInLastSecond = 0;

  // CPU measurement
  const cpuNow = process.cpuUsage();
  const timeNow = Date.now();
  const userDiff = cpuNow.user - lastCpuUsage.user;
  const sysDiff = cpuNow.system - lastCpuUsage.system;
  const timeDiff = (timeNow - lastCpuTime) * 1000; // in microseconds
  if (timeDiff > 0) {
    currentCpuPercent = (((userDiff + sysDiff) / timeDiff) * 100).toFixed(1);
  }
  lastCpuUsage = cpuNow;
  lastCpuTime = timeNow;

  const mem = process.memoryUsage();
  const rssMB = (mem.rss / (1024 * 1024)).toFixed(1);
  const heapMB = (mem.heapUsed / (1024 * 1024)).toFixed(1);
  const kbSec = (currentByteRate / 1024).toFixed(1);

  const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
  console.log(
    `[${timestamp}] [Baseline :${PORT}] Active: ${activeConnections.toString().padStart(4)} | ` +
    `Total: ${totalConnections.toString().padStart(5)} | ` +
    `Rate: ${currentMsgRate.toString().padStart(5)} msg/s (${kbSec.padStart(6)} KB/s) | ` +
    `RSS: ${rssMB.padStart(5)} MB (Heap: ${heapMB} MB) | CPU: ${currentCpuPercent.padStart(4)}%`
  );
}, 1000);

server.listen(PORT, HOST, () => {
  console.log(`=======================================================`);
  console.log(`🚀 Baseline Single-Node TCP Ingestion Server Running`);
  console.log(`   Listening on : tcp://${HOST}:${PORT}`);
  console.log(`   PID          : ${process.pid}`);
  console.log(`   Node Version : ${process.version}`);
  console.log(`=======================================================`);
});

// Graceful shutdown
function shutdown() {
  console.log(`\nShutting down baseline server...`);
  server.close(() => {
    console.log(`Server closed. Bye!`);
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
