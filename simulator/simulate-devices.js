#!/usr/bin/env node
/**
 * simulate-devices.js - IoT Device TCP Connection Generator & Simulator
 * 
 * Simulates N concurrent IoT devices connecting over raw TCP and sending
 * periodic JSON heartbeat messages.
 */

const net = require('net');

// Parse CLI arguments
const args = process.argv.slice(2);
function getArg(flag, defaultVal) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultVal;
}

const TARGET_CONNECTIONS = parseInt(getArg('--connections', process.env.CONNECTIONS || '400'), 10);
const HOST = getArg('--host', process.env.HOST || '127.0.0.1');
const PORT = parseInt(getArg('--port', process.env.PORT || '7001'), 10);
const INTERVAL_MS = parseInt(getArg('--interval', process.env.INTERVAL || '1000'), 10);
const RAMP_RATE = parseInt(getArg('--ramp-rate', process.env.RAMP_RATE || '100'), 10); // conn/sec
const DURATION_SEC = parseInt(getArg('--duration', process.env.DURATION || '0'), 10); // 0 = indefinite

// Statistics
let attempted = 0;
let established = 0;
let failed = 0;
let disconnected = 0;
let totalMessagesSent = 0;
let messagesInLastSecond = 0;
let currentMsgRate = 0;

const sockets = [];
const intervals = [];
let isShuttingDown = false;
const startTime = Date.now();

console.log(`=======================================================`);
console.log(`⚡ IoT Device TCP Connection Simulator`);
console.log(`   Target Host        : tcp://${HOST}:${PORT}`);
console.log(`   Target Connections : ${TARGET_CONNECTIONS} devices`);
console.log(`   Heartbeat Interval : ${INTERVAL_MS} ms`);
console.log(`   Ramp Rate          : ${RAMP_RATE} connections/sec`);
console.log(`   Duration           : ${DURATION_SEC > 0 ? DURATION_SEC + 's' : 'Indefinite (Press Ctrl+C to stop)'}`);
console.log(`=======================================================`);

// Connect single device
function createDevice(deviceId) {
  if (isShuttingDown) return;

  attempted++;
  const socket = new net.Socket();
  socket.setNoDelay(true);
  socket.setKeepAlive(true, 10000);

  let seq = 0;
  let timer = null;

  socket.connect(PORT, HOST, () => {
    established++;
    sockets.push(socket);

    // Initial heartbeat
    sendHeartbeat();

    // Periodic heartbeat
    timer = setInterval(sendHeartbeat, INTERVAL_MS);
    intervals.push(timer);
  });

  function sendHeartbeat() {
    if (socket.destroyed || isShuttingDown) return;
    seq++;
    const payload = JSON.stringify({
      id: deviceId,
      ts: Date.now(),
      seq: seq
    }) + '\n';

    socket.write(payload, () => {
      totalMessagesSent++;
      messagesInLastSecond++;
    });
  }

  socket.on('error', (err) => {
    if (!socket._hasFailed) {
      socket._hasFailed = true;
      failed++;
    }
  });

  socket.on('close', () => {
    if (timer) clearInterval(timer);
    if (!socket._hasFailed && established > 0) {
      established = Math.max(0, established - 1);
      disconnected++;
    }
  });
}

// Smooth ramp-up of connections
let currentCount = 0;
const rampDelayMs = Math.max(10, Math.floor(1000 / RAMP_RATE));

function rampNext() {
  if (isShuttingDown) return;
  if (currentCount < TARGET_CONNECTIONS) {
    currentCount++;
    const deviceId = `dev-${currentCount.toString().padStart(6, '0')}`;
    createDevice(deviceId);
    setTimeout(rampNext, rampDelayMs);
  }
}

rampNext();

// Live terminal monitor
const monitorTimer = setInterval(() => {
  currentMsgRate = messagesInLastSecond;
  messagesInLastSecond = 0;

  const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(0);
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);

  process.stdout.write(
    `\r[${timestamp}] Elapsed: ${elapsedSec.padStart(3)}s | ` +
    `Connected: ${established.toString().padStart(5)}/${TARGET_CONNECTIONS} | ` +
    `Attempted: ${attempted.toString().padStart(5)} | ` +
    `Failed: ${failed.toString().padStart(4)} | ` +
    `Throughput: ${currentMsgRate.toString().padStart(6)} msg/s`
  );

  if (DURATION_SEC > 0 && parseInt(elapsedSec, 10) >= DURATION_SEC) {
    printSummaryAndExit();
  }
}, 1000);

function printSummaryAndExit() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  clearInterval(monitorTimer);

  console.log(`\n\n==================== SIMULATION SUMMARY ====================`);
  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const avgRate = totalElapsed > 0 ? (totalMessagesSent / totalElapsed).toFixed(1) : 0;

  console.log(` Target Server            : tcp://${HOST}:${PORT}`);
  console.log(` Total Time Elapsed       : ${totalElapsed} seconds`);
  console.log(` Connections Attempted    : ${attempted}`);
  console.log(` Connections Established  : ${established}`);
  console.log(` Connections Failed/Refused: ${failed}`);
  console.log(` Connections Disconnected : ${disconnected}`);
  console.log(` Total Heartbeats Sent    : ${totalMessagesSent}`);
  console.log(` Average Message Rate     : ${avgRate} msgs/sec`);
  console.log(` Success Rate             : ${attempted > 0 ? ((established / attempted) * 100).toFixed(2) : 0}%`);
  console.log(`============================================================`);

  console.log(`\nClosing client sockets...`);
  intervals.forEach((t) => clearInterval(t));
  sockets.forEach((s) => s.destroy());

  setTimeout(() => {
    process.exit(0);
  }, 500).unref();
}

process.on('SIGINT', printSummaryAndExit);
process.on('SIGTERM', printSummaryAndExit);
