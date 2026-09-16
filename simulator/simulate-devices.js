#!/usr/bin/env node
/**
 * simulator/simulate-devices.js - IoT Device TCP Connection Generator & Simulator
 * 
 * Simulates N concurrent IoT telemetry devices connecting to the L4 TCP Load Balancer
 * (or baseline node) and streaming periodic JSON telemetry heartbeats.
 */

const net = require('net');

// Parse CLI arguments
const args = process.argv.slice(2);
function getArg(flag, defaultVal) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultVal;
}

function hasArg(flag) {
  return args.includes(flag);
}

const TARGET_CONNECTIONS = parseInt(getArg('--connections', process.env.CONNECTIONS || '400'), 10);
const HOST = getArg('--host', process.env.HOST || '127.0.0.1');
const PORT = parseInt(getArg('--port', process.env.PORT || '7000'), 10); // Defaults to LB port 7000
const INTERVAL_MS = parseInt(getArg('--interval', process.env.INTERVAL || '1000'), 10);
const RAMP_RATE = parseInt(getArg('--ramp-rate', process.env.RAMP_RATE || '100'), 10); // conn/sec
const DURATION_SEC = parseInt(getArg('--duration', process.env.DURATION || '0'), 10); // 0 = indefinite
const AUTO_RECONNECT = hasArg('--reconnect') || process.env.AUTO_RECONNECT === 'true';

// Statistics
let attempted = 0;
let established = 0;
let failed = 0;
let disconnected = 0;
let reconnected = 0;
let totalMessagesSent = 0;
let totalBytesSent = 0;
let messagesInLastSecond = 0;
let bytesInLastSecond = 0;
let currentMsgRate = 0;

const sockets = [];
const intervals = [];
let isShuttingDown = false;
const startTime = Date.now();

console.log(`=======================================================`);
console.log(`⚡ IoT Device TCP Connection Simulator (Phase 4)`);
console.log(`   Target Endpoint    : tcp://${HOST}:${PORT}`);
console.log(`   Target Devices     : ${TARGET_CONNECTIONS} concurrent sockets`);
console.log(`   Heartbeat Interval : ${INTERVAL_MS} ms`);
console.log(`   Ramp Rate          : ${RAMP_RATE} connections/sec`);
console.log(`   Auto-Reconnect     : ${AUTO_RECONNECT ? 'Enabled' : 'Disabled'}`);
console.log(`   Duration           : ${DURATION_SEC > 0 ? DURATION_SEC + ' seconds' : 'Indefinite (Ctrl+C to stop)'}`);
console.log(`=======================================================`);

// Create simulated device socket
function createDevice(deviceId) {
  if (isShuttingDown) return;

  attempted++;
  const socket = new net.Socket();
  socket.setNoDelay(true);
  socket.setKeepAlive(true, 10000);

  let seq = 0;
  let timer = null;
  let isConnected = false;

  socket.connect(PORT, HOST, () => {
    isConnected = true;
    established++;
    sockets.push(socket);

    // Initial heartbeat
    sendHeartbeat();

    // Periodic telemetry heartbeat stream
    timer = setInterval(sendHeartbeat, INTERVAL_MS);
    intervals.push(timer);
  });

  function sendHeartbeat() {
    if (socket.destroyed || isShuttingDown || !isConnected) return;
    seq++;

    // Realistic IoT telemetry payload
    const payload = JSON.stringify({
      deviceId: deviceId,
      ts: Date.now(),
      seq: seq,
      temp: parseFloat((22.0 + Math.random() * 5.0).toFixed(2)),
      battery: parseFloat((95.0 - (seq * 0.01)).toFixed(1)),
      status: 'nominal'
    }) + '\n';

    socket.write(payload, () => {
      totalMessagesSent++;
      messagesInLastSecond++;
      totalBytesSent += payload.length;
      bytesInLastSecond += payload.length;
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
    if (isConnected) {
      isConnected = false;
      established = Math.max(0, established - 1);
      disconnected++;

      if (AUTO_RECONNECT && !isShuttingDown) {
        reconnected++;
        setTimeout(() => createDevice(deviceId), 1000);
      }
    }
  });
}

// Smooth ramp-up of device connections
let currentDeviceIndex = 0;
const rampDelayMs = Math.max(5, Math.floor(1000 / RAMP_RATE));

function rampNext() {
  if (isShuttingDown) return;
  if (currentDeviceIndex < TARGET_CONNECTIONS) {
    currentDeviceIndex++;
    const deviceId = `iot-${currentDeviceIndex.toString().padStart(6, '0')}`;
    createDevice(deviceId);
    setTimeout(rampNext, rampDelayMs);
  }
}

rampNext();

// Live Terminal Dashboard Monitor
const monitorTimer = setInterval(() => {
  currentMsgRate = messagesInLastSecond;
  const currentKbRate = (bytesInLastSecond / 1024).toFixed(1);
  messagesInLastSecond = 0;
  bytesInLastSecond = 0;

  const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);

  // Visual progress bar for connection ramp
  const progressRatio = Math.min(1, established / TARGET_CONNECTIONS);
  const barLength = 20;
  const filled = Math.round(barLength * progressRatio);
  const bar = '█'.repeat(filled) + '░'.repeat(barLength - filled);

  process.stdout.write(
    `\r[${timestamp}] [${bar}] ` +
    `Connected: ${established.toString().padStart(4)}/${TARGET_CONNECTIONS} | ` +
    `Rate: ${currentMsgRate.toString().padStart(5)} msg/s (${currentKbRate.padStart(5)} KB/s) | ` +
    `Failed: ${failed.toString().padStart(2)} | ` +
    `Elapsed: ${elapsedSec.toString().padStart(3)}s`
  );

  if (DURATION_SEC > 0 && elapsedSec >= DURATION_SEC) {
    printSummaryAndExit();
  }
}, 1000);

// Summary reporting
function printSummaryAndExit() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  clearInterval(monitorTimer);

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const avgRate = totalElapsed > 0 ? (totalMessagesSent / totalElapsed).toFixed(1) : 0;
  const totalMB = (totalBytesSent / (1024 * 1024)).toFixed(2);
  const successPct = attempted > 0 ? ((established / attempted) * 100).toFixed(2) : '0.00';

  console.log(`\n\n==================== SIMULATION SUMMARY ====================`);
  console.log(` Target Server               : tcp://${HOST}:${PORT}`);
  console.log(` Test Duration               : ${totalElapsed} seconds`);
  console.log(` Connections Attempted       : ${attempted}`);
  console.log(` Connections Established     : ${established}`);
  console.log(` Connections Failed/Refused  : ${failed}`);
  console.log(` Connections Reconnected     : ${reconnected}`);
  console.log(` Connections Disconnected    : ${disconnected}`);
  console.log(` Total Telemetry Sent        : ${totalMessagesSent} messages (${totalMB} MB)`);
  console.log(` Average Message Throughput  : ${avgRate} msgs/sec`);
  console.log(` Connection Success Rate     : ${successPct}%`);
  console.log(`============================================================`);

  console.log(`\nTearing down simulated sockets...`);
  intervals.forEach((t) => clearInterval(t));
  sockets.forEach((s) => s.destroy());

  setTimeout(() => {
    process.exit(0);
  }, 500).unref();
}

process.on('SIGINT', printSummaryAndExit);
process.on('SIGTERM', printSummaryAndExit);
