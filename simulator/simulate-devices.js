#!/usr/bin/env node
/**
 * simulator/simulate-devices.js - IoT Device TCP Connection Generator & Simulator
 * 
 * Simulates N concurrent IoT telemetry devices connecting to the L4 TCP Load Balancer
 * (or baseline node) and streaming periodic JSON telemetry heartbeats.
 * 
 * Measures client-side T_conn (handshake & routing latency) and T_data (end-to-end RTT)
 * using HDR histograms (hdr-histogram-js), with backpressure handling and error tracking.
 */

const net = require('net');
const hdr = require('hdr-histogram-js');

function createHistogram() {
  return hdr.build({
    lowestDiscernibleValue: 1, // 1 microsecond
    highestTrackableValue: 60_000_000, // 60 seconds (60,000,000 us)
    numberOfSignificantValueDigits: 3
  });
}

function recordSafe(hist, valUs) {
  if (!hist) return;
  const v = Math.max(1, Math.min(60_000_000, Math.round(valUs)));
  hist.recordValue(v);
}

function extractPercentiles(hist) {
  if (!hist || hist.totalCount === 0) {
    return { p50: 0, p90: 0, p95: 0, p99: 0, p99_9: 0, p99_99: 0, count: 0 };
  }
  return {
    count: hist.totalCount,
    p50: +(hist.getValueAtPercentile(50) / 1000).toFixed(3),
    p90: +(hist.getValueAtPercentile(90) / 1000).toFixed(3),
    p95: +(hist.getValueAtPercentile(95) / 1000).toFixed(3),
    p99: +(hist.getValueAtPercentile(99) / 1000).toFixed(3),
    p99_9: +(hist.getValueAtPercentile(99.9) / 1000).toFixed(3),
    p99_99: +(hist.getValueAtPercentile(99.99) / 1000).toFixed(3)
  };
}

class DeviceSimulator {
  constructor(options = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 7000;
    this.targetConnections = options.targetConnections || 400;
    this.intervalMs = options.intervalMs || 1000;
    this.rampRate = options.rampRate || 100;
    this.autoReconnect = options.autoReconnect || false;
    this.collectingSamples = options.collectingSamples !== undefined ? options.collectingSamples : true;

    // HDR Histograms (recorded in microseconds)
    this.tConnHist = createHistogram();
    this.tDataHist = createHistogram();
    this.stageTConnHist = createHistogram();
    this.stageTDataHist = createHistogram();

    // Connection & Message Stats
    this.attempted = 0;
    this.established = 0;
    this.failed = 0;
    this.disconnected = 0;
    this.reconnected = 0;
    this.totalMessagesSent = 0;
    this.totalBytesSent = 0;
    this.messagesInLastSecond = 0;
    this.bytesInLastSecond = 0;
    this.currentMsgRate = 0;

    // Error categorisation
    this.errorCounts = {
      ECONNRESET: 0,
      EPIPE: 0,
      ETIMEDOUT: 0,
      other: 0
    };

    this.sockets = new Set();
    this.intervals = new Set();
    this.isShuttingDown = false;
    this.currentDeviceIndex = 0;
    this.rampTimeout = null;
  }

  start() {
    this.isShuttingDown = false;
    this.rampNext();
  }

  scaleTo(newTarget) {
    this.targetConnections = newTarget;
    if (this.currentDeviceIndex < this.targetConnections) {
      this.rampNext();
    }
  }

  resetHistograms() {
    this.tConnHist.reset();
    this.tDataHist.reset();
    this.stageTConnHist.reset();
    this.stageTDataHist.reset();
  }

  resetStageHistograms() {
    this.stageTConnHist.reset();
    this.stageTDataHist.reset();
  }

  getPercentiles() {
    return {
      tConn: extractPercentiles(this.tConnHist),
      tData: extractPercentiles(this.tDataHist)
    };
  }

  getStagePercentiles() {
    return {
      tConn: extractPercentiles(this.stageTConnHist),
      tData: extractPercentiles(this.stageTDataHist)
    };
  }

  getErrorCounts() {
    return { ...this.errorCounts, totalErrors: this.errorCounts.ECONNRESET + this.errorCounts.EPIPE + this.errorCounts.ETIMEDOUT + this.errorCounts.other };
  }

  rampNext() {
    if (this.isShuttingDown) return;
    if (this.currentDeviceIndex < this.targetConnections) {
      this.currentDeviceIndex++;
      const deviceId = `iot-${this.currentDeviceIndex.toString().padStart(6, '0')}`;
      this.createDevice(deviceId);
      const rampDelayMs = Math.max(5, Math.floor(1000 / this.rampRate));
      this.rampTimeout = setTimeout(() => this.rampNext(), rampDelayMs);
    }
  }

  async waitForConnections(target, timeoutMs = 30000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (this.isShuttingDown) break;
      if (this.established >= target) break;
      if (this.attempted >= target && (this.established + this.failed >= target)) {
        break;
      }
      await new Promise(r => setTimeout(r, 50));
    }
    return this.established;
  }

  createDevice(deviceId) {
    if (this.isShuttingDown) return;

    this.attempted++;
    const socket = new net.Socket();
    socket.setNoDelay(true); // Disable Nagle's algorithm
    socket.setKeepAlive(true, 10000);

    let seq = 0;
    let timer = null;
    let isConnected = false;
    let isPaused = false;
    let recvBuf = '';
    const dispatchMap = new Map();

    const connectStart = process.hrtime.bigint();

    socket.connect(this.port, this.host, () => {
      if (this.isShuttingDown) {
        socket.destroy();
        return;
      }
      const connectNs = process.hrtime.bigint() - connectStart;
      recordSafe(this.tConnHist, Number(connectNs) / 1000);
      recordSafe(this.stageTConnHist, Number(connectNs) / 1000);

      isConnected = true;
      this.established++;
      this.sockets.add(socket);

      // Initial heartbeat
      sendHeartbeat();

      // Periodic telemetry heartbeat stream
      timer = setInterval(sendHeartbeat, this.intervalMs);
      this.intervals.add(timer);
    });

    const sendHeartbeat = () => {
      if (socket.destroyed || this.isShuttingDown || !isConnected || isPaused) return;
      seq++;

      const payload = JSON.stringify({
        deviceId: deviceId,
        ts: Date.now(),
        seq: seq,
        temp: parseFloat((22.0 + Math.random() * 5.0).toFixed(2)),
        battery: parseFloat((95.0 - (seq * 0.01)).toFixed(1)),
        status: 'nominal',

        location: {
          latitude: 22.5726 + Math.random() * 0.01,
          longitude: 88.3639 + Math.random() * 0.01,
          altitude: 12.5,
          speed: parseFloat((Math.random() * 80).toFixed(2)),
          heading: parseFloat((Math.random() * 360).toFixed(2))
        },

        sensors: {
          humidity: parseFloat((40 + Math.random() * 30).toFixed(2)),
          pressure: parseFloat((990 + Math.random() * 30).toFixed(2)),
          vibration: parseFloat((Math.random() * 10).toFixed(3)),
          acceleration: {
            x: parseFloat((Math.random() * 4 - 2).toFixed(3)),
            y: parseFloat((Math.random() * 4 - 2).toFixed(3)),
            z: parseFloat((Math.random() * 4 - 2).toFixed(3))
          },
          gyroscope: {
            x: parseFloat((Math.random() * 360 - 180).toFixed(2)),
            y: parseFloat((Math.random() * 360 - 180).toFixed(2)),
            z: parseFloat((Math.random() * 360 - 180).toFixed(2))
          }
        },

        engine: {
          rpm: Math.floor(800 + Math.random() * 3000),
          coolantTemp: parseFloat((70 + Math.random() * 20).toFixed(2)),
          oilPressure: parseFloat((30 + Math.random() * 20).toFixed(2)),
          fuelLevel: parseFloat((20 + Math.random() * 80).toFixed(2)),
          load: parseFloat((Math.random() * 100).toFixed(2))
        },

        diagnostics: {
          signalStrength: Math.floor(-100 + Math.random() * 60),
          packetLoss: parseFloat((Math.random() * 5).toFixed(2)),
          firmwareVersion: 'v2.4.1',
          uptime: seq * 1000,
          errors: [],
          warnings: []
        },

        metadata: {
          manufacturer: 'IoT-Simulator',
          model: 'Telemetry-X100',
          region: 'IN-EAST',
          protocol: 'TCP',
          encryption: 'TLS',
          network: '4G',
          timestampSource: 'device',
          testRun: 'payload-stress-01'
        }
      }) + '\n';

      const dispatchTime = process.hrtime.bigint();
      dispatchMap.set(seq, dispatchTime);

      const canWrite = socket.write(payload, () => {
        this.totalMessagesSent++;
        this.messagesInLastSecond++;
        this.totalBytesSent += payload.length;
        this.bytesInLastSecond += payload.length;
      });

      // Handle backpressure
      if (!canWrite) {
        isPaused = true;
        socket.once('drain', () => {
          isPaused = false;
        });
      }
    };

    socket.on('data', (chunk) => {
      recvBuf += chunk.toString('utf8');
      let idx;
      while ((idx = recvBuf.indexOf('\n')) !== -1) {
        const line = recvBuf.slice(0, idx).trim();
        recvBuf = recvBuf.slice(idx + 1);
        if (!line) continue;

        const match = /^ACK\s+(\d+)$/i.exec(line);
        if (match) {
          const ackSeq = parseInt(match[1], 10);
          const sendTime = dispatchMap.get(ackSeq);
          if (sendTime !== undefined) {
            const rttNs = process.hrtime.bigint() - sendTime;
            if (this.collectingSamples) {
              recordSafe(this.tDataHist, Number(rttNs) / 1000);
              recordSafe(this.stageTDataHist, Number(rttNs) / 1000);
            }
            dispatchMap.delete(ackSeq);
          }
        }
      }
      if (recvBuf.length > 65536) recvBuf = '';
    });

    socket.on('error', (err) => {
      if (err.code === 'ECONNRESET') {
        this.errorCounts.ECONNRESET++;
      } else if (err.code === 'EPIPE') {
        this.errorCounts.EPIPE++;
      } else if (err.code === 'ETIMEDOUT') {
        this.errorCounts.ETIMEDOUT++;
      } else {
        this.errorCounts.other++;
      }

      if (!socket._hasFailed) {
        socket._hasFailed = true;
        this.failed++;
      }
    });

    socket.on('close', () => {
      dispatchMap.clear();
      if (timer) {
        clearInterval(timer);
        this.intervals.delete(timer);
      }
      this.sockets.delete(socket);

      if (isConnected) {
        isConnected = false;
        this.established = Math.max(0, this.established - 1);
        this.disconnected++;

        if (this.autoReconnect && !this.isShuttingDown) {
          this.reconnected++;
          setTimeout(() => this.createDevice(deviceId), 1000);
        }
      }
    });
  }

  stop() {
    this.isShuttingDown = true;
    if (this.rampTimeout) clearTimeout(this.rampTimeout);
    for (const timer of this.intervals) {
      clearInterval(timer);
    }
    this.intervals.clear();
    for (const socket of this.sockets) {
      socket.destroy();
    }
    this.sockets.clear();
  }
}

// -------------------------------------------------------------
// CLI Execution
// -------------------------------------------------------------
if (require.main === module) {
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
  const PORT = parseInt(getArg('--port', process.env.PORT || '7000'), 10);
  const INTERVAL_MS = parseInt(getArg('--interval', process.env.INTERVAL || '1000'), 10);
  const RAMP_RATE = parseInt(getArg('--ramp-rate', process.env.RAMP_RATE || '100'), 10);
  const DURATION_SEC = parseInt(getArg('--duration', process.env.DURATION || '0'), 10);
  const AUTO_RECONNECT = hasArg('--reconnect') || process.env.AUTO_RECONNECT === 'true';

  const simulator = new DeviceSimulator({
    host: HOST,
    port: PORT,
    targetConnections: TARGET_CONNECTIONS,
    intervalMs: INTERVAL_MS,
    rampRate: RAMP_RATE,
    autoReconnect: AUTO_RECONNECT
  });

  const startTime = Date.now();

  console.log(`=======================================================`);
  console.log(`⚡ IoT Device TCP Connection Simulator (Benchmarking Enabled)`);
  console.log(`   Target Endpoint    : tcp://${HOST}:${PORT}`);
  console.log(`   Target Devices     : ${TARGET_CONNECTIONS} concurrent sockets`);
  console.log(`   Heartbeat Interval : ${INTERVAL_MS} ms`);
  console.log(`   Ramp Rate          : ${RAMP_RATE} connections/sec`);
  console.log(`   Auto-Reconnect     : ${AUTO_RECONNECT ? 'Enabled' : 'Disabled'}`);
  console.log(`   Duration           : ${DURATION_SEC > 0 ? DURATION_SEC + ' seconds' : 'Indefinite (Ctrl+C to stop)'}`);
  console.log(`=======================================================`);

  simulator.start();

  const monitorTimer = setInterval(() => {
    simulator.currentMsgRate = simulator.messagesInLastSecond;
    const currentKbRate = (simulator.bytesInLastSecond / 1024).toFixed(1);
    simulator.messagesInLastSecond = 0;
    simulator.bytesInLastSecond = 0;

    const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
    const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);

    const progressRatio = Math.min(1, simulator.established / TARGET_CONNECTIONS);
    const barLength = 20;
    const filled = Math.round(barLength * progressRatio);
    const bar = '█'.repeat(filled) + '░'.repeat(barLength - filled);

    const percentiles = simulator.getPercentiles();
    process.stdout.write(
      `\r[${timestamp}] [${bar}] ` +
      `Conn: ${simulator.established.toString().padStart(4)}/${TARGET_CONNECTIONS} | ` +
      `Rate: ${simulator.currentMsgRate.toString().padStart(5)} msg/s (${currentKbRate.padStart(5)} KB/s) | ` +
      `T_conn P50: ${percentiles.tConn.p50}ms | T_data P50: ${percentiles.tData.p50}ms | ` +
      `Elapsed: ${elapsedSec.toString().padStart(3)}s`
    );

    if (DURATION_SEC > 0 && elapsedSec >= DURATION_SEC) {
      printSummaryAndExit();
    }
  }, 1000);

  function printSummaryAndExit() {
    clearInterval(monitorTimer);
    simulator.stop();

    const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const avgRate = totalElapsed > 0 ? (simulator.totalMessagesSent / totalElapsed).toFixed(1) : 0;
    const totalMB = (simulator.totalBytesSent / (1024 * 1024)).toFixed(2);
    const successPct = simulator.attempted > 0 ? ((simulator.established / simulator.attempted) * 100).toFixed(2) : '0.00';
    const percentiles = simulator.getPercentiles();
    const errors = simulator.getErrorCounts();

    console.log(`\n\n==================== SIMULATION SUMMARY ====================`);
    console.log(` Target Server               : tcp://${HOST}:${PORT}`);
    console.log(` Test Duration               : ${totalElapsed} seconds`);
    console.log(` Connections Attempted       : ${simulator.attempted}`);
    console.log(` Connections Established     : ${simulator.established}`);
    console.log(` Connections Failed/Refused  : ${simulator.failed}`);
    console.log(` Connections Reconnected     : ${simulator.reconnected}`);
    console.log(` Connections Disconnected    : ${simulator.disconnected}`);
    console.log(` Total Telemetry Sent        : ${simulator.totalMessagesSent} messages (${totalMB} MB)`);
    console.log(` Average Message Throughput  : ${avgRate} msgs/sec`);
    console.log(` Connection Success Rate     : ${successPct}%`);
    console.log(`-------------------- LATENCY PERCENTILES -------------------`);
    console.log(` T_conn (Handshake Latency)  : Samples: ${percentiles.tConn.count}`);
    console.log(`   P50: ${percentiles.tConn.p50}ms | P90: ${percentiles.tConn.p90}ms | P95: ${percentiles.tConn.p95}ms`);
    console.log(`   P99: ${percentiles.tConn.p99}ms | P99.9: ${percentiles.tConn.p99_9}ms | P99.99: ${percentiles.tConn.p99_99}ms`);
    console.log(` T_data (Application RTT)   : Samples: ${percentiles.tData.count}`);
    console.log(`   P50: ${percentiles.tData.p50}ms | P90: ${percentiles.tData.p90}ms | P95: ${percentiles.tData.p95}ms`);
    console.log(`   P99: ${percentiles.tData.p99}ms | P99.9: ${percentiles.tData.p99_9}ms | P99.99: ${percentiles.tData.p99_99}ms`);
    console.log(`---------------------- ERROR BREAKDOWN ---------------------`);
    console.log(` ECONNRESET: ${errors.ECONNRESET} | EPIPE: ${errors.EPIPE} | ETIMEDOUT: ${errors.ETIMEDOUT} | Other: ${errors.other}`);
    console.log(`============================================================`);

    setTimeout(() => {
      process.exit(0);
    }, 500).unref();
  }

  process.on('SIGINT', printSummaryAndExit);
  process.on('SIGTERM', printSummaryAndExit);
}

module.exports = {
  DeviceSimulator,
  createHistogram,
  extractPercentiles,
  recordSafe
};
