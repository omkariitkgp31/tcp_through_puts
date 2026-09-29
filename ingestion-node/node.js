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
let ACK_EVERY = parseInt(getArg('--ack-every', process.env.ACK_EVERY || '1'), 10);
let PARSE_MODE = getArg('--parse-mode', process.env.PARSE_MODE || 'regex');

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
// Sequence Extraction Functions (Regex & Buffer Scanning)
// -------------------------------------------------------------
function extractSeqRegex(line) {
  const match = /(?:^|[^a-zA-Z0-9_])"?seq"?\s*[:= ]\s*(\d+)/i.exec(line);
  return match ? parseInt(match[1], 10) : null;
}

function isAlphaNumericOrUnderscore(byte) {
  return (
    (byte >= 0x30 && byte <= 0x39) || // 0-9
    (byte >= 0x41 && byte <= 0x5A) || // A-Z
    (byte >= 0x61 && byte <= 0x7A) || // a-z
    byte === 0x5F                      // _
  );
}

function isWhitespace(byte) {
  return byte === 0x20 || byte === 0x09 || byte === 0x0D || byte === 0x0A;
}

function extractSeqBuffer(buf, start = 0, end = buf.length) {
  const len = end - 2; // need at least 3 chars for 'seq'
  for (let i = start; i < len; i++) {
    const b0 = buf[i];
    if (b0 === 0x73 || b0 === 0x53) { // 's' or 'S'
      const b1 = buf[i + 1];
      if (b1 === 0x65 || b1 === 0x45) { // 'e' or 'E'
        const b2 = buf[i + 2];
        if (b2 === 0x71 || b2 === 0x51) { // 'q' or 'Q'
          // Preceding boundary check: (?:^|[^a-zA-Z0-9_])
          let validStart = false;
          if (i === start) {
            validStart = true;
          } else {
            const prev = buf[i - 1];
            if (prev === 0x22) { // '"'
              if (i - 1 === start || !isAlphaNumericOrUnderscore(buf[i - 2])) {
                validStart = true;
              }
            } else if (!isAlphaNumericOrUnderscore(prev)) {
              validStart = true;
            }
          }

          if (!validStart) continue;

          let p = i + 3;
          // Trailing quote if any
          if (p < end && buf[p] === 0x22) { // '"'
            p++;
          }

          // Followed by alphanumeric or underscore means not a boundary
          if (p < end && isAlphaNumericOrUnderscore(buf[p])) {
            continue;
          }

          // Skip whitespace
          while (p < end && isWhitespace(buf[p])) {
            p++;
          }

          // Separator: ':', '=', or whitespace was already separator
          if (p < end && (buf[p] === 0x3A || buf[p] === 0x3D)) { // ':' or '='
            p++;
            while (p < end && isWhitespace(buf[p])) {
              p++;
            }
          }

          // Parse digits via byte arithmetic (no toString, no regex)
          if (p < end && buf[p] >= 0x30 && buf[p] <= 0x39) {
            let seq = 0;
            let digitCount = 0;
            while (p < end && buf[p] >= 0x30 && buf[p] <= 0x39) {
              seq = seq * 10 + (buf[p] - 0x30);
              digitCount++;
              p++;
            }
            if (digitCount > 0) {
              return seq;
            }
          }
        }
      }
    }
  }
  return null;
}

// -------------------------------------------------------------
// 1. Raw TCP Server
// -------------------------------------------------------------
const tcpServer = net.createServer({ noDelay: true, keepAlive: true }, (socket) => {
  activeConnections++;
  totalConnections++;

  socket.setKeepAlive(true, 10000);
  socket.setNoDelay(true);

  // Parsing buffers for both modes
  let strBuf = '';
  let remainderBuf = null;

  socket.on('data', (chunk) => {
    totalBytes += chunk.length;
    bytesInLastSecond += chunk.length;

    if (PARSE_MODE === 'buffer') {
      const dataBuf = remainderBuf ? Buffer.concat([remainderBuf, chunk]) : chunk;
      remainderBuf = null;

      let start = 0;
      let newlineIdx;
      while ((newlineIdx = dataBuf.indexOf(0x0A, start)) !== -1) {
        if (newlineIdx > start) {
          totalMessages++;
          messagesInLastSecond++;

          const seq = extractSeqBuffer(dataBuf, start, newlineIdx);
          if (seq !== null && socket.writable) {
            if (ACK_EVERY <= 1 || seq % ACK_EVERY === 0) {
              socket.write('ACK ' + seq + '\n');
            }
          }
        }
        start = newlineIdx + 1;
      }

      if (start < dataBuf.length) {
        if (dataBuf.length - start > 65536) {
          remainderBuf = null;
        } else {
          remainderBuf = dataBuf.subarray(start);
        }
      }
    } else {
      // Existing regex path
      strBuf += chunk.toString('utf8');
      let idx;
      while ((idx = strBuf.indexOf('\n')) !== -1) {
        const line = strBuf.slice(0, idx);
        strBuf = strBuf.slice(idx + 1);
        if (line.length === 0) continue;

        totalMessages++;
        messagesInLastSecond++;

        const seq = extractSeqRegex(line);
        if (seq !== null && socket.writable) {
          if (ACK_EVERY <= 1 || seq % ACK_EVERY === 0) {
            socket.write('ACK ' + seq + '\n');
          }
        }
      }
      if (strBuf.length > 65536) strBuf = '';
    }
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

  if (url === '/config') {
    if (req.method === 'POST') {
      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', () => {
        try {
          const cfg = JSON.parse(body || '{}');
          if (typeof cfg.ackEvery === 'number' && cfg.ackEvery >= 1) {
            ACK_EVERY = cfg.ackEvery;
          }
          if (typeof cfg.parseMode === 'string') {
            PARSE_MODE = cfg.parseMode;
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'ok', ackEvery: ACK_EVERY, parseMode: PARSE_MODE }));
        } catch (e) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ackEvery: ACK_EVERY, parseMode: PARSE_MODE }));
      return;
    }
  }

  if (url === '/metrics') {
    const mem = process.memoryUsage();
    const metricsData = {
      nodeId: NODE_ID,
      pid: process.pid,
      tcpPort: TCP_PORT,
      httpPort: HTTP_PORT,
      ackEvery: ACK_EVERY,
      parseMode: PARSE_MODE,
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
  res.end(JSON.stringify({ error: 'Not Found', endpoints: ['/health', '/metrics', '/config'] }));
});

httpServer.on('error', (err) => {
  console.error(`[${NODE_ID} HTTP Server] Error: ${err.message}`);
});

// Start listeners & monitoring when run directly
if (require.main === module) {
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
}

module.exports = {
  extractSeqRegex,
  extractSeqBuffer
};
