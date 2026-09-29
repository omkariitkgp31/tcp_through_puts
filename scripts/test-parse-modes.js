#!/usr/bin/env node
/**
 * scripts/test-parse-modes.js - Unit Test Suite for Sequence Extraction
 * 
 * Verifies that both PARSE_MODE=regex and PARSE_MODE=buffer produce identical
 * sequence values across standard payloads, edge cases, whitespace variations,
 * negative / false-positive cases, and merged multi-line chunks.
 */

const assert = require('assert');
const { extractSeqRegex, extractSeqBuffer } = require('../ingestion-node/node.js');

const testCases = [
  {
    name: 'Standard JSON payload',
    line: '{"deviceId":"iot-000001","ts":1727572392000,"seq":42,"temp":24.5}',
    expected: 42
  },
  {
    name: 'seq at start of line (unquoted with colon)',
    line: 'seq: 100',
    expected: 100
  },
  {
    name: 'seq at start of line (quoted with colon)',
    line: '"seq": 200',
    expected: 200
  },
  {
    name: 'seq at start of line with no whitespace',
    line: '"seq":300',
    expected: 300
  },
  {
    name: 'seq at end of line (trailing brace)',
    line: '{"deviceId":"iot-000001","temp":24.5,"seq":9999}',
    expected: 9999
  },
  {
    name: 'seq at end of line (unquoted, bare)',
    line: 'device=iot-001 seq: 8888',
    expected: 8888
  },
  {
    name: 'Extra whitespace around key, separator, and value',
    line: '   "seq"   :   123456   ',
    expected: 123456
  },
  {
    name: 'Whitespace with tabs and carriage return',
    line: '\t{\t"seq"\t:\t7777\r}',
    expected: 7777
  },
  {
    name: 'Separator with equals sign',
    line: 'deviceId=iot-1 seq=555 temp=30',
    expected: 555
  },
  {
    name: 'Large sequence number',
    line: '{"seq":2147483647,"status":"ok"}',
    expected: 2147483647
  },
  {
    name: 'Single digit zero sequence',
    line: '{"seq":0}',
    expected: 0
  },
  {
    name: 'Single digit non-zero sequence',
    line: '{"seq":9}',
    expected: 9
  },
  {
    name: 'Uppercase SEQ',
    line: '{"SEQ":50}',
    expected: 50
  },
  {
    name: 'Mixed case Seq',
    line: '{"Seq":60}',
    expected: 60
  },
  {
    name: 'Negative test: false positive with prefix (preseq)',
    line: '{"preseq": 999}',
    expected: null
  },
  {
    name: 'Negative test: false positive with underscore prefix (my_seq)',
    line: '{"my_seq": 999}',
    expected: null
  },
  {
    name: 'Negative test: false positive with suffix (sequence)',
    line: '{"sequence": 999}',
    expected: null
  },
  {
    name: 'Negative test: false positive with underscore suffix (seq_num)',
    line: '{"seq_num": 999}',
    expected: null
  },
  {
    name: 'Negative test: payload without seq field',
    line: '{"deviceId":"iot-000001","temp":22.5,"status":"nominal"}',
    expected: null
  },
  {
    name: 'Realistic simulator payload',
    line: JSON.stringify({
      deviceId: 'iot-000042',
      ts: Date.now(),
      seq: 10452,
      temp: 23.45,
      battery: 94.2,
      sensors: { humidity: 55.2, pressure: 1013.25 },
      metadata: { manufacturer: 'IoT-Simulator', protocol: 'TCP' }
    }),
    expected: 10452
  }
];

console.log('======================================================================');
console.log('🧪 UNIT TESTS: Sequence Extraction (PARSE_MODE=regex vs buffer)');
console.log('======================================================================\n');

let passedSingleLine = 0;

for (let i = 0; i < testCases.length; i++) {
  const tc = testCases[i];
  const buf = Buffer.from(tc.line + '\n', 'utf8');
  // Segment before newline
  const newlineIdx = buf.indexOf(0x0A);
  
  const regexResult = extractSeqRegex(tc.line);
  const bufferResult = extractSeqBuffer(buf, 0, newlineIdx);

  try {
    assert.strictEqual(
      regexResult,
      tc.expected,
      `[Regex] Expected ${tc.expected}, got ${regexResult}`
    );
    assert.strictEqual(
      bufferResult,
      tc.expected,
      `[Buffer] Expected ${tc.expected}, got ${bufferResult}`
    );
    assert.strictEqual(
      bufferResult,
      regexResult,
      `Parity mismatch between buffer (${bufferResult}) and regex (${regexResult})`
    );

    console.log(`  ✔ [Pass ${i + 1}/${testCases.length}] ${tc.name} -> Extracted: ${bufferResult}`);
    passedSingleLine++;
  } catch (err) {
    console.error(`  ❌ [FAIL ${i + 1}/${testCases.length}] ${tc.name}: ${err.message}`);
    process.exit(1);
  }
}

// -------------------------------------------------------------------
// Multi-Line Merged Chunks Test
// -------------------------------------------------------------------
console.log('\n----------------------------------------------------------------------');
console.log('🧪 MULTI-LINE CHUNK TEST: Merged Multi-Line Buffer Stream');
console.log('----------------------------------------------------------------------');

const multiLineChunk = [
  '{"deviceId":"iot-01","seq":101}',
  '   "seq"   :   102   ',
  'seq:103',
  '{"status":"nominal","seq":104}',
  '{"seq":105,"data":"trailing"}',
  '{"no_seq_here":true}',
  '{"seq":107}'
].join('\n') + '\n';

const rawChunkBuf = Buffer.from(multiLineChunk, 'utf8');

// Parse using Regex mode (line by line via toString)
const regexExtracted = [];
let strBuf = multiLineChunk;
let idx;
while ((idx = strBuf.indexOf('\n')) !== -1) {
  const line = strBuf.slice(0, idx);
  strBuf = strBuf.slice(idx + 1);
  if (line.length === 0) continue;
  regexExtracted.push(extractSeqRegex(line));
}

// Parse using Buffer mode (raw buffer scanning)
const bufferExtracted = [];
let start = 0;
let newlineIdx;
while ((newlineIdx = rawChunkBuf.indexOf(0x0A, start)) !== -1) {
  if (newlineIdx > start) {
    bufferExtracted.push(extractSeqBuffer(rawChunkBuf, start, newlineIdx));
  }
  start = newlineIdx + 1;
}

assert.deepStrictEqual(
  bufferExtracted,
  regexExtracted,
  'Buffer scanning results must match regex mode on merged multi-line chunks'
);

const expectedMulti = [101, 102, 103, 104, 105, null, 107];
assert.deepStrictEqual(bufferExtracted, expectedMulti);

console.log(`  ✔ Merged 7-line chunk parsed identically across both modes: [${bufferExtracted.join(', ')}]`);

console.log('\n======================================================================');
console.log(`🎉 ALL TESTS PASSED: ${passedSingleLine} single-line cases + 1 multi-line suite.`);
console.log('   Both PARSE_MODE=regex and PARSE_MODE=buffer exhibit 100% parity.');
console.log('======================================================================\n');
