'use strict';

const crypto = require('crypto');

/**
 * Heuristic checksum validators for blockchain addresses extracted by regex.
 * Each validator takes the raw matched string and returns true if it passes
 * the chain-specific checksum/structural test. All validators are total —
 * they return false on any malformed input rather than throwing.
 *
 * Covered:
 *   - Solana    : Base58 decode → must be exactly 32 bytes (Ed25519 pubkey).
 *   - Tron      : Base58Check — 25 bytes, prefix 0x41, SHA256(SHA256(first 21))[0:4] tail.
 *   - Cardano Shelley : Bech32 decode (hrp must start with "addr").
 *   - Cardano Byron   : Base58 → CBOR outer [tagged(24, bytes), crc32(u32)],
 *                       where crc32 of the untagged inner bytes must match.
 */

// ---------- Base58 ----------

const B58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const B58_MAP = (() => {
  const m = new Int8Array(128).fill(-1);
  for (let i = 0; i < B58_ALPHABET.length; i++) m[B58_ALPHABET.charCodeAt(i)] = i;
  return m;
})();

function base58Decode(s) {
  if (typeof s !== 'string' || s.length === 0) return null;
  let num = 0n;
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i);
    const v = code < 128 ? B58_MAP[code] : -1;
    if (v < 0) return null;
    num = num * 58n + BigInt(v);
  }
  let zeros = 0;
  while (zeros < s.length && s.charCodeAt(zeros) === 49 /* '1' */) zeros++;
  const tail = [];
  while (num > 0n) {
    tail.push(Number(num & 0xffn));
    num >>= 8n;
  }
  tail.reverse();
  return Buffer.concat([Buffer.alloc(zeros), Buffer.from(tail)]);
}

// ---------- Solana ----------

function validateSolana(addr) {
  const bytes = base58Decode(addr);
  return bytes !== null && bytes.length === 32;
}

// ---------- Tron ----------

function validateTron(addr) {
  const bytes = base58Decode(addr);
  if (!bytes || bytes.length !== 25) return false;
  if (bytes[0] !== 0x41) return false;
  const payload = bytes.subarray(0, 21);
  const checksum = bytes.subarray(21, 25);
  const h1 = crypto.createHash('sha256').update(payload).digest();
  const h2 = crypto.createHash('sha256').update(h1).digest();
  return h2.subarray(0, 4).equals(checksum);
}

// ---------- Bech32 (Cardano Shelley) ----------

const BECH32_ALPHABET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
const BECH32_MAP = (() => {
  const m = new Int8Array(128).fill(-1);
  for (let i = 0; i < BECH32_ALPHABET.length; i++) m[BECH32_ALPHABET.charCodeAt(i)] = i;
  return m;
})();
const BECH32_GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];

function bech32Polymod(values) {
  let chk = 1;
  for (let i = 0; i < values.length; i++) {
    const top = chk >>> 25;
    chk = (((chk & 0x1ffffff) << 5) >>> 0) ^ values[i];
    for (let j = 0; j < 5; j++) {
      if ((top >> j) & 1) chk ^= BECH32_GEN[j];
    }
    chk >>>= 0;
  }
  return chk;
}

function validateCardanoShelley(addr) {
  if (typeof addr !== 'string') return false;
  const pos = addr.lastIndexOf('1');
  if (pos < 1 || pos + 7 > addr.length) return false;
  const hrp = addr.substring(0, pos);
  if (!hrp.startsWith('addr')) return false;
  const data = addr.substring(pos + 1);
  const hrpExpanded = new Array(hrp.length * 2 + 1);
  for (let i = 0; i < hrp.length; i++) {
    const c = hrp.charCodeAt(i);
    if (c < 33 || c > 126) return false;
    hrpExpanded[i] = c >> 5;
    hrpExpanded[hrp.length + 1 + i] = c & 31;
  }
  hrpExpanded[hrp.length] = 0;
  const values = new Array(data.length);
  for (let i = 0; i < data.length; i++) {
    const code = data.charCodeAt(i);
    const v = code < 128 ? BECH32_MAP[code] : -1;
    if (v < 0) return false;
    values[i] = v;
  }
  return bech32Polymod(hrpExpanded.concat(values)) === 1;
}

// ---------- Cardano Byron (CBOR + CRC32) ----------

const CRC32_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    t[i] = c >>> 0;
  }
  return t;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = CRC32_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

// Minimal CBOR reader for the two shapes we need.
// Returns { major, info, len, value, next } for the head at offset p,
// where `value` is a Buffer for byte strings and a number for small uints.
function readCborHead(buf, p) {
  if (p >= buf.length) return null;
  const major = buf[p] >> 5;
  const info = buf[p] & 0x1f;
  let headerLen;
  let payload;
  if (info < 24) { payload = info; headerLen = 1; }
  else if (info === 24) {
    if (p + 1 >= buf.length) return null;
    payload = buf[p + 1]; headerLen = 2;
  }
  else if (info === 25) {
    if (p + 2 >= buf.length) return null;
    payload = (buf[p + 1] << 8) | buf[p + 2];
    headerLen = 3;
  }
  else if (info === 26) {
    if (p + 4 >= buf.length) return null;
    payload = ((buf[p + 1] * 0x1000000) + ((buf[p + 2] << 16) | (buf[p + 3] << 8) | buf[p + 4])) >>> 0;
    headerLen = 5;
  }
  else return null; // indefinite / 8-byte lengths not supported here
  return { major, info, payload, next: p + headerLen };
}

function validateCardanoByron(addr) {
  const bytes = base58Decode(addr);
  if (!bytes || bytes.length < 4) return false;

  // Outer: array of length 2 (major=4, info=2).
  if (bytes[0] !== 0x82) return false;
  let p = 1;

  // Element 0: tag 24 (major=6, value=24) = 0xd8 0x18
  if (p + 1 >= bytes.length || bytes[p] !== 0xd8 || bytes[p + 1] !== 0x18) return false;
  p += 2;

  // Then a byte string containing the inner CBOR.
  const head = readCborHead(bytes, p);
  if (!head || head.major !== 2) return false;
  const innerStart = head.next;
  const innerEnd = innerStart + head.payload;
  if (innerEnd > bytes.length) return false;
  const inner = bytes.subarray(innerStart, innerEnd);
  p = innerEnd;

  // Element 1: unsigned int (major=0) holding CRC32.
  const crcHead = readCborHead(bytes, p);
  if (!crcHead || crcHead.major !== 0) return false;
  if (crcHead.next !== bytes.length) return false;

  return crc32(inner) === crcHead.payload;
}

// ---------- Dispatch ----------

const VALIDATORS = {
  solana: validateSolana,
  tron: validateTron,
  cardano: validateCardanoShelley,
  cardano_legacy: validateCardanoByron
};

function validateAddress(chain, addr) {
  const fn = VALIDATORS[chain];
  if (!fn) return true; // no validator registered (e.g. ethereum) → accept
  try {
    return fn(addr);
  } catch {
    return false;
  }
}

module.exports = {
  validateAddress,
  validateSolana,
  validateTron,
  validateCardanoShelley,
  validateCardanoByron,
  // exported for tests
  base58Decode,
  crc32
};
