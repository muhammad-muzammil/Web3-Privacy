/**
 * Crash-safe append-only buffer for MongoDB writes from the wallet crawler.
 *
 * Two files per instance live under BUFFER_DIR:
 *   ${instanceId}-crawl_domains.jsonl  (one line per pre-crawl domain stamp)
 *   ${instanceId}-crawls.jsonl         (one line per interesting crawl result)
 *
 * The hot path appends synchronously; the drain phase atomically renames the
 * active file to a sibling `.draining` file (so further appends start fresh)
 * then bulkWrites its contents to MongoDB. On crash mid-drain, the `.draining`
 * file plus a per-file sidecar offset survive and are resumed on next start.
 *
 * Domain timestamps are coalesced (latest per URL) at drain time because the
 * crawl_domains upsert is keyed by URL; crawl results are not coalesced
 * because each row carries its own followups[] payload.
 */

const fs = require('fs');
const path = require('path');
const { buildDomainTimestampOps, buildCrawlResultOps } = require('./mongodb.js');

// Hard cap on the size of any single JSONL line, both at write time and at
// drain time. Lines above this cap cannot be stored in MongoDB anyway
// (the BSON document limit is 16 MiB), and trying to load one as a single
// V8 string blows the heap. Anything larger is dropped with a warning.
// 12 MiB leaves headroom for BSON overhead under the Mongo 16 MiB ceiling.
const MAX_LINE_BYTES = 12 * 1024 * 1024;

let bufferDir = null;
let instanceId = null;
let flushThresholdBytes = 0;
let drainBatchSize = 500;
let drainWriteTimeoutMs = 30000;
let drainWriteRetries = 3;
let domainsActivePath = null;
let crawlsActivePath = null;

function init(opts) {
  bufferDir = opts.bufferDir;
  instanceId = opts.instanceId;
  flushThresholdBytes = opts.flushThresholdBytes;
  if (typeof opts.drainBatchSize === 'number' && opts.drainBatchSize > 0) {
    drainBatchSize = opts.drainBatchSize;
  }
  if (typeof opts.drainWriteTimeoutMs === 'number' && opts.drainWriteTimeoutMs > 0) {
    drainWriteTimeoutMs = opts.drainWriteTimeoutMs;
  }
  if (typeof opts.drainWriteRetries === 'number' && opts.drainWriteRetries >= 0) {
    drainWriteRetries = opts.drainWriteRetries;
  }

  fs.mkdirSync(bufferDir, { recursive: true });

  domainsActivePath = path.join(bufferDir, `${instanceId}-crawl_domains.jsonl`);
  crawlsActivePath = path.join(bufferDir, `${instanceId}-crawls.jsonl`);

  console.log(`Buffer ready: dir=${bufferDir} instance=${instanceId} threshold=${flushThresholdBytes}B batch=${drainBatchSize}`);
}

function appendDomainTimestamp(url) {
  const line = JSON.stringify({ url, ts: new Date().toISOString() }) + '\n';
  fs.appendFileSync(domainsActivePath, line);
}

function appendCrawlResult(doc) {
  const line = JSON.stringify(doc) + '\n';
  // Defense at the source: a single crawl record with a giant pageSrc or
  // responseBody can produce a line larger than the Mongo doc limit, which
  // would then OOM the drain on read-back (no streaming line reader can
  // emit a line without holding it in one string). Drop oversized records
  // with a loud warning so the operator notices.
  if (Buffer.byteLength(line, 'utf8') > MAX_LINE_BYTES) {
    console.error(`buffer: dropping oversized crawl record url=${doc && doc.url} size=${line.length}B (cap=${MAX_LINE_BYTES}B)`);
    return;
  }
  fs.appendFileSync(crawlsActivePath, line);
}

/**
 * Bounded raw-byte line streamer. Reads the file in chunks of `chunkBytes`,
 * splits on `\n` (0x0a) without ever materializing more than ~maxLineBytes
 * in memory, and yields one record per line:
 *
 *   { oversized: false, line: <string>, lineStart, lineEnd }
 *   { oversized: true,  line: null,     lineStart, lineEnd }
 *
 * `lineStart` is the file byte offset of the first byte of the line.
 * `lineEnd`   is the file byte offset just past the terminating `\n`
 *             (or end-of-file for a trailing newline-less line).
 *
 * Lines larger than maxLineBytes — whether through pathological payloads or
 * file corruption — are skipped without ever building the full string.
 * They still consume their byte range, so caller offset tracking stays
 * correct against the on-disk file.
 */
async function* streamLines(filePath, maxLineBytes) {
  const stream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });
  let leftover = null;        // Buffer holding incomplete trailing bytes
  let leftoverStart = 0;      // file offset where `leftover` begins
  let pos = 0;                // total bytes pulled from the file so far
  let skipping = false;       // true while discarding bytes of an oversized line
  let skipStart = 0;          // file offset where the current skip began

  for await (const chunk of stream) {
    let buf;
    let bufStart;
    if (leftover) {
      buf = Buffer.concat([leftover, chunk]);
      bufStart = leftoverStart;
      leftover = null;
    } else {
      buf = chunk;
      bufStart = pos;
    }
    pos = bufStart + buf.length;

    let i = 0;
    while (i < buf.length) {
      const nl = buf.indexOf(0x0a, i);
      if (nl === -1) break;

      if (skipping) {
        yield { oversized: true, line: null, lineStart: skipStart, lineEnd: bufStart + nl + 1 };
        skipping = false;
      } else if ((nl - i) > maxLineBytes) {
        yield { oversized: true, line: null, lineStart: bufStart + i, lineEnd: bufStart + nl + 1 };
      } else {
        yield {
          oversized: false,
          line: buf.slice(i, nl).toString('utf8'),
          lineStart: bufStart + i,
          lineEnd: bufStart + nl + 1
        };
      }
      i = nl + 1;
    }

    const remainderLen = buf.length - i;
    if (skipping) {
      // Drop the unterminated tail; we're already mid-skip.
    } else if (remainderLen > maxLineBytes) {
      // No newline yet and we've already buffered more than allowed.
      // Enter skip mode; the bytes already in `buf` are discarded.
      skipping = true;
      skipStart = bufStart + i;
    } else if (remainderLen > 0) {
      leftover = buf.slice(i);
      leftoverStart = bufStart + i;
    }
  }

  // EOF — emit any tail
  if (skipping) {
    yield { oversized: true, line: null, lineStart: skipStart, lineEnd: pos };
  } else if (leftover) {
    if (leftover.length > maxLineBytes) {
      yield { oversized: true, line: null, lineStart: leftoverStart, lineEnd: pos };
    } else {
      yield {
        oversized: false,
        line: leftover.toString('utf8'),
        lineStart: leftoverStart,
        lineEnd: pos
      };
    }
  }
}

function fileSize(p) {
  try {
    return fs.statSync(p).size;
  } catch (e) {
    if (e.code === 'ENOENT') return 0;
    throw e;
  }
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// A document/op that exceeds Mongo's 16 MiB BSON limit. This is deterministic
// — retrying never helps — so we isolate and drop it rather than wedging the
// drain. Covers the client-side serialize error ("Document is larger than the
// maximum size 16777216", code 10334) and the server-side resulting-doc error
// (code 17419).
function isOversizeError(e) {
  if (!e) return false;
  if (e.code === 10334 || e.code === 17419) return true;
  const msg = e.message ? e.message : String(e);
  return /larger than the maximum size|BSONObjectTooLarge|resulting document.*larger/i.test(msg);
}

function oversizedLogPath(collectionName) {
  return path.join(bufferDir, `${instanceId}-${collectionName}.oversized.jsonl`);
}

// Single bulkWrite attempt with a per-op `timeoutMS` (driver CSOT) plus a hard
// Promise.race backstop (timeoutMS + 5s) in case the driver's own timeout fails
// to fire. A leaked underlying op is acceptable versus a wedged consumer.
async function bulkWriteOnce(db, collectionName, ops) {
  let hardTimer;
  const hardTimeout = new Promise((_, rej) => {
    hardTimer = setTimeout(
      () => rej(new Error(`bulkWrite hard timeout after ${drainWriteTimeoutMs + 5000}ms`)),
      drainWriteTimeoutMs + 5000
    );
  });
  try {
    await Promise.race([
      db.collection(collectionName).bulkWrite(ops, { ordered: false, timeoutMS: drainWriteTimeoutMs }),
      hardTimeout
    ]);
  } finally {
    clearTimeout(hardTimer);
  }
}

// Fallback when a batch hits the BSON size limit: write each op individually so
// the good records still land and only the oversized one(s) are dropped (they
// can never be stored in Mongo regardless). Dropped records are logged and
// appended to a `.oversized.jsonl` sidecar for audit. A non-oversize error on a
// single op is rethrown so the outer drain fails and the `.draining` file
// replays as usual.
async function drainPerOpDroppingOversized(db, collectionName, ops, label) {
  let ok = 0;
  let dropped = 0;
  for (const op of ops) {
    try {
      await bulkWriteOnce(db, collectionName, [op]);
      ok++;
    } catch (e) {
      if (!isOversizeError(e)) throw e;
      const filter = (op.updateOne && op.updateOne.filter) || {};
      const url = filter.url || filter._id || '<unknown>';
      console.error(`buffer: dropping oversized ${label} record url=${url} (exceeds 16MiB BSON limit)`);
      try {
        fs.appendFileSync(
          oversizedLogPath(collectionName),
          JSON.stringify({ url, ts: new Date().toISOString(), label }) + '\n'
        );
      } catch (logErr) {
        console.error(`buffer: failed to record oversized ${label} url=${url}: ${logErr.message}`);
      }
      dropped++;
    }
  }
  console.error(`buffer: ${label} per-op fallback complete (${ok} written, ${dropped} dropped oversized)`);
}

/**
 * Run a bulkWrite with a per-attempt timeout and bounded retries. Two layers
 * of timeout guard against the original hang (a bulkWrite that never returns
 * against an unresponsive Mongo, which permanently pauses the consumer):
 *   1. per-operation `timeoutMS` (driver CSOT) aborts the op client-side;
 *   2. a hard Promise.race backstop (timeoutMS + 5s) rejects even if the
 *      driver's own timeout fails to fire. The underlying op may leak, which
 *      is acceptable — better a leaked op than a wedged consumer.
 * A BSON size-limit error is deterministic, so instead of retrying we fall back
 * to per-op writes that drop the oversized record (see
 * drainPerOpDroppingOversized). On other failures we back off (1s, 2s, 4s, …)
 * and retry; after the last attempt we throw so the caller (drainOne → drain →
 * runDrainPhase) logs it and resumes the consumer, leaving the `.draining` file
 * for replay.
 */
async function bulkWriteWithRetry(db, collectionName, ops, label) {
  const totalAttempts = drainWriteRetries + 1;
  let lastErr;
  for (let attempt = 1; attempt <= totalAttempts; attempt++) {
    try {
      await bulkWriteOnce(db, collectionName, ops);
      return;
    } catch (e) {
      if (isOversizeError(e)) {
        console.error(`buffer: ${label} batch hit BSON size limit — retrying per-op to isolate oversized records`);
        await drainPerOpDroppingOversized(db, collectionName, ops, label);
        return;
      }
      lastErr = e;
      const msg = e && e.message ? e.message : String(e);
      console.error(`buffer: ${label} bulkWrite attempt ${attempt}/${totalAttempts} failed: ${msg}`);
      if (attempt < totalAttempts) {
        await sleep(1000 * Math.pow(2, attempt - 1));
      }
    }
  }
  throw lastErr;
}

function shouldDrain() {
  return fileSize(domainsActivePath) >= flushThresholdBytes ||
         fileSize(crawlsActivePath) >= flushThresholdBytes;
}

function bufferedBytes() {
  return fileSize(domainsActivePath) + fileSize(crawlsActivePath);
}

/**
 * Drain one collection's `.draining` file (renaming the active file first if
 * no `.draining` already exists). Returns the number of records bulk-written.
 * Reads the file with a line-streaming reader so peak memory is bounded by
 * one line plus the in-flight bulkWrite batch — not the file size.
 *
 * For crawl_domains, lines are coalesced into a Map of size <= drainBatchSize
 * and flushed when full. `.draining.offset` is unused because the upsert is
 * fully idempotent on replay (sequential batches; last write wins per URL).
 *
 * For crawls, records are flushed in chunks of `drainBatchSize` and the
 * trailing byte offset of the last fully-flushed line is written to
 * `.draining.offset` after each chunk. On crash, resumeIncompleteDrain reads
 * that offset and re-runs only the in-flight chunk.
 */
async function drainOne(db, activePath, collectionName, coalesce) {
  const drainingPath = activePath + '.draining';
  const offsetPath = drainingPath + '.offset';

  if (!fs.existsSync(drainingPath)) {
    if (fileSize(activePath) === 0) return 0;
    fs.renameSync(activePath, drainingPath);
  }

  // The offset sidecar is only valid if it was written during the drain of
  // the current `.draining` file. A previous drain that crashed between
  // unlinking the draining file and unlinking its offset can leave an orphan
  // sidecar behind; trusting it would silently skip records in the new
  // draining file. Rename preserves mtime, so within a single drain the
  // offset's mtime (written during the drain) is strictly newer than the
  // draining file's mtime (carried over from the last pre-drain append).
  let startOffset = 0;
  if (fs.existsSync(offsetPath)) {
    try {
      const offsetMtime = fs.statSync(offsetPath).mtimeMs;
      const drainingMtime = fs.statSync(drainingPath).mtimeMs;
      if (offsetMtime > drainingMtime) {
        const val = parseInt(fs.readFileSync(offsetPath, 'utf8'), 10);
        if (!isNaN(val) && val > 0) startOffset = val;
      } else {
        console.error(`buffer: discarding stale offset sidecar ${offsetPath} (mtime ${offsetMtime} <= draining ${drainingMtime}) — starting from 0`);
        try { fs.unlinkSync(offsetPath); } catch (e) { /* ignore */ }
      }
    } catch (e) {
      console.error(`buffer: bad offset sidecar ${offsetPath}: ${e.message} — starting from 0`);
    }
  }

  // Stream the file with a bounded raw-byte reader (streamLines). The
  // earlier readline-based version still OOMed on files containing a
  // single pathologically large line (one ~500 MB crawl record): readline
  // accumulates bytes between newlines into one V8 string before emitting,
  // and that string allocation blew the heap with a SIGABRT (exit 134).
  // streamLines caps each line at MAX_LINE_BYTES and skips anything bigger.
  let written = 0;

  if (coalesce) {
    // Coalesce within each flushed batch instead of across the whole file.
    // The original implementation built a single Map keyed by URL across the
    // entire file so each unique URL produced exactly one bulkWrite op; that
    // Map could itself reach hundreds of MB for a saturated domains buffer.
    // Per-batch coalescing keeps peak memory tiny (drainBatchSize entries)
    // at the cost of more bulkWrite round trips when the same URL recurs
    // across distant batches. Final document state is identical: the upsert
    // is $set keyed on _id=url, and batches are awaited sequentially so the
    // last-written timestamp wins. Replay idempotency is preserved.
    const batchMap = new Map();
    let bad = 0;
    let oversized = 0;

    const flushBatch = async () => {
      if (batchMap.size === 0) return;
      const records = Array.from(batchMap, ([url, ts]) => ({ url, ts }));
      console.log(`buffer: inserting ${records.length} ${collectionName} records (bulkWrite start)`);
      const t0 = Date.now();
      await bulkWriteWithRetry(db, collectionName, buildDomainTimestampOps(records), collectionName);
      console.log(`buffer: ${collectionName} bulkWrite done (${records.length} records, ${Date.now() - t0}ms)`);
      written += records.length;
      batchMap.clear();
    };

    for await (const item of streamLines(drainingPath, MAX_LINE_BYTES)) {
      if (item.oversized) { oversized++; continue; }
      if (!item.line) continue;
      try {
        const rec = JSON.parse(item.line);
        if (rec && rec.url) {
          batchMap.set(rec.url, rec.ts);
          if (batchMap.size >= drainBatchSize) await flushBatch();
        }
      } catch (e) {
        bad++;
      }
    }
    await flushBatch();

    if (bad > 0) console.error(`buffer: skipped ${bad} malformed lines in ${drainingPath}`);
    if (oversized > 0) console.error(`buffer: skipped ${oversized} oversized (>${MAX_LINE_BYTES}B) lines in ${drainingPath}`);
    console.log(`buffer: drained ${written} unique URLs to ${collectionName}`);
  } else {
    let batch = [];
    let lastBatchEnd = startOffset;
    let bad = 0;
    let oversized = 0;
    // Tracks the byte offset just past the last fully-emitted line. Used as
    // the offset-sidecar value after each bulkWrite. streamLines yields
    // exact byte ranges, so we never have to guess `+1` for the terminator.
    let cursorEnd = 0;

    for await (const item of streamLines(drainingPath, MAX_LINE_BYTES)) {
      cursorEnd = item.lineEnd;

      if (item.lineStart < startOffset) continue;
      if (item.oversized) { oversized++; continue; }
      if (!item.line) continue;

      try {
        batch.push(JSON.parse(item.line));
      } catch (e) {
        bad++;
        continue;
      }

      if (batch.length >= drainBatchSize) {
        const ops = buildCrawlResultOps(batch);
        console.log(`buffer: inserting ${batch.length} ${collectionName} records (bulkWrite start, ${written} done so far)`);
        const t0 = Date.now();
        await bulkWriteWithRetry(db, collectionName, ops, collectionName);
        console.log(`buffer: ${collectionName} bulkWrite done (${batch.length} records, ${Date.now() - t0}ms)`);
        written += batch.length;
        lastBatchEnd = cursorEnd;
        fs.writeFileSync(offsetPath, String(lastBatchEnd));
        batch = [];
      }
    }

    if (batch.length > 0) {
      const ops = buildCrawlResultOps(batch);
      console.log(`buffer: inserting final ${batch.length} ${collectionName} records (bulkWrite start)`);
      const t0 = Date.now();
      await bulkWriteWithRetry(db, collectionName, ops, collectionName);
      console.log(`buffer: ${collectionName} bulkWrite done (${batch.length} records, ${Date.now() - t0}ms)`);
      written += batch.length;
      // Persist the offset past the residual batch too. Without this, a
      // crash between this bulkWrite and the drainingPath unlink below
      // would cause resumeIncompleteDrain to replay the residual records —
      // which for crawls means duplicate followups[] entries via
      // $concatArrays. Use exact file size to be definitive.
      fs.writeFileSync(offsetPath, String(fs.statSync(drainingPath).size));
    }

    if (bad > 0) console.error(`buffer: skipped ${bad} malformed lines in ${drainingPath}`);
    if (oversized > 0) console.error(`buffer: skipped ${oversized} oversized (>${MAX_LINE_BYTES}B) lines in ${drainingPath}`);
    console.log(`buffer: drained ${written} records to ${collectionName}`);
  }

  fs.unlinkSync(drainingPath);
  if (fs.existsSync(offsetPath)) {
    try { fs.unlinkSync(offsetPath); } catch (e) { /* ignore */ }
  }
  return written;
}

async function drain(db) {
  const start = Date.now();
  console.log('buffer: drain starting');
  const d = await drainOne(db, domainsActivePath, 'crawl_domains', true);
  const c = await drainOne(db, crawlsActivePath, 'crawls', false);
  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`buffer: drain done in ${elapsed}s (domains=${d}, crawls=${c})`);
  return { domains: d, crawls: c, elapsedMs: Date.now() - start };
}

/**
 * Finish any `.draining` files belonging to this instance left over from a
 * previous crash. Called once at startup before normal appends begin.
 */
async function resumeIncompleteDrain(db) {
  let entries;
  try {
    entries = fs.readdirSync(bufferDir);
  } catch (e) {
    if (e.code === 'ENOENT') return;
    throw e;
  }

  const domainsName = path.basename(domainsActivePath) + '.draining';
  const crawlsName = path.basename(crawlsActivePath) + '.draining';

  if (entries.includes(domainsName)) {
    console.log(`buffer: resuming incomplete domain drain: ${domainsName}`);
    await drainOne(db, domainsActivePath, 'crawl_domains', true);
  }
  if (entries.includes(crawlsName)) {
    console.log(`buffer: resuming incomplete crawl drain: ${crawlsName}`);
    await drainOne(db, crawlsActivePath, 'crawls', false);
  }
}

module.exports = {
  init,
  appendDomainTimestamp,
  appendCrawlResult,
  shouldDrain,
  bufferedBytes,
  drain,
  resumeIncompleteDrain
};
