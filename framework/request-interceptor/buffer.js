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
const readline = require('readline');
const { buildDomainTimestampOps, buildCrawlResultOps } = require('./mongodb.js');

let bufferDir = null;
let instanceId = null;
let flushThresholdBytes = 0;
let drainBatchSize = 500;
let domainsActivePath = null;
let crawlsActivePath = null;

function init(opts) {
  bufferDir = opts.bufferDir;
  instanceId = opts.instanceId;
  flushThresholdBytes = opts.flushThresholdBytes;
  if (typeof opts.drainBatchSize === 'number' && opts.drainBatchSize > 0) {
    drainBatchSize = opts.drainBatchSize;
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
  fs.appendFileSync(crawlsActivePath, line);
}

function fileSize(p) {
  try {
    return fs.statSync(p).size;
  } catch (e) {
    if (e.code === 'ENOENT') return 0;
    throw e;
  }
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

  // Stream the file line-by-line. fs.readFileSync of the whole .draining
  // file used to OOM-kill the container during recovery: a 500 MB JSONL on
  // disk peaks at ~1.5–2 GB resident once V8's UTF-16 string plus the
  // .split('\n') array plus JSON.parse transient allocations are accounted
  // for. With readline, peak memory is bounded by one line plus the current
  // in-flight batch.
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
    const rl = readline.createInterface({
      input: fs.createReadStream(drainingPath, { encoding: 'utf8' }),
      crlfDelay: Infinity
    });
    const batchMap = new Map();
    let bad = 0;

    const flushBatch = async () => {
      if (batchMap.size === 0) return;
      const records = Array.from(batchMap, ([url, ts]) => ({ url, ts }));
      await db.collection(collectionName).bulkWrite(
        buildDomainTimestampOps(records),
        { ordered: false }
      );
      written += records.length;
      batchMap.clear();
    };

    for await (const line of rl) {
      if (!line) continue;
      try {
        const rec = JSON.parse(line);
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
    console.log(`buffer: drained ${written} unique URLs to ${collectionName}`);
  } else {
    const rl = readline.createInterface({
      input: fs.createReadStream(drainingPath, { encoding: 'utf8' }),
      crlfDelay: Infinity
    });
    let cursor = 0;
    let batch = [];
    let lastBatchEnd = startOffset;
    let bad = 0;

    for await (const line of rl) {
      const lineStart = cursor;
      // readline strips the terminator. Assume '\n' (1 byte). The only case
      // this is wrong is the file's final line if the file doesn't end with
      // a newline — corrected by writing the exact file size on the
      // residual-batch offset below.
      cursor += Buffer.byteLength(line, 'utf8') + 1;

      if (lineStart < startOffset) continue;
      if (!line) continue;

      try {
        batch.push(JSON.parse(line));
      } catch (e) {
        bad++;
        continue;
      }

      if (batch.length >= drainBatchSize) {
        const ops = buildCrawlResultOps(batch);
        await db.collection(collectionName).bulkWrite(ops, { ordered: false });
        written += batch.length;
        lastBatchEnd = cursor;
        fs.writeFileSync(offsetPath, String(lastBatchEnd));
        batch = [];
      }
    }

    if (batch.length > 0) {
      const ops = buildCrawlResultOps(batch);
      await db.collection(collectionName).bulkWrite(ops, { ordered: false });
      written += batch.length;
      // Persist the offset past the residual batch too. Without this, a
      // crash between this bulkWrite and the drainingPath unlink below
      // would cause resumeIncompleteDrain to replay the residual records —
      // which for crawls means duplicate followups[] entries via
      // $concatArrays. Use exact file size to avoid the +1 off-by-one when
      // the file doesn't end with a newline.
      fs.writeFileSync(offsetPath, String(fs.statSync(drainingPath).size));
    }

    if (bad > 0) console.error(`buffer: skipped ${bad} malformed lines in ${drainingPath}`);
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
