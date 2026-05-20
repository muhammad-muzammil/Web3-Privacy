const { MongoClient } = require('mongodb');

let db = null;

/**
 * Initialize the database connection.
 * Environment variables: DB_HOST, DB_USER, DB_PWD, DB_NAME
 * @returns {Object|null} The database object, or null on failure.
 */
async function initDb() {
  const dbHost = process.env.DB_HOST;
  const dbUser = process.env.DB_USER;
  const dbPwd = process.env.DB_PWD;
  const dbName = process.env.DB_NAME;

  let connectionString = 'mongodb://';
  if (dbUser && dbPwd) {
    connectionString += `${dbUser}:${dbPwd}@`;
  }
  connectionString += dbHost;

  try {
    const client = new MongoClient(connectionString, {
      authSource: 'admin',
      // Without these, socketTimeoutMS defaults to infinite: a bulkWrite against
      // an unresponsive server never returns, which permanently pauses the
      // consumer mid-drain. Bound every connection-level wait so operations
      // error out (and get retried by buffer.bulkWriteWithRetry) instead.
      serverSelectionTimeoutMS: parseInt(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || '10000', 10),
      connectTimeoutMS: parseInt(process.env.MONGO_CONNECT_TIMEOUT_MS || '10000', 10),
      socketTimeoutMS: parseInt(process.env.MONGO_SOCKET_TIMEOUT_MS || '60000', 10),
    });
    await client.connect();
    await client.db('admin').command({ ping: 1 });
    db = client.db(dbName);
    console.log(`MongoDB connection established for ${dbHost}`);
    return db;
  } catch (e) {
    console.error(`Failed to connect to MongoDB at ${dbHost}: ${e.message}`);
    return null;
  }
}

/**
 * Transform Puppeteer crawl result into MongoDB document format and insert.
 * Matches the upsert/followup logic from crawler/mongodb.py.
 *
 * @param {string} url - Initial URL crawled
 * @param {string} redirectedUrl - Final URL after redirects
 * @param {Date}   accessedDate - Date of crawl
 * @param {number} status - HTTP status of main request
 * @param {string} pageSrc - Full HTML source
 * @param {Array}  additionalRequests - Mapped request array [{endpoint, method, status, requestBody, responseBody, type}]
 * @param {Array}  interactions - [{type, info}] pairs including wallet interaction data
 * @param {Array}  matchedAddresses - Extracted blockchain address tuples [[chain, addr], ...]
 * @param {number} crawlerVersion - Monotonic version of the crawler code that produced this record.
 *                                  The top-level `crawlerVersion` field is overwritten on every
 *                                  upsert so "latest run wins"; the per-run copy is also preserved
 *                                  inside each `followups[]` entry.
 */
async function insertCrawlResult(url, redirectedUrl, accessedDate, status, pageSrc = '', additionalRequests = [], interactions = [], matchedAddresses = [], evalScripts = [], crawlerVersion = 1) {
  if (!db) {
    throw new Error('Database not initialized. Call initDb first.');
  }

  const crawlData = {
    redirectedUrl,
    accessedDate,
    status,
    pageSrc,
    additionalRequests,
    interactions,
    matchedAddresses,
    evalScripts,
    crawlerVersion
  };

  try {
    const result = await db.collection('crawls').updateOne(
      { url },
      [
        {
          $set: {
            url: { $ifNull: ['$url', url] },
            redirectedUrl: { $ifNull: ['$redirectedUrl', redirectedUrl] },
            accessedDate: { $ifNull: ['$accessedDate', accessedDate] },
            status: { $ifNull: ['$status', status] },
            pageSrc: { $ifNull: ['$pageSrc', pageSrc] },
            additionalRequests: { $ifNull: ['$additionalRequests', additionalRequests] },
            interactions: { $ifNull: ['$interactions', interactions] },
            matchedAddresses: {$ifNull: ['$matchedAddresses', matchedAddresses]},
            evalScripts: { $ifNull: ['$evalScripts', evalScripts] },
            crawlerVersion: crawlerVersion,
            followups: {
              $cond: {
                if: { $isArray: '$followups' },
                then: { $concatArrays: ['$followups', [crawlData]] },
                else: []
              }
            }
          }
        }
      ],
      { upsert: true }
    );

    if (result.upsertedId) {
      console.log(`Inserted new crawl result for ${url} with ID ${result.upsertedId}`);
      return { action: 'new crawl', id: result.upsertedId.toString() };
    } else {
      console.log(`Added followup crawl result for ${url}`);
      return { action: 'followup crawl' };
    }
  } catch (e) {
    console.error(`Failed to insert crawl result for ${url}: ${e.message}`);
    return null;
  }
}

/**
 * Transform raw Puppeteer request log entries into additionalRequests schema format.
 * @param {Array} requests - Raw requests from crawl.js [{url, method, status, postData, responseBody, mimeType, type}]
 * @returns {Array} Mapped to [{endpoint, method, status, requestBody, responseBody, type}]
 */
function mapRequests(requests) {
  return (requests || []).map(r => ({
    endpoint: r.url || '',
    method: r.method || 'GET',
    status: typeof r.status === 'number' ? r.status : 0,
    requestBody: r.postData || '',
    responseBody: r.responseBody || '',
    type: r.mimeType || r.type || ''
  }));
}

/**
 * Upsert a {url, timestamp} record into the `domains` tracking collection.
 * The URL is used as the document _id so this is idempotent regardless of
 * whether the ct-stream ingester already created the record. Called once per
 * URL before any browser work, to mark "the crawler has picked this up".
 *
 * @param {string} url The URL being crawled (also the document primary key)
 */
async function upsertDomainTimestamp(url) {
  if (!db) {
    throw new Error('Database not initialized. Call initDb first.');
  }
  try {
    // $setOnInsert keeps _id immutable (and matches the bridge's pattern at
    // ctbridge/certstream-bridge.py); $set always refreshes the timestamp.
    await db.collection('crawl_domains').updateOne(
      { _id: url },
      {
        $setOnInsert: { _id: url },
        $set: { timestamp: new Date() }
      },
      { upsert: true }
    );
  } catch (e) {
    console.error(`Failed to upsert crawl_domains record for ${url}: ${e.message}`);
  }
}

/**
 * Build interactions array from wallet crawl result.
 * @param {Object} crawlLog - Result from crawlUrl()
 * @returns {Array} [{type, info}] pairs
 */
function buildInteractions(crawlLog) {
  const interactions = [];

  interactions.push({
    type: 'wallet_connect',
    info: JSON.stringify({
      connected: crawlLog.connected || false,
      connectLabel: crawlLog.connect_label || '',
      metamaskLabel: crawlLog.metamask_label || '',
      checkboxClicked: crawlLog.checkbox_clicked || false
    })
  });

  interactions.push({
    type: 'signature_request',
    info: String(crawlLog.signature_request || false)
  });

  interactions.push({
    type: 'switch_network',
    info: String(crawlLog.switch_network || false)
  });

  if (crawlLog.cookies && crawlLog.cookies.length > 0) {
    interactions.push({
      type: 'cookies',
      info: JSON.stringify(crawlLog.cookies)
    });
  }

  return interactions;
}

/**
 * Build a bulkWrite ops array for the crawl_domains collection.
 * Mirrors the upsert shape used by upsertDomainTimestamp so the drain path
 * and the (now-unused) inline path produce identical documents.
 *
 * @param {Array<{url: string, ts: string|Date}>} records
 * @returns {Array} ops array ready for collection.bulkWrite()
 */
function buildDomainTimestampOps(records) {
  return records.map(rec => ({
    updateOne: {
      filter: { _id: rec.url },
      update: {
        $setOnInsert: { _id: rec.url },
        $set: { timestamp: rec.ts instanceof Date ? rec.ts : new Date(rec.ts) }
      },
      upsert: true
    }
  }));
}

/**
 * Build a bulkWrite ops array for the crawls collection.
 * Mirrors the followup-aware upsert shape used by insertCrawlResult so the
 * drain path produces identical documents (including the followups[] append
 * on re-crawls). Date strings from JSONL serialisation are rehydrated.
 *
 * @param {Array<Object>} records crawl-result docs as written by buffer.appendCrawlResult
 * @returns {Array} ops array ready for collection.bulkWrite()
 */
function buildCrawlResultOps(records) {
  return records.map(rec => {
    const accessedDate = rec.accessedDate instanceof Date
      ? rec.accessedDate
      : new Date(rec.accessedDate);
    const crawlData = {
      redirectedUrl: rec.redirectedUrl,
      accessedDate,
      status: rec.status,
      pageSrc: rec.pageSrc,
      additionalRequests: rec.additionalRequests,
      interactions: rec.interactions,
      matchedAddresses: rec.matchedAddresses,
      evalScripts: rec.evalScripts,
      crawlerVersion: rec.crawlerVersion
    };
    return {
      updateOne: {
        filter: { url: rec.url },
        update: [
          {
            $set: {
              url: { $ifNull: ['$url', rec.url] },
              redirectedUrl: { $ifNull: ['$redirectedUrl', rec.redirectedUrl] },
              accessedDate: { $ifNull: ['$accessedDate', accessedDate] },
              status: { $ifNull: ['$status', rec.status] },
              pageSrc: { $ifNull: ['$pageSrc', rec.pageSrc] },
              additionalRequests: { $ifNull: ['$additionalRequests', rec.additionalRequests] },
              interactions: { $ifNull: ['$interactions', rec.interactions] },
              matchedAddresses: { $ifNull: ['$matchedAddresses', rec.matchedAddresses] },
              evalScripts: { $ifNull: ['$evalScripts', rec.evalScripts] },
              crawlerVersion: rec.crawlerVersion,
              followups: {
                $cond: {
                  if: { $isArray: '$followups' },
                  then: { $concatArrays: ['$followups', [crawlData]] },
                  else: []
                }
              }
            }
          }
        ],
        upsert: true
      }
    };
  });
}

module.exports = {
  initDb,
  insertCrawlResult,
  upsertDomainTimestamp,
  mapRequests,
  buildInteractions,
  buildDomainTimestampOps,
  buildCrawlResultOps
};
