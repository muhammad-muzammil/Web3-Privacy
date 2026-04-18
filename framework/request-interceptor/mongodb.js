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
    const client = new MongoClient(connectionString, { authSource: 'admin' });
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
 * @param {number} crawlerType - Crawler type flag (1 = Puppeteer)
 */
async function insertCrawlResult(url, redirectedUrl, accessedDate, status, pageSrc = '', additionalRequests = [], interactions = [], crawlerType = 1) {
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
    crawlerType
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
            crawlerType: { $ifNull: ['$crawlerType', crawlerType] },
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
    await db.collection('domains').updateOne(
      { _id: url },
      { $set: { _id: url, timestamp: new Date() } },
      { upsert: true }
    );
  } catch (e) {
    console.error(`Failed to upsert domains record for ${url}: ${e.message}`);
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

module.exports = {
  initDb,
  insertCrawlResult,
  upsertDomainTimestamp,
  mapRequests,
  buildInteractions
};
