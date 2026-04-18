#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Kafka } = require('kafkajs');
const chromePuppeteerLib = require('./chrome/puppeteer.js');
const chromeLoggerLib = require('./chrome/logging.js');
const { crawlUrl, timeoutPromise } = require('./chrome/crawl.js');
const { importMetaMaskWallet } = require('./chrome/helper.js');
const {
  initDb,
  insertCrawlResult,
  upsertDomainTimestamp,
  mapRequests,
  buildInteractions
} = require('./mongodb.js');
const { loadConfig, scanText } = require('./match.js');

// Configuration from environment
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const KAFKA_GROUP = process.env.KAFKA_GROUP || 'ct-crawlers';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'ct-stream';
const INDEX_TOPIC = process.env.INDEX_TOPIC || 'crawled-urls';
const METAMASK_PATH = process.env.METAMASK_PATH || './metamask-chrome-10.22.2';
const CRAWL_TIMEOUT = parseInt(process.env.CRAWL_TIMEOUT || '30', 10) * 1000;
const SITES_PER_SESSION = parseInt(process.env.SITES_PER_SESSION || '100', 10);
const DEBUG_LEVEL = process.env.DEBUG_LEVEL || 'none';
const MAX_CRAWL_RETRIES = 3;

const logger = chromeLoggerLib.getLoggerForLevel(DEBUG_LEVEL);

// Load search_terms / false_flags once at startup. Both files live next to
// this script (resolved by match.js via __dirname).
const { searchTerms, falseFlags } = loadConfig();

/**
 * Parse URL from Certificate Transparency stream message.
 * Handles formats: "DNS:example.com", "IP Address:1.2.3.4", or raw domain.
 */
function parseUrl(messageStr) {
  let part = messageStr.split(',')[0];
  let indexDns = part.indexOf('DNS:');
  let indexIp = part.indexOf('IP Address:');

  let url;
  if (indexDns !== -1) {
    url = part.substring(indexDns + 4);
  } else if (indexIp !== -1) {
    url = part.substring(indexIp + 11);
  } else {
    url = part;
  }

  // Strip wildcard prefix
  if (url.startsWith('*.')) {
    url = url.substring(2);
  }

  return url.trim();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function startBrowser() {
  // Fresh user-data-dir per browser session. Reused across all SITES_PER_SESSION
  // crawls in this session so MetaMask state (imported once at session start)
  // survives between URLs. Deleted on session refresh — see main() — mimicking
  // sel-wire.py's per-launch profile cleanup.
  const profilePath = path.join(os.tmpdir(), `wallet-crawler-profile-${Date.now()}-${process.pid}`);
  try {
    fs.mkdirSync(profilePath, { recursive: true });
  } catch (e) {
    logger.debug(`Failed to create profile dir ${profilePath}: ${e.message}`);
  }

  const args = {
    debugLevel: DEBUG_LEVEL,
    headless: false, // Extensions require a display (use xvfb)
    walletPath: METAMASK_PATH,
    profilePath,
    printFrameHierarchy: false,
    executablePath: undefined // Use Puppeteer's bundled Chromium
  };

  const browser = await chromePuppeteerLib.launch(args);

  // Set up the targetcreated handler for network capture
  const requestLog = { requests: [] };
  const cdpClients = [];
  const webSockets = [];

  // Import onTargetCreated handler by requiring crawl.js internals
  // Since onTargetCreated is not exported, we set up the handler on the browser
  // using the same pattern as crawl()
  browser.on('targetcreated', async (target) => {
    if (target.type() !== 'page') return;

    const page = await target.page();
    const chromeLoggerLib = require('./chrome/logging.js');
    const loggerInner = chromeLoggerLib.getLoggerForLevel(args.debugLevel);

    page.on('request', async (request) => {
      let requestContext = [];
      const frame = request.frame();
      if (frame) {
        requestContext.push(frame.url());
      }

      const requestUrl = request.url();
      const requestType = request.resourceType()[0].toUpperCase() + request.resourceType().substring(1);
      const requestMethod = request.method();
      const requestHeaders = {};
      Object.keys(request.headers()).forEach(name => {
        requestHeaders[name.toLowerCase().trim()] = request.headers()[name];
      });

      let requestPostData = request.postData();
      if (requestPostData === undefined) requestPostData = '';

      requestLog.requests.push({
        requestContext,
        id: request._requestId,
        url: requestUrl,
        type: requestType,
        status: undefined,
        method: requestMethod,
        headers: requestHeaders,
        postData: requestPostData,
        responseHeaders: {},
        responseBody: '',
        mimeType: ''
      });
    });

    const cdpClient = await page.target().createCDPSession();
    await cdpClient.send('Network.enable');
    await cdpClient.send('Page.enable');

    cdpClient.on('Network.responseReceived', async (event) => {
      for (let i = 0; i < requestLog.requests.length; i++) {
        if (requestLog.requests[i].id === event.requestId) {
          requestLog.requests[i].status = event.response.status;
          requestLog.requests[i].mimeType = event.response.mimeType || '';
          const headers = {};
          Object.keys(event.response.headers).forEach(name => {
            headers[name.toLowerCase().trim()] = event.response.headers[name];
          });
          requestLog.requests[i].responseHeaders = headers;
          try {
            const { body, base64Encoded } = await cdpClient.send('Network.getResponseBody', {
              requestId: event.requestId
            });
            requestLog.requests[i].responseBody = base64Encoded ? '[base64]' : body;
          } catch (e) {
            requestLog.requests[i].responseBody = '';
          }
          break;
        }
      }
    });

    cdpClients.push(cdpClient);
    loggerInner.debug('Configured new page: ' + page.url());
  });

  // Wait for MetaMask extension to load, then import wallet
  await sleep(3000);
  const pages = await browser.pages();
  if (pages.length > 1) {
    const wallet = pages[pages.length - 1];
    await wallet.bringToFront();
    try {
      wallet.setDefaultNavigationTimeout(0);
      await importMetaMaskWallet(logger, wallet);
      logger.debug('MetaMask wallet imported successfully');
    } catch (e) {
      logger.debug('Failed to import MetaMask wallet: ' + e.toString());
    }
  }

  return { browser, requestLog, cdpClients, args, profilePath };
}

/**
 * Tear down a browser session: close the browser, then recursively delete the
 * user-data-dir so the next session starts from a clean slate (mimicking
 * sel-wire.py:181 `shutil.rmtree`).
 */
async function destroySession(session) {
  if (!session) return;
  try { await session.browser.close(); } catch (e) { /* ignore */ }
  if (session.profilePath) {
    try {
      fs.rmSync(session.profilePath, { recursive: true, force: true });
      logger.debug(`Removed profile dir ${session.profilePath}`);
    } catch (e) {
      logger.debug(`Failed to remove profile dir ${session.profilePath}: ${e.message}`);
    }
  }
}

async function main() {
  // Initialize MongoDB
  const db = await initDb();
  if (!db) {
    console.error('Failed to connect to MongoDB. Exiting.');
    process.exit(1);
  }

  // Initialize Kafka
  const kafka = new Kafka({
    clientId: 'wallet-crawler',
    brokers: [KAFKA_BROKER]
  });

  const consumer = kafka.consumer({
    groupId: KAFKA_GROUP,
    maxWaitTimeInMs: 10000,
    sessionTimeout: 60000,
    maxPollIntervalMs: 900000
  });

  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

  let siteCounter = 0;
  let session = await startBrowser();

  console.log(`Kafka consumer started. Group: ${KAFKA_GROUP}, Topic: ${KAFKA_TOPIC}`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const commitOffset = () =>
        consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);

      // Session refresh: tear down the browser AND wipe its user-data-dir, then
      // launch fresh. MetaMask will be re-imported on the new session.
      if (siteCounter >= SITES_PER_SESSION) {
        logger.debug('Session refresh: restarting browser');
        await destroySession(session);
        session = await startBrowser();
        siteCounter = 0;
      }

      const messageStr = message.value.toString();
      const url = parseUrl(messageStr);

      if (!url) {
        await commitOffset();
        return;
      }

      const accessedDate = new Date();

      // Step 1: stamp the domains tracking collection BEFORE any browser work.
      // Failure here is non-fatal — we still try to crawl.
      try {
        await upsertDomainTimestamp(url);
      } catch (e) {
        console.error(`Failed to upsert domains record for ${url}: ${e.message}`);
      }

      // Step 2: crawl with up to MAX_CRAWL_RETRIES attempts. Mimics
      // sel-wire.py:138-181 — three tries, then move on if still failing.
      let crawlLog = null;
      for (let attempt = 1; attempt <= MAX_CRAWL_RETRIES; attempt++) {
        try {
          logger.debug(`Crawling ${url} (attempt ${attempt}/${MAX_CRAWL_RETRIES})`);
          const crawlPromise = crawlUrl(
            session.browser,
            session.requestLog,
            session.cdpClients,
            `https://${url}`,
            { ...session.args, secs: Math.floor(CRAWL_TIMEOUT / 1000) },
            logger,
            true // skipImport — wallet already imported at session start
          );
          const result = await timeoutPromise(crawlPromise, CRAWL_TIMEOUT);

          // timeoutPromise resolves to the literal `1` on timeout.
          if (result === 1) {
            logger.debug(`Timed out crawling ${url} (attempt ${attempt}/${MAX_CRAWL_RETRIES})`);
            continue;
          }

          crawlLog = result;
          break;
        } catch (e) {
          const firstLine = (e && e.message ? e.message : String(e)).split('\n')[0];
          logger.debug(`Attempt ${attempt}/${MAX_CRAWL_RETRIES} failed for ${url}: ${firstLine}`);
        }
      }

      // Step 3: handle exhausted retries — commit offset and skip the rest.
      // No publish to crawled-urls, no crawls write.
      if (!crawlLog) {
        console.error(`Giving up on ${url} after ${MAX_CRAWL_RETRIES} attempts`);
        await commitOffset();
        await heartbeat();
        siteCounter++;
        return;
      }

      // Step 4: scan captured requests for non-false-flagged token hits and
      // build the filtered additionalRequests array. Only requests where the
      // URL, request body, or response body contains an interesting token are
      // kept.
      const allMapped = mapRequests(crawlLog.requests || []);
      const interestingRequests = [];
      const matchedTokens = new Set();
      for (const req of allMapped) {
        const urlScan = scanText(req.endpoint || '', searchTerms, falseFlags);
        const reqScan = scanText(req.requestBody || '', searchTerms, falseFlags);
        const respScan = scanText(req.responseBody || '', searchTerms, falseFlags);
        if (urlScan.interesting || reqScan.interesting || respScan.interesting) {
          interestingRequests.push(req);
          urlScan.tokens.forEach(t => matchedTokens.add(t));
          reqScan.tokens.forEach(t => matchedTokens.add(t));
          respScan.tokens.forEach(t => matchedTokens.add(t));
        }
      }

      // Step 5: detect any privacy interaction the wallet flow recorded.
      const anyPrivacyInteraction = !!(
        crawlLog.connected || crawlLog.signature_request || crawlLog.switch_network
      );

      const interesting = interestingRequests.length > 0 || anyPrivacyInteraction;

      // Step 6: conditionally write the full record to the crawls collection.
      if (interesting) {
        const redirectedUrl = crawlLog.redirectedUrl || url;
        const status = typeof crawlLog.status === 'number' ? crawlLog.status : -1;
        const pageSrc = crawlLog.pageSrc || '';
        const interactions = buildInteractions(crawlLog);

        logger.debug(
          `Interesting crawl for ${url}: ${interestingRequests.length} matching requests, ` +
          `tokens={${Array.from(matchedTokens).join(',')}}, walletInteraction=${anyPrivacyInteraction}`
        );

        try {
          await insertCrawlResult(
            url,
            redirectedUrl,
            accessedDate,
            status,
            pageSrc,
            interestingRequests,
            interactions,
            1 // crawlerType = 1 (Puppeteer)
          );
        } catch (e) {
          console.error(`Failed to insert crawls record for ${url}: ${e.message}`);
        }
      } else {
        logger.debug(`Skipping crawls insert for ${url} (no interesting tokens or interactions)`);
      }

      // Step 7: always publish to crawled-urls on a successful crawl, then
      // commit the Kafka offset.
      try {
        await producer.send({
          topic: INDEX_TOPIC,
          messages: [{ value: url }]
        });
      } catch (e) {
        console.error(`Failed to produce to ${INDEX_TOPIC} for ${url}: ${e.message}`);
      }

      await commitOffset();
      await heartbeat();
      siteCounter++;
    }
  });
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
