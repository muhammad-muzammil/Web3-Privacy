#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const os = require('os');
const path = require('path');
const { Kafka } = require('kafkajs');
const chromePuppeteerLib = require('./chrome/puppeteer.js');
const chromeLoggerLib = require('./chrome/logging.js');
const { crawlUrl } = require('./chrome/crawl.js');
const { importMetaMaskWallet } = require('./chrome/helper.js');
const {
  initDb,
  insertCrawlResult,
  upsertDomainTimestamp,
  mapRequests,
  buildInteractions
} = require('./mongodb.js');
const { loadConfig, scanText } = require('./match.js');
const metrics = require('./metrics.js');

// Configuration from environment
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const KAFKA_GROUP = process.env.KAFKA_GROUP || 'ct-crawlers';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'ct-stream';
const INDEX_TOPIC = process.env.INDEX_TOPIC || 'crawled-urls';
const METAMASK_PATH = process.env.METAMASK_PATH || './metamask-chrome-10.22.2';
// Page interaction (goto + wallet connect + dwell + collection): hard wall-clock
// after which the active page is forcibly closed and analysis runs on whatever
// requests/evalScripts the event handlers captured up to that point.
const PAGE_TIMEOUT = parseInt(process.env.PAGE_TIMEOUT || '18', 10) * 1000;
// Analysis (request scanning, MongoDB insert, Kafka publish) gets its own,
// shorter budget so a stalled DB or broker cannot wedge the consumer loop.
const ANALYSIS_TIMEOUT = parseInt(process.env.ANALYSIS_TIMEOUT || '5', 10) * 1000;
const SITES_PER_SESSION = parseInt(process.env.SITES_PER_SESSION || '100', 10);
const DEBUG_LEVEL = process.env.DEBUG_LEVEL || 'none';
const MAX_CRAWL_RETRIES = 1;
var session_dead = false;

const logger = chromeLoggerLib.getLoggerForLevel(DEBUG_LEVEL);

// Load search_terms / false_flags once at startup. Both files live next to
// this script (resolved by match.js via __dirname).
const { searchTerms, falseFlags, urlTerms, safeEndpointDomains, safeRedirectDomains } = loadConfig();


/*****
 * Scans a URL domain for whether it is from a known safe domain or not. Used to check for 
 * and quickly drop irrelevant redirects to google or other sites.
 */
isSafeRedirectDomain = (url) => {
  try {
    const host = new URL(url.startsWith('http') ? url : `https://${url}`).hostname;
    return safeRedirectDomains.some(safe => host === safe || host.endsWith('.' + safe));
  } catch { return false; }
};

/* Scans the URL of outgoing network requests for safe domains */
const isSafeEndpoint = (url) => safeEndpointDomains.some(
    safe => { try { return new URL(url).hostname.endsWith(safe); } catch { return false; } }
  );

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
  };

  const browser = await chromePuppeteerLib.launch(args);
  browser.on('disconnected', () => { session_dead = true; });

  // Set up the targetcreated handler for network capture
  const requestLog = { requests: [], evalScripts: [] };
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

    // Filter eval-script capture by the page's URL at parse time. Gating at
    // targetcreated was unreliable — MetaMask popup targets often report
    // about:blank or '' before navigating to chrome-extension://..., so the
    // gate let their internal generated scripts through. Checking page.url()
    // when each script is parsed catches the popup case correctly.
    await cdpClient.send('Debugger.enable');

    cdpClient.on('Debugger.scriptParsed', async (params) => {
      if (params.url) return; // scripts with a URL are captured by the network handler
      const capturedFromUrl = page.url() || '';
      if (capturedFromUrl.startsWith('chrome-extension://') ||
          capturedFromUrl.startsWith('devtools://')) {
        return;
      }
      try {
        const { scriptSource } = await cdpClient.send('Debugger.getScriptSource', { scriptId: params.scriptId });
        if (scriptSource) {
          requestLog.evalScripts.push({ source: scriptSource, capturedFromUrl });
        }
      } catch (e) {}
    });

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
  await sleep(2500);
  const pages = await browser.pages();
  if (pages.length > 1) {
    const wallet = pages[pages.length - 1];
    await wallet.bringToFront();
    try {
      wallet.setDefaultNavigationTimeout(0);
      await importMetaMaskWallet(logger, wallet);
      console.log('MetaMask wallet imported successfully');
    } catch (e) {
      console.error('Failed to import MetaMask wallet: ' + e.toString());
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

/**
 * Run crawlUrl under a hard wall-clock budget. On expiry, forcibly close any
 * non-extension page so the in-flight Puppeteer awaits inside crawlUrl throw,
 * and synthesize a partial-result log from whatever the event handlers wrote
 * into `requestLog.requests` / `requestLog.evalScripts`. Replaces the old
 * `timeoutPromise` helper, which leaked Puppeteer ops past the timeout.
 */
async function crawlUrlBounded(session, url, args, logger, ms) {
  const requestsBefore = session.requestLog.requests.length;
  const evalScriptsBefore = session.requestLog.evalScripts ? session.requestLog.evalScripts.length : 0;

  let timer;
  let timedOut = false;
  const timeout = new Promise(resolve => {
    timer = setTimeout(() => { timedOut = true; resolve(null); }, ms);
  });

  const crawlPromise = crawlUrl(
    session.browser,
    session.requestLog,
    session.cdpClients,
    url,
    args,
    logger,
    true // skipImport — wallet already imported at session start
  );
  // Make sure a late rejection after timeout doesn't surface as an unhandled rejection.
  crawlPromise.catch(() => {});

  let winner;
  let crawlError;
  try {
    winner = await Promise.race([crawlPromise, timeout]);
  } catch (e) {
    crawlError = e;
  } finally {
    clearTimeout(timer);
  }

  if (!timedOut && !crawlError) {
    return winner;
  }

  // Either the page budget was hit, or crawlUrl rejected (commonly: the
  // wallet-connect hard timeout closed the page and a subsequent await threw
  // 'Target closed'). Either way, close any remaining non-extension pages so
  // handlers stop firing, then synthesize a partial-result log from whatever
  // the event handlers wrote into requestLog.
  if (timedOut) {
    logger.debug(`Page budget hit (${ms}ms) for ${url} — closing pages and returning partial capture`);
  } else {
    const firstLine = (crawlError && crawlError.message ? crawlError.message : String(crawlError)).split('\n')[0];
    logger.debug(`crawlUrl rejected for ${url} (${firstLine}) — closing pages and returning partial capture`);
  }
  try {
    const pages = await session.browser.pages();
    for (const p of pages) {
      const u = p.url() || '';
      if (!u.startsWith('chrome-extension://') && !u.startsWith('about:')) {
        try { await p.close({ runBeforeUnload: false }); } catch (e) {}
      }
    }
  } catch (e) {}

  return {
    url,
    redirectedUrl: '',
    success: false,
    timedOut: timedOut,
    pageSrc: '',
    status: 0,
    connected: false,
    signature_request: false,
    switch_network: false,
    cookies: [],
    requests: session.requestLog.requests.slice(requestsBefore),
    evalScripts: session.requestLog.evalScripts ? session.requestLog.evalScripts.slice(evalScriptsBefore) : []
  };
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

  // Timeouts are sized to the actual per-message budget: PAGE_TIMEOUT (~10s) +
  // ANALYSIS_TIMEOUT (~3s) plus generous slack. A dead pod or hung crawl now
  // recovers in tens of seconds instead of multiple minutes.
  const consumer = kafka.consumer({
    groupId: KAFKA_GROUP,
    maxWaitTimeInMs: 5000,
    sessionTimeout: 45000,
    heartbeatInterval: 10000,
    rebalanceTimeout: 60000,
    maxPollIntervalMs: 60000
  });

  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

  let consecutiveFailures = 0;
  let siteCounter = 0;
  let session = await startBrowser();

  console.log(`Crawler config: PAGE_TIMEOUT=${PAGE_TIMEOUT}ms, ANALYSIS_TIMEOUT=${ANALYSIS_TIMEOUT}ms, SITES_PER_SESSION=${SITES_PER_SESSION}`);
  console.log(`Kafka consumer started. Group: ${KAFKA_GROUP}, Topic: ${KAFKA_TOPIC}`);
  //Session compromised error message flag
  const SESSION_DEAD_RE = /Target closed|Session closed|Connection closed|Protocol error/;
  
  async function runConsumer() {
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        console.log(`recv p=${partition} off=${message.offset}`);
        const commitOffset = () =>
          consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);

        // Background heartbeat — kafkajs does not auto-heartbeat during eachMessage,
        // so a long crawl (esp. MetaMask "wallet connect hard timeout") would
        // otherwise let the broker session expire and trigger a rebalance.
        const heartbeatTicker = setInterval(() => {
          heartbeat().catch(e => console.error(`background heartbeat failed: ${e.message}`));
        }, 30000);

        try {
        // Session refresh: tear down the browser AND wipe its user-data-dir, then
        // launch fresh. MetaMask will be re-imported on the new session.
        if (siteCounter >= SITES_PER_SESSION || session_dead) {
          logger.debug('Session refresh: restarting browser');
          await destroySession(session);
          session = await startBrowser();
          siteCounter = 0;
          session_dead = false;
          consecutiveFailures = 0;
        }

        const messageStr = message.value.toString();
        const url = parseUrl(messageStr);

        if (!url) {
          await commitOffset();
          return;
        }

        metrics.urlsConsumed.inc();

        // Step 1: stamp the crawl_domains tracking collection BEFORE any browser
        // work. Failure here is non-fatal — we still try to crawl.
        try {
          await upsertDomainTimestamp(url);
        } catch (e) {
          console.error(`Failed to upsert crawl_domains record for ${url}: ${e.message}`);
        }

        // Step 2: crawl with up to MAX_CRAWL_RETRIES attempts. Mimics
        // sel-wire.py:138-181 — three tries, then move on if still failing.
        // Page interaction is bounded by PAGE_TIMEOUT (default 10s); on expiry
        // crawlUrlBounded returns a partial result with timedOut: true and we
        // still flow into the analysis step so any captured requests/scripts
        // are scanned and persisted.
        let crawlLog = null;
        let sessionCompromised = false;
        let accessedDate;
        for (let attempt = 1; attempt <= MAX_CRAWL_RETRIES; attempt++) {
          await heartbeat();
          try {
            logger.debug(`Crawling ${url} (attempt ${attempt}/${MAX_CRAWL_RETRIES})`);
            accessedDate = new Date();
            crawlLog = await crawlUrlBounded(
              session,
              `https://${url}`,
              { ...session.args, secs: 1 },
              logger,
              PAGE_TIMEOUT
            );
            break;
          } catch (e) {
            const firstLine = (e && e.message ? e.message : String(e)).split('\n')[0];
            console.error(`Attempt ${attempt}/${MAX_CRAWL_RETRIES} failed for ${url}: ${firstLine}`);
            if (SESSION_DEAD_RE.test(firstLine)) {
              sessionCompromised = true;
            }
            break;
          }
        }

        // Step 3: handle exhausted retries — commit offset and skip the rest.
        // No publish to crawled-urls, no crawls write.
        if (!crawlLog) {
          console.error(`Giving up on ${url} after ${MAX_CRAWL_RETRIES} attempts`);
          consecutiveFailures++;
          siteCounter++;
          if (sessionCompromised) {
              console.error('Session compromised — rebuilding');
              await destroySession(session);
              session = await startBrowser();
              siteCounter = 0;
              consecutiveFailures = 0;
          }
          else if (consecutiveFailures >= 3) {
            console.error('Rebuilding session after 3 consecutive failures');
            await destroySession(session);
            session = await startBrowser();
            consecutiveFailures = 0;
          }
          try {
            await commitOffset();
          } catch (e) {
            console.error(`commit failed for ${url} (likely rebalance in progress, will be redelivered): ${e.message}`);
          }
          await heartbeat();
          return;
        }
        metrics.crawlsCompleted.inc();
        consecutiveFailures = 0;

        /* Drop the log and consume if the initial response was an HTTP error.
         * The page returned 4xx/5xx, so there's nothing useful to analyze —
         * skip straight to commit and move on. */
        if (typeof crawlLog.status === 'number' && crawlLog.status >= 400) {
          logger.debug(`Skipping ${url} — HTTP ${crawlLog.status}`);
          consecutiveFailures = 0;
          siteCounter++;
          try { await commitOffset(); } catch (e) { /* ... */ }
          await heartbeat();
          return;
        }

        /* Drop the log and consume if it redirected to a known safe domain */
        if (crawlLog.redirectedUrl && isSafeRedirectDomain(crawlLog.redirectedUrl)) {
          logger.debug(`Skipping ${url} — redirected to safe domain: ${crawlLog.redirectedUrl}`);
          consecutiveFailures = 0;
          siteCounter++;
          try { await commitOffset(); } catch (e) { /* ... */ }
          await heartbeat();
          return;
        }

        // Steps 4-7: analysis (request scanning, MongoDB insert, Kafka publish,
        // offset commit). Wrapped in ANALYSIS_TIMEOUT so a stalled DB or broker
        // cannot wedge the consumer loop. On timeout we still try to commit the
        // offset to avoid redelivery.
        // Snapshot the captured requests so late `Network.getResponseBody`
        // events from in-flight CDP awaits can't mutate response fields under
        // us mid-scan. Shallow copy is sufficient: scanText reads strings, and
        // the network handler reassigns properties (responseBody, status,
        // responseHeaders) rather than mutating their contents.
        const requestsForAnalysis = (crawlLog.requests || []).map(r => ({ ...r }));

        // One-shot commit guard: whichever of (analysis success, analysis
        // timeout) runs first commits the offset; the other becomes a no-op.
        // Avoids the dual-commit race where the deadline fires mid-publish and
        // both paths reach commitOffset.
        let committed = false;
        const tryCommit = async () => {
          if (committed) return;
          committed = true;
          try {
            await commitOffset();
          } catch (e) {
            console.error(`commit failed for ${url} (likely rebalance in progress, will be redelivered): ${e.message}`);
          }
        };

        const analysis = (async () => {
          // Step 4: scan captured requests for non-false-flagged token hits.
          const allMapped = mapRequests(requestsForAnalysis);
          const interestingRequests = [];
          const matchedTokens = new Set();
          const matchedAddresses = [];

          for (const req of allMapped) {
            //skip request if it's made to a known safe endpoint
            if (isSafeEndpoint(req.endpoint || '')) continue; 
            const urlScan = scanText(req.endpoint || '', urlTerms, falseFlags);
            const reqScan = scanText(req.requestBody || '', searchTerms, falseFlags);
            const respScan = scanText(req.responseBody || '', searchTerms, falseFlags);
            if (urlScan.interesting || reqScan.interesting || respScan.interesting) {
              const reqTokens = new Set();
              urlScan.tokens.forEach(t => { matchedTokens.add(t); reqTokens.add(t); });
              reqScan.tokens.forEach(t => { matchedTokens.add(t); reqTokens.add(t); });
              respScan.tokens.forEach(t => { matchedTokens.add(t); reqTokens.add(t); });
              interestingRequests.push({
                request: req,
                matchedTokens: [...reqTokens]
              });
              urlScan.addresses.forEach(a => matchedAddresses.push(a.split(':')));
              reqScan.addresses.forEach(a => matchedAddresses.push(a.split(':')));
              respScan.addresses.forEach(a => matchedAddresses.push(a.split(':')));
            }
          }

          // Step 5: detect any privacy interaction the wallet flow recorded.
          const anyPrivacyInteraction = !!(
            crawlLog.connected || crawlLog.signature_request || crawlLog.switch_network
          );
          const interesting = interestingRequests.length > 0 || anyPrivacyInteraction;

          if (anyPrivacyInteraction) metrics.walletInteractions.inc();
          if (interestingRequests.length > 0) metrics.filterPassedRequests.inc();

          // Step 6: conditionally write the full record to the crawls collection.
          if (interesting) {
            const redirectedUrl = crawlLog.redirectedUrl || url;
            const status = typeof crawlLog.status === 'number' ? crawlLog.status : -1;
            const pageSrc = crawlLog.pageSrc || '';
            const interactions = buildInteractions(crawlLog);

            console.log(
              `Interesting crawl for ${url}: ${interestingRequests.length} matching requests, ` +
              `tokens={${Array.from(matchedTokens).join(',')}}, walletInteraction=${anyPrivacyInteraction}`
            );

            try {
              // insertCrawlResult swallows Mongo exceptions and returns null on
              // failure (see mongodb.js:105-108); only count the metric when the
              // write actually landed.
              const result = await insertCrawlResult(
                url,
                redirectedUrl,
                accessedDate,
                status,
                pageSrc,
                interestingRequests,
                interactions,
                matchedAddresses,
                crawlLog.evalScripts || [],
                4 // crawlerVersion — bump when making schema-affecting changes
              );
              if (result) {
                metrics.crawlInserts.inc();
              }
            } catch (e) {
              console.error(`Failed to insert crawls record for ${url}: ${e.message}`);
            }
          } else {
            logger.debug(`Skipping crawls insert for ${url} (no interesting tokens or interactions)`);
          }

          // Step 7: always publish to crawled-urls on a successful crawl, then
          // commit the Kafka offset (via tryCommit so the deadline path can't
          // commit twice).
          try {
            await producer.send({
              topic: INDEX_TOPIC,
              messages: [{ value: url }]
            });
          } catch (e) {
            console.error(`Failed to produce to ${INDEX_TOPIC} for ${url}: ${e.message}`);
          }

          await tryCommit();
        })();

        let analysisTimer;
        const analysisDeadline = new Promise((_, rej) => {
          analysisTimer = setTimeout(() => rej(new Error('analysis timeout')), ANALYSIS_TIMEOUT);
        });
        // Pre-attach a swallow so a late rejection from the deadline (if Race
        // already resolved on `analysis`) doesn't surface as unhandled.
        analysisDeadline.catch(() => {});
        try {
          await Promise.race([analysis, analysisDeadline]);
        } catch (e) {
          if (e && e.message === 'analysis timeout') {
            console.warn(`Analysis exceeded ${ANALYSIS_TIMEOUT}ms for ${url} — committing offset and moving on`);
            await tryCommit();
          } else {
            console.error(`Analysis error for ${url}: ${e && e.message ? e.message : e}`);
          }
          // Suppress any late rejection from the still-running analysis promise.
          analysis.catch(() => {});
        } finally {
          clearTimeout(analysisTimer);
        }

        await heartbeat();
        siteCounter++;
        } finally {
          clearInterval(heartbeatTicker);
        }
      }
    });
  }

  consumer.on(consumer.events.CRASH, async (event) => {
    console.error('Consumer crashed, restarting in 5s:', event.payload.error.message);
    await sleep(5000);
    try {
      await consumer.connect();
      await runConsumer();
    } catch (e) {
      console.error('Consumer failed to restart, exiting:', e.message);
      process.exit(1);
    }
  });

  await runConsumer();
  
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
