const http = require('http');
const client = require('prom-client');

const register = client.register;

const urlsConsumed = new client.Counter({
  name: 'wallet_crawler_urls_consumed_total',
  help: 'Total URLs consumed from the Kafka topic by this crawler instance.'
});

const crawlInserts = new client.Counter({
  name: 'wallet_crawler_crawl_inserts_total',
  help: 'Total successful inserts into the MongoDB crawls collection by this crawler instance.'
});

const crawlsCompleted = new client.Counter({
  name: 'wallet_crawler_crawls_completed_total',
  help: 'Crawls where the retry loop produced a non-null result (denominator for outcome ratios).'
});

const walletInteractions = new client.Counter({
  name: 'wallet_crawler_wallet_interactions_total',
  help: 'Crawls where the dApp invoked the wallet (connect, signature_request, or switch_network).'
});

const filterPassedRequests = new client.Counter({
  name: 'wallet_crawler_filter_passed_requests_total',
  help: 'Crawls where at least one captured request passed the search-term/false-flag filter.'
});

const port = parseInt(process.env.METRICS_PORT || '9090', 10);

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/metrics') {
    try {
      const body = await register.metrics();
      res.writeHead(200, { 'Content-Type': register.contentType });
      res.end(body);
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(`metrics error: ${e.message}`);
    }
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

server.listen(port, () => {
  console.log(`Metrics server listening on :${port}/metrics`);
});

server.on('error', (e) => {
  console.error(`Metrics server error: ${e.message}`);
});

module.exports = { urlsConsumed, crawlInserts, crawlsCompleted, walletInteractions, filterPassedRequests };
