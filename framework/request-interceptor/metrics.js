const http = require('http');
const os = require('os');
const client = require('prom-client');

const register = client.register;

register.setDefaultLabels({
  host: os.hostname(),
  instance: process.env.INSTANCE_ID || '0'
});

const urlsConsumed = new client.Counter({
  name: 'wallet_crawler_urls_consumed_total',
  help: 'Total URLs consumed from the Kafka topic by this crawler instance.'
});

const crawlInserts = new client.Counter({
  name: 'wallet_crawler_crawl_inserts_total',
  help: 'Total successful inserts into the MongoDB crawls collection by this crawler instance.'
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

module.exports = { urlsConsumed, crawlInserts };
