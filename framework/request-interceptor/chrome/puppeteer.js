const path = require('path')
const { addExtra } = require('puppeteer-extra')
const puppeteerExtraPluginStealthLib = require('puppeteer-extra-plugin-stealth')
const { getLogger } = require('./logging')

// Force puppeteer-extra to wrap full `puppeteer` (which ships bundled
// Chromium) rather than auto-resolving `puppeteer-core` (which does not).
const puppeteerExtraLib = addExtra(require('puppeteer'))
puppeteerExtraLib.use(puppeteerExtraPluginStealthLib())

const launch = async args => {
  const puppeteerArgs = {
    defaultViewport: null,
    args: [],
    headless: args.headless
  }
  puppeteerArgs.args.push(`--start-maximized`)
  puppeteerArgs.args.push(`--disable-popup-blocking`)
  puppeteerArgs.args.push(`--allow-popups-during-upload`)
  puppeteerArgs.args.push(`--disable-site-isolation-trials`)
  puppeteerArgs.args.push(`--no-sandbox`)
  puppeteerArgs.args.push(`--disable-dev-shm-usage`)

  if (args.walletPath) {
    const resolvedWalletPath = path.resolve(args.walletPath)
    puppeteerArgs.args.push(`--disable-extensions-except=${resolvedWalletPath}`)
    puppeteerArgs.args.push(`--load-extension=${resolvedWalletPath}`)
  }

  if (args.profilePath) {
    puppeteerArgs.args.push(`--user-data-dir=${args.profilePath}`)
  }

  if (args.extraArgs) {
    puppeteerArgs.args.push(...args.extraArgs)
  }

  const browser =  await puppeteerExtraLib.launch(puppeteerArgs)

  const pages = await browser.pages()

  return browser
}

module.exports = {
  launch
}
