'use strict';

const fs = require('fs');
const path = require('path');

const SEARCH_TERMS_FILENAME = 'crawler_search_terms.json';
const FALSE_FLAGS_FILENAME = 'crawler_false_flags.json';

/**
 * Load the search terms and false flags configuration from JSON files in the
 * given directory (defaults to this module's directory). Both files are
 * optional: if either is missing or malformed, an empty default is used and a
 * warning is logged so the crawler can still run in dev environments.
 *
 * crawler_search_terms.json: ["term1", "term2", ...]
 * crawler_false_flags.json:  { "term1": [["bankroll", 1], ...], ... }
 *
 * All search terms and false-flag reference strings (and the keys of the
 * false_flags map) are lowercased so matching is case-insensitive.
 *
 * @param {string} [dir] Directory containing the two JSON files.
 * @returns {{searchTerms: string[], falseFlags: Object<string, Array<[string, number]>>}}
 */
function loadConfig(dir) {
  const baseDir = dir || __dirname;
  const searchTermsPath = path.join(baseDir, SEARCH_TERMS_FILENAME);
  const falseFlagsPath = path.join(baseDir, FALSE_FLAGS_FILENAME);

  let searchTerms = [];
  try {
    const raw = fs.readFileSync(searchTermsPath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error('expected a JSON array of strings');
    }
    searchTerms = parsed
      .filter(t => typeof t === 'string' && t.length > 0)
      .map(t => t.toLowerCase());
  } catch (e) {
    console.warn(`[match] Could not load ${searchTermsPath}: ${e.message}. Using empty search_terms.`);
    searchTerms = [];
  }

  let falseFlags = {};
  try {
    const raw = fs.readFileSync(falseFlagsPath, 'utf8');
    const parsed = JSON.parse(raw);
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('expected a JSON object');
    }
    for (const key of Object.keys(parsed)) {
      const list = parsed[key];
      if (!Array.isArray(list)) continue;
      const normalized = [];
      for (const entry of list) {
        if (!Array.isArray(entry) || entry.length < 2) continue;
        const refStr = entry[0];
        const offset = entry[1];
        if (typeof refStr !== 'string' || typeof offset !== 'number') continue;
        normalized.push([refStr.toLowerCase(), offset]);
      }
      falseFlags[key.toLowerCase()] = normalized;
    }
  } catch (e) {
    console.warn(`[match] Could not load ${falseFlagsPath}: ${e.message}. Using empty false_flags.`);
    falseFlags = {};
  }

  console.log(`[match] Loaded ${searchTerms.length} search terms and ${Object.keys(falseFlags).length} false-flag entries.`);
  return { searchTerms, falseFlags };
}

/**
 * Scan a text blob for any search-term hit that is not eliminated by a
 * false flag. For each occurrence p of a token in the (lowercased) text, the
 * window text[p - offset : p - offset + refStr.length] is compared verbatim to
 * refStr for every (refStr, offset) pair in falseFlags[token]. If the window
 * matches any false_flag reference string, that occurrence is discarded. If
 * every occurrence of a token is discarded, the token is not interesting in
 * this text. If at least one occurrence survives, the token is added to the
 * returned set and `interesting` is true.
 *
 * @param {string} text Text to scan (typically a request URL, body, or response body).
 * @param {string[]} searchTerms Lowercased list of tokens to look for.
 * @param {Object<string, Array<[string, number]>>} falseFlags Map of token -> [[refStr, offset], ...].
 * @returns {{interesting: boolean, tokens: Set<string>}}
 */
function scanText(text, searchTerms, falseFlags) {
  const result = { interesting: false, tokens: new Set() };
  if (!text || typeof text !== 'string' || !searchTerms || searchTerms.length === 0) {
    return result;
  }

  const haystack = text.toLowerCase();
  const ffMap = falseFlags || {};

  for (const token of searchTerms) {
    if (!token) continue;
    const tokenFlags = ffMap[token] || [];

    let pos = haystack.indexOf(token);
    let foundReal = false;
    while (pos !== -1) {
      let isFalseFlag = false;
      for (const entry of tokenFlags) {
        const refStr = entry[0];
        const offset = entry[1];
        const start = pos - offset;
        const end = start + refStr.length;
        if (start < 0 || end > haystack.length) continue;
        if (haystack.substring(start, end) === refStr) {
          isFalseFlag = true;
          break;
        }
      }
      if (!isFalseFlag) {
        foundReal = true;
        break;
      }
      pos = haystack.indexOf(token, pos + 1);
    }

    if (foundReal) {
      result.tokens.add(token);
      result.interesting = true;
    }
  }

  return result;
}

module.exports = {
  loadConfig,
  scanText,
  SEARCH_TERMS_FILENAME,
  FALSE_FLAGS_FILENAME
};
