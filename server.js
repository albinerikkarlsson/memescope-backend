// MemeScope backend - Phase 1 + on-chain analysis (bundled wallets, creator history)
// Listens to Pump.fun launches and trades (via PumpPortal), calculates metrics,
// and serves them to the website through REST + WebSocket.
// Everything is kept in memory (no database needed yet). See docs/api-contract.md.

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { WebSocketServer, WebSocket } from 'ws';
import { analyzeLaunch, creatorLaunches, chainStats, onchainEnabled, onchainCapped, dailyCap, CapError } from './onchain.js';

// ---------------------------------------------------------------- settings
const PORT = Number(process.env.PORT) || 3000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map((s) => s.trim()).filter(Boolean);
const PUMP_WS_URL = process.env.PUMPPORTAL_WS_URL || 'wss://pumpportal.fun/api/data';

const TOTAL_SUPPLY = 1_000_000_000; // every Pump.fun token has 1 billion supply
const INIT_VIRTUAL_TOKENS = 1_073_000_000; // bonding curve start (virtual token reserve)
const CURVE_TOKENS = 793_100_000; // tokens sold on the curve before graduation
const SLOT_MS = 10_000; // trades are grouped in 10 second slots
const KEEP_SLOTS = 1080; // = 3 hours of slots
const TRADE_KEEP = 200; // last trades kept per token
const SNIPER_MS = 3_000; // buys within 3s of launch count as "sniper"
const MAX_TOKENS = 15_000;
const TOKEN_TTL_MS = 6 * 3600 * 1000; // forget tokens after 6h
const IDLE_UNSUB_MS = 3 * 60 * 1000; // stop streaming trades for a coin after 3 min without trades
const ACTIVE_MAX_AGE_MS = 30 * 60 * 1000; // ...and after 30 min of age, unless it is watched or moving
const DEAD_AFTER_MS = 10 * 60 * 1000; // "dead" needs 10 min without trades AND verified data

// PumpPortal trade streams need a funded API key (set PUMPPORTAL_API_KEY on Render).
const API_KEY = process.env.PUMPPORTAL_API_KEY || '';
const FORCE_SUBSCRIBE = process.env.FORCE_TRADE_SUBSCRIBE === '1'; // try trade streams even without a key
const DAILY_TRADE_CAP = Number(process.env.DAILY_TRADE_CAP) || 50_000; // max streamed trades per UTC day (cost control)

// DEX Screener fallback for coins whose trades are not streamed (free, rate limited)
const DEX_URL = process.env.DEXSCREENER_URL || 'https://api.dexscreener.com';
const DEX_POLL_MS = 20_000;
const DEX_BATCH = 30; // addresses per request
const DEX_BATCHES_PER_CYCLE = 10; // = max 30 requests/min
const DEX_MAX_AGE_MS = 3 * 3600 * 1000; // only coins younger than 3h
const GRAD_MIN_LIQ_USD = 10_000; // a PumpSwap pool needs at least this much liquidity to count as graduated

// Saved state (survives restarts if DATA_DIR is on a persistent disk)
const DATA_DIR = process.env.DATA_DIR || './data';
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const SAVE_MS = 60_000;
const SAVE_MAX_TOKENS = 3000;
const SAVE_MAX_SLOTS = 180; // 30 min of 10s slots
const FLUSH_MS = 300; // how often updates are pushed to browsers
const FULL_REFRESH_MS = 5_000; // how often time-based fields are recalculated

// ---------------------------------------------------------------- state
const tokens = new Map(); // mint -> token
const byCreator = new Map(); // creator wallet -> Set of mints
const dirty = new Set(); // mints changed since last flush
const newCoins = []; // mints created since last flush
const pendingTrades = new Map(); // mint -> trades to push to coin subscribers
const clients = new Set(); // connected browsers
const subQueue = new Set(); // mints waiting for a trade subscription
const startedAt = Date.now();

let solPrice = { usd: null, sek: null, eur: null, updatedAt: 0, status: 'calculating', reason: 'Waiting for first price', source: null };
const stats = { upstream: 'connecting', events: 0, eventsPerSec: 0, lastUpstreamMsg: 0, otherLogged: 0, tradesToday: 0, day: new Date().toISOString().slice(0, 10), capped: false, dexOk: 0, dexLastAt: 0 };

// ---------------------------------------------------------------- helpers
const num = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
};
const clamp = (x, a, b) => Math.min(b, Math.max(a, x));
const round = (x, d = 4) => (x === null || x === undefined || !Number.isFinite(x) ? null : Number(x.toFixed(d)));
const pct = (now, past) => (past > 0 ? ((now - past) / past) * 100 : 0);

function ipfsToHttp(u) {
  if (!u || typeof u !== 'string') return null;
  if (u.startsWith('ipfs://')) return 'https://ipfs.io/ipfs/' + u.slice(7).replace(/^ipfs\//, '');
  return u;
}

// ---------------------------------------------------------------- token logic
function wallet(t, addr, now) {
  let w = t.wallets.get(addr);
  if (!w) {
    w = { bal: 0, bTok: 0, bSol: 0, sTok: 0, sSol: 0, firstAt: now };
    t.wallets.set(addr, w);
  }
  return w;
}

function setBalance(t, w, newBal) {
  const was = w.bal > 0;
  w.bal = newBal;
  const is = newBal > 0;
  if (!was && is) t.holderCount++;
  else if (was && !is) t.holderCount--;
  t.holdersDirty = true;
}

function addToSlot(t, now, side, sol) {
  const k = Math.floor(now / SLOT_MS);
  let s = t.slots[t.slots.length - 1];
  if (!s || s.k !== k) {
    const open = s ? s.c : t.mcap;
    s = { k, o: open, h: Math.max(open, t.mcap), l: Math.min(open, t.mcap), c: t.mcap, b: 0, s: 0, v: 0 };
    t.slots.push(s);
    while (t.slots.length > KEEP_SLOTS) t.slots.shift();
  }
  s.h = Math.max(s.h, t.mcap);
  s.l = Math.min(s.l, t.mcap);
  s.c = t.mcap;
  if (side === 'buy') s.b++;
  else s.s++;
  s.v += sol;
}

// ---- trade stream state
function streamWanted() {
  return !stats.capped && (Boolean(API_KEY) || FORCE_SUBSCRIBE);
}
function tradeStreamState() {
  if (stats.capped) return 'capped';
  if (!API_KEY && !FORCE_SUBSCRIBE) return 'no_key';
  if (stats.upstream !== 'connected') return 'connecting';
  return 'live';
}
const streamOn = () => tradeStreamState() === 'live';

function ensureSubscribed(t) {
  if (t.subscribed || !streamWanted()) return;
  t.subscribed = true;
  t.idleVerified = false;
  subQueue.add(t.mint);
}

// true when we really know whether the coin is trading or not (live stream, a verified idle period, or fresh DEX Screener data)
function isVerified(t, now) {
  return t.idleVerified || (t.subscribed && streamOn()) || Boolean(t.ext && now - t.ext.at < 120_000);
}

function usingExt(t, now) {
  return Boolean(t.ext && now - t.ext.at < 90_000 && !(t.subscribed && streamOn()));
}

function makeToken(o, now) {
  const mcap = o.mcap || 0;
  return {
    mint: o.mint,
    name: String(o.name || '').slice(0, 100),
    symbol: String(o.symbol || '').slice(0, 40),
    uri: o.uri || null,
    creator: o.creator || null,
    createdAt: o.createdAt ?? now,
    image: o.image || null,
    description: o.description || null,
    twitter: o.twitter || null,
    telegram: o.telegram || null,
    website: o.website || null,
    meta: o.metaLive
      ? { status: 'live', reason: null, tries: 0 }
      : { status: o.uri ? 'calculating' : 'not_applicable', reason: o.uri ? 'Fetching token metadata' : 'Token has no metadata link', tries: 0 },
    mcap,
    initialMcap: o.initialMcap ?? mcap,
    ath: o.ath ?? mcap,
    vTokens: o.vTokens ?? null,
    vSol: o.vSol ?? null,
    lastTradeAt: o.lastTradeAt ?? now,
    tradeCount: o.tradeCount || 0,
    slots: o.slots || [],
    trades: [],
    wallets: new Map(),
    holderCount: 0,
    snipers: new Set(),
    creatorMax: o.creatorMax || 0,
    top10Pct: 0,
    top10At: 0,
    holdersDirty: true,
    subscribed: false,
    ext: null, // latest DEX Screener data (fallback when trades are not streamed)
    extCheckedAt: 0,
    idleVerified: Boolean(o.idleVerified),
    gradExt: Boolean(o.gradExt),
    chain: o.chain && o.chain.status === 'done' ? o.chain : { status: 'idle' }, // on-chain launch analysis
    sum: null,
  };
}

function registerToken(t) {
  tokens.set(t.mint, t);
  if (t.creator) {
    if (!byCreator.has(t.creator)) byCreator.set(t.creator, new Set());
    byCreator.get(t.creator).add(t.mint);
  }
}

function onCreate(d) {
  if (!d.mint || tokens.has(d.mint)) return;
  if (tokens.size >= MAX_TOKENS) housekeeping(true);
  const now = Date.now();
  const t = makeToken(
    {
      mint: d.mint,
      name: d.name,
      symbol: d.symbol,
      uri: d.uri,
      creator: d.traderPublicKey,
      image: ipfsToHttp(d.imageUri),
      mcap: num(d.marketCapSol) || 0,
      vTokens: num(d.vTokensInBondingCurve),
      vSol: num(d.vSolInBondingCurve),
    },
    now,
  );
  // creator's own first buy
  const initialBuy = num(d.initialBuy);
  if (t.creator && initialBuy && initialBuy > 0) {
    const w = wallet(t, t.creator, now);
    w.bTok = initialBuy;
    w.bSol = num(d.solAmount) || 0;
    setBalance(t, w, initialBuy);
    t.creatorMax = initialBuy;
  }
  registerToken(t);
  t.sum = buildSummary(t, now);
  newCoins.push(t.mint);
  ensureSubscribed(t); // only streams trades if a key is set and the daily cap is not reached
  if (t.uri) enqueueMeta(t);
}

function onTrade(d) {
  const t = tokens.get(d.mint);
  if (!t) return;
  const now = Date.now();
  const side = d.txType;
  const sol = num(d.solAmount) || 0;
  const tok = num(d.tokenAmount) || 0;
  const mcap = num(d.marketCapSol);
  if (mcap !== null && mcap > 0) {
    t.mcap = mcap;
    if (mcap > t.ath) t.ath = mcap;
  }
  const vt = num(d.vTokensInBondingCurve);
  const vs = num(d.vSolInBondingCurve);
  if (vt !== null) t.vTokens = vt;
  if (vs !== null) t.vSol = vs;
  t.lastTradeAt = now;
  t.idleVerified = false;
  t.tradeCount++;
  addToSlot(t, now, side, sol);

  const addr = d.traderPublicKey;
  if (addr) {
    const w = wallet(t, addr, now);
    if (side === 'buy') {
      w.bTok += tok;
      w.bSol += sol;
      if (now - t.createdAt <= SNIPER_MS && addr !== t.creator) t.snipers.add(addr);
    } else {
      w.sTok += tok;
      w.sSol += sol;
    }
    const nb = num(d.newTokenBalance);
    setBalance(t, w, nb !== null ? Math.max(0, nb) : Math.max(0, w.bal + (side === 'buy' ? tok : -tok)));
    if (addr === t.creator && w.bal > t.creatorMax) t.creatorMax = w.bal;
  }

  const trade = {
    signature: d.signature || null,
    time: now,
    side,
    solAmount: round(sol, 6),
    tokenAmount: round(tok, 2),
    wallet: addr || null,
    marketCapSol: round(t.mcap, 4),
    priceSol: t.mcap / TOTAL_SUPPLY,
  };
  t.trades.push(trade);
  if (t.trades.length > TRADE_KEEP) t.trades.shift();
  if (!pendingTrades.has(t.mint)) pendingTrades.set(t.mint, []);
  pendingTrades.get(t.mint).push(trade);
  dirty.add(t.mint);
}

// ---- rolling windows
function windowStats(t, now) {
  const kNow = Math.floor(now / SLOT_MS);
  const vol = { m1: 0, m5: 0, h1: 0 };
  const tx = { m1: { buys: 0, sells: 0 }, m5: { buys: 0, sells: 0 }, h1: { buys: 0, sells: 0 } };
  for (let i = t.slots.length - 1; i >= 0; i--) {
    const s = t.slots[i];
    const ago = kNow - s.k;
    if (ago >= 360) break;
    vol.h1 += s.v;
    tx.h1.buys += s.b;
    tx.h1.sells += s.s;
    if (ago < 30) {
      vol.m5 += s.v;
      tx.m5.buys += s.b;
      tx.m5.sells += s.s;
    }
    if (ago < 6) {
      vol.m1 += s.v;
      tx.m1.buys += s.b;
      tx.m1.sells += s.s;
    }
  }
  return { vol, tx };
}

function mcapAgo(t, now, ms) {
  const target = Math.floor((now - ms) / SLOT_MS);
  for (let i = t.slots.length - 1; i >= 0; i--) {
    if (t.slots[i].k <= target) return t.slots[i].c;
  }
  return t.initialMcap;
}

function progress(t) {
  if (t.gradExt) return 1; // DEX Screener lists the coin on a DEX (it has left the bonding curve)
  if (t.vTokens === null) return 0;
  return clamp((INIT_VIRTUAL_TOKENS - t.vTokens) / CURVE_TOKENS, 0, 1);
}

function refreshHolders(t, now) {
  if (!t.holdersDirty || now - t.top10At < 1000) return;
  const bals = [];
  for (const w of t.wallets.values()) if (w.bal > 0) bals.push(w.bal);
  bals.sort((a, b) => b - a);
  let top = 0;
  for (let i = 0; i < Math.min(10, bals.length); i++) top += bals[i];
  t.top10Pct = (top / TOTAL_SUPPLY) * 100;
  t.top10At = now;
  t.holdersDirty = false;
}

// Status rules (version 1, heuristics - documented in docs/api-contract.md)
function categoriesFor(t, now, win, prog, changeM5) {
  const age = now - t.createdAt;
  const idle = now - t.lastTradeAt;
  const devSold = t.creatorMax > 0 && (t.wallets.get(t.creator)?.bal ?? 0) <= t.creatorMax * 0.2;
  const crashed = t.initialMcap > 0 && t.ath >= t.initialMcap * 1.5 && t.mcap <= t.ath * 0.4;
  const crashedHard = t.initialMcap > 0 && t.ath >= t.initialMcap * 2 && t.mcap <= t.ath * 0.2;
  const rugged = (crashed && devSold) || crashedHard;
  const cats = [];
  if (prog >= 1) cats.push('graduated');
  if (rugged) cats.push('rugged');
  if (idle > DEAD_AFTER_MS && isVerified(t, now) && !rugged && prog < 1) cats.push('dead'); // never 'dead' without real data
  if (prog >= 0.8 && prog < 1 && !rugged) cats.push('about_to_graduate');
  if (age < 5 * 60 * 1000) cats.push('new');
  const trades1m = win.tx.m1.buys + win.tx.m1.sells;
  if (idle < 60 * 1000 && (changeM5 >= 20 || trades1m >= 10) && !rugged) cats.push('moving');
  if (!cats.length) cats.push('cooling');
  const priority = ['graduated', 'rugged', 'dead', 'about_to_graduate', 'moving', 'new', 'cooling'];
  const status = priority.find((p) => cats.includes(p));
  return { cats, status };
}

function buildSummary(t, now) {
  refreshHolders(t, now);
  const win = windowStats(t, now);
  const useExt = usingExt(t, now);
  if (useExt) {
    // trades are not streamed for this coin: use DEX Screener numbers for 5m/1h windows
    const usd = solPrice.usd;
    const toSol = (v) => (usd ? (v || 0) / usd : 0);
    win.vol.m5 = toSol(t.ext.volM5Usd);
    win.vol.h1 = toSol(t.ext.volH1Usd);
    win.tx.m5 = { ...t.ext.m5 };
    win.tx.h1 = { ...t.ext.h1 };
  }
  const prog = progress(t);
  const changeM5 = useExt && t.ext.chM5 !== null ? t.ext.chM5 : pct(t.mcap, mcapAgo(t, now, 5 * 60 * 1000));
  const changeH1 = useExt && t.ext.chH1 !== null ? t.ext.chH1 : pct(t.mcap, mcapAgo(t, now, 3600 * 1000));
  const dataSource = t.subscribed && streamOn() ? 'stream' : useExt ? 'dexscreener' : t.tradeCount > 0 ? 'stream_stale' : 'launch_only';
  const { cats, status } = categoriesFor(t, now, win, prog, changeM5);
  const creatorBal = t.creator ? t.wallets.get(t.creator)?.bal ?? 0 : 0;
  let sniperBal = 0;
  for (const a of t.snipers) sniperBal += t.wallets.get(a)?.bal ?? 0;

  const notes = { riskScore: { status: 'not_applicable', reason: 'This service does not score risk; the website shows the RugCheck report' } };
  const bn = bundleNote(t);
  if (bn) notes.bundledPct = bn;
  if (t.meta.status !== 'live') notes.metadata = { status: t.meta.status, reason: t.meta.reason };
  if (t.vTokens === null && !t.gradExt) notes.bondingProgress = { status: 'calculating', reason: 'Waiting for first curve data' };
  if (now - t.lastTradeAt > DEAD_AFTER_MS && !isVerified(t, now)) notes.activity = { status: 'calculating', reason: 'No live trade data for this coin right now, so activity is unverified (not marked dead)' };

  return {
    mint: t.mint,
    name: t.name,
    symbol: t.symbol,
    image: t.image,
    creator: t.creator,
    createdAt: t.createdAt,
    status,
    categories: cats,
    priceSol: t.mcap / TOTAL_SUPPLY,
    marketCapSol: round(t.mcap, 4),
    athMarketCapSol: round(t.ath, 4),
    initialMarketCapSol: round(t.initialMcap, 4),
    bondingProgress: round(prog, 4),
    volumeSol: { m1: round(win.vol.m1, 4), m5: round(win.vol.m5, 4), h1: round(win.vol.h1, 4) },
    txns: win.tx,
    changePct: {
      m1: round(pct(t.mcap, mcapAgo(t, now, 60 * 1000)), 2),
      m5: round(changeM5, 2),
      h1: round(changeH1, 2),
    },
    holders: t.holderCount,
    top10Pct: round(t.top10Pct, 2),
    devHoldingPct: round((creatorBal / TOTAL_SUPPLY) * 100, 2),
    sniperPct: round((sniperBal / TOTAL_SUPPLY) * 100, 2),
    bundledPct: t.chain.status === 'done' ? t.chain.bundledPct : null,
    bundleWallets: t.chain.status === 'done' ? t.chain.bundleWallets : null,
    tradeCount: t.tradeCount,
    lastTradeAt: t.lastTradeAt,
    socials: { twitter: t.twitter, telegram: t.telegram, website: t.website },
    links: { pumpfun: 'https://pump.fun/coin/' + t.mint, solscan: 'https://solscan.io/token/' + t.mint },
    dataSource,
    notes,
  };
}

// ---------------------------------------------------------------- on-chain launch analysis (bundled wallets)
const CHAIN_MIN_AGE_MS = 20_000; // wait until the launch slot is confirmed
const CHAIN_RETRY_MS = 2 * 60 * 1000;
const chainQueue = [];

function bundleNote(t) {
  const c = t.chain;
  if (c.status === 'done') return null;
  if (!onchainEnabled()) return { status: 'not_applicable', reason: 'On-chain analysis is off (HELIUS_API_KEY not set)' };
  if (c.status === 'queued' || c.status === 'running') return { status: 'calculating', reason: 'Checking launch-block wallets on-chain' };
  if (c.status === 'error') return { status: 'error', reason: c.reason };
  if (onchainCapped()) return { status: 'stale', reason: 'Daily on-chain analysis limit reached; resumes next UTC day' };
  return { status: 'not_applicable', reason: 'Analyzed when a coin is opened or gains traction' };
}

// Only coins people look at or that gain traction are analyzed, to protect the Helius plan.
function wantsChain(t, now, viewed = false) {
  if (!onchainEnabled() || onchainCapped()) return false;
  if (t.chain.status === 'done' || t.chain.status === 'queued' || t.chain.status === 'running') return false;
  if (t.chain.status === 'error' && now - t.chain.at < CHAIN_RETRY_MS) return false;
  if (now - t.createdAt < CHAIN_MIN_AGE_MS) return false;
  if (viewed) return true;
  const cats = t.sum?.categories || [];
  return cats.includes('moving') || cats.includes('about_to_graduate') || t.ath >= t.initialMcap * 1.5 || t.tradeCount >= 20;
}

function queueChain(t, viewed = false) {
  if (viewed) t.chainViewed = true; // remembered, so a coin opened in its first seconds is analyzed once old enough
  if (!wantsChain(t, Date.now(), viewed || t.chainViewed)) return;
  t.chain = { status: 'queued' };
  if (viewed) chainQueue.unshift(t.mint);
  else chainQueue.push(t.mint);
  dirty.add(t.mint);
}

let chainRunning = 0;
async function pumpChain() {
  while (chainRunning < 2 && chainQueue.length) {
    const t = tokens.get(chainQueue.shift());
    if (!t || t.chain.status !== 'queued') continue;
    chainRunning++;
    t.chain = { status: 'running' };
    analyzeLaunch(t.mint, t.creator)
      .then((r) => (t.chain = { status: 'done', at: Date.now(), ...r }))
      .catch((e) => (t.chain = e instanceof CapError ? { status: 'idle' } : { status: 'error', at: Date.now(), reason: 'On-chain check failed: ' + e.message }))
      .finally(() => {
        chainRunning--;
        dirty.add(t.mint);
        pumpChain();
      });
  }
}
setInterval(() => {
  for (const t of tokens.values()) queueChain(t);
  pumpChain();
}, 5_000);

// creator history cache (wallet|cursor -> result) so repeated page views do not spend RPC calls
const creatorCache = new Map();
async function creatorOnChain(wallet, cursor) {
  const key = wallet + '|' + (cursor || '');
  const hit = creatorCache.get(key);
  if (hit && Date.now() - hit.at < 10 * 60 * 1000) return hit.value;
  const value = creatorLaunches(wallet, cursor);
  creatorCache.set(key, { at: Date.now(), value });
  value.catch(() => creatorCache.delete(key));
  if (creatorCache.size > 500) creatorCache.delete(creatorCache.keys().next().value);
  return value;
}

async function creatorResponse(walletAddr, cursor) {
  const seen = [...(byCreator.get(walletAddr) || [])].map((m) => tokens.get(m)?.sum).filter(Boolean);
  let onChain;
  if (!onchainEnabled()) onChain = { status: 'not_applicable', reason: 'On-chain history is off (HELIUS_API_KEY not set)', launches: [], scanned: 0, nextCursor: null };
  else {
    try {
      const r = await creatorOnChain(walletAddr, cursor);
      onChain = { status: 'live', reason: null, ...r };
    } catch (e) {
      onChain = { status: e instanceof CapError ? 'stale' : 'error', reason: e.message, launches: [], scanned: 0, nextCursor: null };
    }
  }
  return {
    wallet: walletAddr,
    coinsSeen: seen.length,
    ruggedCount: seen.filter((c) => c.status === 'rugged').length,
    deadCount: seen.filter((c) => c.status === 'dead').length,
    coins: seen.sort((a, b) => b.createdAt - a.createdAt).slice(0, 100),
    onChain,
    coverage: {
      status: onChain.status === 'live' ? 'live' : onChain.status,
      reason: onChain.status === 'live'
        ? `Verified Pump.fun launches in the ${onChain.scanned} most recent wallet transactions loaded; use nextCursor for older history. Rugged/dead counts cover coins this server tracked.`
        : onChain.reason,
    },
  };
}

function diffSummary(prev, next) {
  if (!prev) return next;
  const out = {};
  let changed = false;
  for (const k of Object.keys(next)) {
    if (JSON.stringify(prev[k]) !== JSON.stringify(next[k])) {
      out[k] = next[k];
      changed = true;
    }
  }
  return changed ? out : null;
}

// ---------------------------------------------------------------- metadata (image, socials)
const metaQueue = [];
let metaActive = 0;

function enqueueMeta(t) {
  metaQueue.push(t.mint);
  pumpMeta();
}

function pumpMeta() {
  while (metaActive < 8 && metaQueue.length) {
    const mint = metaQueue.shift();
    const t = tokens.get(mint);
    if (!t) continue;
    metaActive++;
    fetchMeta(t).finally(() => {
      metaActive--;
      pumpMeta();
    });
  }
}

async function fetchMeta(t) {
  t.meta.tries++;
  const url = ipfsToHttp(t.uri);
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 7000);
    const res = await fetch(url, { signal: ctl.signal });
    clearTimeout(timer);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const j = await res.json();
    t.image = ipfsToHttp(j.image) || t.image;
    t.description = typeof j.description === 'string' ? j.description.slice(0, 1000) : null;
    t.twitter = j.twitter || null;
    t.telegram = j.telegram || null;
    t.website = j.website || null;
    t.meta = { status: 'live', reason: null, tries: t.meta.tries };
  } catch (e) {
    if (t.meta.tries < 4) {
      t.meta.status = 'calculating';
      t.meta.reason = 'Retrying metadata fetch (' + t.meta.tries + '/3)';
      setTimeout(() => enqueueMeta(t), 4000 * t.meta.tries);
    } else {
      t.meta.status = 'error';
      t.meta.reason = 'Metadata source did not respond (' + (e.message || 'error') + ')';
    }
  }
  dirty.add(t.mint);
}

// ---------------------------------------------------------------- SOL price
async function fetchJson(url, ms = 7000) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), ms);
  try {
    const res = await fetch(url, { signal: ctl.signal, headers: { accept: 'application/json' } });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

// Tried in order. The first one that answers wins, so one provider being down does not break the price.
const PRICE_SOURCES = [
  {
    name: 'coinbase',
    run: async () => {
      const j = await fetchJson('https://api.coinbase.com/v2/prices/SOL-USD/spot');
      const usd = Number(j?.data?.amount);
      if (!(usd > 0)) throw new Error('bad response');
      let sek = null;
      let eur = null;
      try {
        const r = await fetchJson('https://api.coinbase.com/v2/exchange-rates?currency=USD');
        const sekRate = Number(r?.data?.rates?.SEK);
        const eurRate = Number(r?.data?.rates?.EUR);
        if (sekRate > 0) sek = usd * sekRate;
        if (eurRate > 0) eur = usd * eurRate;
      } catch {}
      return { usd, sek, eur };
    },
  },
  {
    name: 'coingecko',
    run: async () => {
      const j = await fetchJson('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd,sek,eur');
      const s = j?.solana;
      if (!s || !(s.usd > 0)) throw new Error('bad response');
      return { usd: s.usd, sek: s.sek ?? null, eur: s.eur ?? null };
    },
  },
  {
    name: 'kraken',
    run: async () => {
      const j = await fetchJson('https://api.kraken.com/0/public/Ticker?pair=SOLUSD');
      const first = j?.result && Object.values(j.result)[0];
      const usd = Number(first?.c?.[0]);
      if (!(usd > 0)) throw new Error('bad response');
      return { usd, sek: null, eur: null };
    },
  },
  {
    name: 'dexscreener',
    run: async () => {
      const arr = await fetchJson(DEX_URL + '/tokens/v1/solana/So11111111111111111111111111111111111111112');
      if (!Array.isArray(arr) || !arr.length) throw new Error('bad response');
      const best = arr.slice().sort((a, b) => (num(b.liquidity?.usd) || 0) - (num(a.liquidity?.usd) || 0))[0];
      const usd = Number(best?.priceUsd);
      if (!(usd > 0)) throw new Error('bad response');
      return { usd, sek: null, eur: null };
    },
  },
];

async function pollSolPrice() {
  const errors = [];
  for (const src of PRICE_SOURCES) {
    try {
      const p = await src.run();
      let { usd, sek, eur } = p;
      // sources that only give USD: reuse the last known SEK/EUR exchange rate
      if (sek === null && solPrice.sek && solPrice.usd) sek = usd * (solPrice.sek / solPrice.usd);
      if (eur === null && solPrice.eur && solPrice.usd) eur = usd * (solPrice.eur / solPrice.usd);
      solPrice = { usd, sek, eur, updatedAt: Date.now(), status: 'live', reason: null, source: src.name };
      broadcast({ type: 'sol_price', ...solPrice });
      return;
    } catch (e) {
      errors.push(src.name + ': ' + (e.name === 'AbortError' ? 'timeout' : e.message));
    }
  }
  console.warn('[sol-price] all sources failed:', errors.join(' | '));
  const age = Date.now() - solPrice.updatedAt;
  if (solPrice.updatedAt) {
    // a single failed round is fine; only call it stale after 2 minutes
    if (age > 120_000 && solPrice.status !== 'stale') {
      solPrice = { ...solPrice, status: 'stale', reason: 'All price sources unreachable' };
      broadcast({ type: 'sol_price', ...solPrice });
    }
  } else {
    solPrice = { ...solPrice, status: 'error', reason: 'Could not fetch SOL price from any source' };
  }
}

// ---------------------------------------------------------------- upstream (PumpPortal)
let up = null;
let upBackoff = 1000;

function upstreamUrl() {
  if (!API_KEY) return PUMP_WS_URL;
  return PUMP_WS_URL + (PUMP_WS_URL.includes('?') ? '&' : '?') + 'api-key=' + encodeURIComponent(API_KEY);
}

function sendUp(obj) {
  if (up && up.readyState === WebSocket.OPEN) up.send(JSON.stringify(obj));
}

function rollDay() {
  const d = new Date().toISOString().slice(0, 10);
  if (d === stats.day) return;
  stats.day = d;
  stats.tradesToday = 0;
  if (stats.capped) {
    stats.capped = false;
    console.log('[cap] new UTC day, trade streaming re-enabled');
    const now = Date.now();
    for (const t of tokens.values()) if (now - t.lastTradeAt < IDLE_UNSUB_MS && now - t.createdAt < ACTIVE_MAX_AGE_MS) ensureSubscribed(t);
  }
}

function countTrade() {
  rollDay();
  stats.tradesToday++;
  if (!stats.capped && stats.tradesToday >= DAILY_TRADE_CAP) {
    stats.capped = true;
    console.warn('[cap] daily trade cap reached (' + DAILY_TRADE_CAP + '), stopping trade streams until next UTC day. DEX Screener fallback takes over.');
    const mints = [];
    for (const t of tokens.values()) {
      if (t.subscribed) {
        t.subscribed = false;
        mints.push(t.mint);
      }
    }
    subQueue.clear();
    for (let i = 0; i < mints.length; i += 100) sendUp({ method: 'unsubscribeTokenTrade', keys: mints.slice(i, i + 100) });
  }
}

function connectUpstream() {
  stats.upstream = 'connecting';
  up = new WebSocket(upstreamUrl());
  up.on('open', () => {
    stats.upstream = 'connected';
    stats.lastUpstreamMsg = Date.now();
    upBackoff = 1000;
    console.log('[upstream] connected (trade stream: ' + tradeStreamState() + ')');
    sendUp({ method: 'subscribeNewToken' });
    if (streamWanted()) {
      const active = [...tokens.values()].filter((t) => t.subscribed).map((t) => t.mint);
      for (let i = 0; i < active.length; i += 100) sendUp({ method: 'subscribeTokenTrade', keys: active.slice(i, i + 100) });
    }
  });
  up.on('message', (raw) => {
    stats.lastUpstreamMsg = Date.now();
    stats.events++;
    let d;
    try {
      d = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (d.txType === 'create') onCreate(d);
    else if (d.txType === 'buy' || d.txType === 'sell') {
      countTrade();
      onTrade(d);
    } else if (stats.otherLogged < 10) {
      // e.g. error messages from PumpPortal (missing key, no funds) show up here in the Render logs
      stats.otherLogged++;
      console.log('[upstream] other message:', JSON.stringify(d).slice(0, 300));
    }
  });
  up.on('error', (e) => console.error('[upstream] error:', e.message));
  up.on('close', (code, reason) => {
    stats.upstream = 'reconnecting';
    console.log('[upstream] closed (' + code + ' ' + String(reason || '').slice(0, 100) + '), reconnecting in', upBackoff, 'ms');
    setTimeout(connectUpstream, upBackoff);
    upBackoff = Math.min(upBackoff * 2, 30000);
  });
}

setInterval(() => {
  // send queued trade subscriptions in batches
  if (!subQueue.size || !up || up.readyState !== WebSocket.OPEN) return;
  if (!streamWanted()) {
    subQueue.clear();
    return;
  }
  const keys = [...subQueue].slice(0, 200);
  keys.forEach((k) => subQueue.delete(k));
  sendUp({ method: 'subscribeTokenTrade', keys });
}, 500);

setInterval(rollDay, 30_000);

setInterval(() => {
  // reconnect if the upstream goes silent
  if (stats.upstream === 'connected' && Date.now() - stats.lastUpstreamMsg > 45000) {
    console.warn('[upstream] silent for 45s, restarting connection');
    up.terminate();
  }
}, 10000);

// ---------------------------------------------------------------- DEX Screener fallback
let dexRunning = false;
let dexBackoffUntil = 0;

function applyExt(t, p, now) {
  const usd = solPrice.usd;
  const mcUsd = num(p.marketCap) ?? num(p.fdv);
  const m5 = { buys: num(p.txns?.m5?.buys) || 0, sells: num(p.txns?.m5?.sells) || 0 };
  const h1 = { buys: num(p.txns?.h1?.buys) || 0, sells: num(p.txns?.h1?.sells) || 0 };
  const prevM5 = t.ext ? t.ext.m5.buys + t.ext.m5.sells : 0;
  t.ext = { at: now, volM5Usd: num(p.volume?.m5) || 0, volH1Usd: num(p.volume?.h1) || 0, m5, h1, chM5: num(p.priceChange?.m5), chH1: num(p.priceChange?.h1) };
  if (mcUsd > 0 && usd) {
    t.mcap = mcUsd / usd;
    if (t.mcap > t.ath) t.ath = t.mcap;
  }
  // graduated = a real PumpSwap pool with real liquidity (anyone can create tiny pools for a coin on other DEXes, those are ignored)
  if (p.dexId === 'pumpswap' && (num(p.liquidity?.usd) || 0) >= GRAD_MIN_LIQ_USD) t.gradExt = true;
  const m5Total = m5.buys + m5.sells;
  if (m5Total > 0) {
    t.idleVerified = false;
    // more trades than last check = fresh activity; otherwise the last trade was somewhere in the last 5 min
    t.lastTradeAt = m5Total > prevM5 ? now : Math.max(t.lastTradeAt, now - 5 * 60_000);
  }
  dirty.add(t.mint);
  // a coin that starts moving gets its trades streamed again (if a key is set and the cap allows)
  if (!t.subscribed && ((t.ext.chM5 !== null && t.ext.chM5 >= 20) || m5Total >= 20)) ensureSubscribed(t);
}

async function dexBatch(batch) {
  let arr;
  try {
    arr = await fetchJson(DEX_URL + '/tokens/v1/solana/' + batch.map((t) => t.mint).join(','), 8000);
  } catch (e) {
    if (/429/.test(e.message)) dexBackoffUntil = Date.now() + 60_000;
    console.warn('[dexscreener] batch failed:', e.name === 'AbortError' ? 'timeout' : e.message);
    return;
  }
  if (!Array.isArray(arr)) return;
  const now = Date.now();
  stats.dexOk++;
  stats.dexLastAt = now;
  const best = new Map();
  for (const p of arr) {
    const m = p?.baseToken?.address;
    if (!m) continue;
    if (p.dexId !== 'pumpfun' && p.dexId !== 'pumpswap') continue; // ignore rogue pools on other DEXes
    const liq = num(p.liquidity?.usd) || 0;
    if (p.dexId === 'pumpswap' && liq < GRAD_MIN_LIQ_USD) continue; // pre-graduation dust pool
    // a real PumpSwap pool beats the bonding-curve pair
    const rank = (p.dexId === 'pumpswap' ? 1e12 : 0) + liq;
    const cur = best.get(m);
    if (!cur || rank > cur.liq) best.set(m, { p, liq: rank });
  }
  for (const t of batch) {
    t.extCheckedAt = now;
    const hit = best.get(t.mint);
    if (hit) applyExt(t, hit.p, now);
  }
}

async function pollDex() {
  if (dexRunning || Date.now() < dexBackoffUntil || !solPrice.usd) return;
  dexRunning = true;
  try {
    const now = Date.now();
    const cands = [];
    for (const t of tokens.values()) {
      if (t.subscribed && streamOn()) continue; // trades already streamed
      if (now - t.createdAt > DEX_MAX_AGE_MS) continue;
      cands.push(t);
    }
    // least recently checked first, newest coins first among equals
    cands.sort((a, b) => a.extCheckedAt - b.extCheckedAt || b.createdAt - a.createdAt);
    const picked = cands.slice(0, DEX_BATCH * DEX_BATCHES_PER_CYCLE);
    const batches = [];
    for (let i = 0; i < picked.length; i += DEX_BATCH) batches.push(picked.slice(i, i + DEX_BATCH));
    for (let i = 0; i < batches.length; i += 3) await Promise.all(batches.slice(i, i + 3).map(dexBatch));
  } finally {
    dexRunning = false;
  }
}

// ---------------------------------------------------------------- housekeeping
function housekeeping(force = false) {
  const now = Date.now();
  const watched = new Set();
  for (const c of clients) for (const m of c.coins) watched.add(m);
  const unsub = [];
  for (const t of [...tokens.values()]) {
    if (now - t.createdAt > TOKEN_TTL_MS) {
      if (t.subscribed) unsub.push(t.mint);
      removeToken(t);
      continue;
    }
    if (t.subscribed) {
      // stream trades only for coins that are watched, moving, or young and still trading
      const idle = now - t.lastTradeAt;
      const age = now - t.createdAt;
      const st = t.sum?.status;
      const keep = watched.has(t.mint) || st === 'moving' || st === 'about_to_graduate' || (idle < IDLE_UNSUB_MS && age < ACTIVE_MAX_AGE_MS);
      if (!keep) {
        t.subscribed = false;
        if (idle >= IDLE_UNSUB_MS && streamOn()) t.idleVerified = true; // we watched it live and it was quiet
        unsub.push(t.mint);
      }
    } else if (watched.has(t.mint)) {
      ensureSubscribed(t);
    }
  }
  for (let i = 0; i < unsub.length; i += 100) sendUp({ method: 'unsubscribeTokenTrade', keys: unsub.slice(i, i + 100) });
  if (force || tokens.size > MAX_TOKENS) {
    const oldest = [...tokens.values()].sort((a, b) => a.createdAt - b.createdAt);
    for (const t of oldest) {
      if (tokens.size <= MAX_TOKENS * 0.9) break;
      removeToken(t);
    }
  }
}

function removeToken(t) {
  tokens.delete(t.mint);
  dirty.delete(t.mint);
  const set = byCreator.get(t.creator);
  if (set) {
    set.delete(t.mint);
    if (!set.size) byCreator.delete(t.creator);
  }
}

setInterval(() => housekeeping(false), 20_000);

// ---------------------------------------------------------------- push to browsers
function safeSend(c, obj) {
  const ws = c.ws;
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > 5_000_000) {
    ws.terminate(); // too slow to keep up
    return;
  }
  ws.send(JSON.stringify(obj));
}

function broadcast(obj) {
  for (const c of clients) safeSend(c, obj);
}

function broadcastFeed(obj) {
  for (const c of clients) if (c.feed) safeSend(c, obj);
}

let lastHoldersPush = 0;

function flush() {
  const now = Date.now();
  const events = [];
  while (newCoins.length) {
    const t = tokens.get(newCoins.shift());
    if (!t) continue;
    t.sum = buildSummary(t, now);
    events.push({ type: 'coin_new', coin: t.sum });
    dirty.delete(t.mint);
  }
  for (const mint of dirty) {
    const t = tokens.get(mint);
    if (!t) continue;
    const next = buildSummary(t, now);
    const diff = diffSummary(t.sum, next);
    t.sum = next;
    if (diff) events.push({ type: 'coin_update', mint, fields: diff });
  }
  dirty.clear();
  if (events.length) broadcastFeed({ type: 'batch', ts: now, events });

  if (pendingTrades.size) {
    for (const c of clients) {
      for (const mint of c.coins) {
        const list = pendingTrades.get(mint);
        if (list && list.length) safeSend(c, { type: 'trades', mint, trades: list });
      }
    }
    pendingTrades.clear();
  }

  if (now - lastHoldersPush >= 1000) {
    lastHoldersPush = now;
    const wanted = new Set();
    for (const c of clients) for (const m of c.coins) wanted.add(m);
    for (const m of wanted) {
      const t = tokens.get(m);
      if (!t) continue;
      const msg = { type: 'holders_update', mint: m, holders: holdersList(t, 20) };
      for (const c of clients) if (c.coins.has(m)) safeSend(c, msg);
    }
  }
}
setInterval(flush, FLUSH_MS);

// time-based fields (age categories, rolling windows) for coins nobody traded recently
setInterval(() => {
  const now = Date.now();
  const events = [];
  for (const t of tokens.values()) {
    const next = buildSummary(t, now);
    const diff = diffSummary(t.sum, next);
    t.sum = next;
    if (diff) events.push({ type: 'coin_update', mint: t.mint, fields: diff });
  }
  if (events.length) broadcastFeed({ type: 'batch', ts: now, events });
}, FULL_REFRESH_MS);

setInterval(() => {
  stats.eventsPerSec = round(stats.events / 5, 1);
  stats.events = 0;
  broadcast({ type: 'system_status', ...statusObject() });
}, 5000);

function statusObject() {
  return {
    upstream: stats.upstream,
    tokensTracked: tokens.size,
    eventsPerSec: stats.eventsPerSec,
    clients: clients.size,
    uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
    solPriceStatus: solPrice.status,
    solPriceSource: solPrice.source,
    tradeStream: tradeStreamState(),
    tradesToday: stats.tradesToday,
    dailyTradeCap: DAILY_TRADE_CAP,
    streamedCoins: [...tokens.values()].filter((t) => t.subscribed).length,
    dexLastOkAt: stats.dexLastAt || null,
    onchain: onchainEnabled() ? (onchainCapped() ? 'capped' : 'on') : 'no_key',
    onchainCallsToday: chainStats.callsToday,
    onchainDailyCap: dailyCap,
    onchainErrors: chainStats.errors,
    serverTime: Date.now(),
  };
}

// ---------------------------------------------------------------- data for endpoints
function holdersList(t, limit, offset = 0) {
  const price = t.mcap / TOTAL_SUPPLY;
  const rows = [];
  for (const [addr, w] of t.wallets) {
    if (w.bal <= 0) continue;
    const avgEntry = w.bTok > 0 ? w.bSol / w.bTok : null;
    rows.push({
      wallet: addr,
      balance: round(w.bal, 2),
      pctSupply: round((w.bal / TOTAL_SUPPLY) * 100, 3),
      valueSol: round(w.bal * price, 4),
      avgEntrySol: avgEntry,
      realizedPnlSol: avgEntry !== null ? round(w.sSol - avgEntry * w.sTok, 4) : null,
      unrealizedPnlSol: avgEntry !== null ? round(w.bal * price - avgEntry * w.bal, 4) : null,
      status: w.sTok > 0 ? 'partly_sold' : 'holding',
      tags: [addr === t.creator ? 'creator' : null, t.snipers.has(addr) ? 'sniper' : null].filter(Boolean),
      firstSeenAt: w.firstAt,
      links: { solscan: 'https://solscan.io/account/' + addr },
    });
  }
  rows.sort((a, b) => b.balance - a.balance);
  return rows.slice(offset, offset + limit).map((r, i) => ({ rank: offset + i + 1, ...r }));
}

function candles(t, tf) {
  const minutes = { '1m': 1, '5m': 5, '15m': 15, '1h': 60 }[tf];
  if (!minutes) return null;
  const perBucket = (minutes * 60_000) / SLOT_MS;
  const out = [];
  let cur = null;
  for (const s of t.slots) {
    const b = Math.floor(s.k / perBucket);
    if (!cur || cur.b !== b) {
      cur = { b, time: b * minutes * 60_000, open: s.o, high: s.h, low: s.l, close: s.c, volumeSol: 0, buys: 0, sells: 0 };
      out.push(cur);
    }
    cur.high = Math.max(cur.high, s.h);
    cur.low = Math.min(cur.low, s.l);
    cur.close = s.c;
    cur.volumeSol += s.v;
    cur.buys += s.b;
    cur.sells += s.s;
  }
  return out.map(({ b, ...c }) => ({
    ...c,
    open: round(c.open, 4),
    high: round(c.high, 4),
    low: round(c.low, 4),
    close: round(c.close, 4),
    volumeSol: round(c.volumeSol, 4),
  }));
}

const encodeCursor = (n) => Buffer.from(String(n)).toString('base64url');
const decodeCursor = (c) => {
  if (!c) return 0;
  const n = parseInt(Buffer.from(String(c), 'base64url').toString(), 10);
  return Number.isFinite(n) && n >= 0 ? n : 0;
};
const bool = (v) => v === 'true' || v === '1';

function listCoins(q) {
  const now = Date.now();
  const f = (k) => num(q.get(k));
  const minMc = f('minMarketCapSol'), maxMc = f('maxMarketCapSol');
  const minAge = f('minAgeSec'), maxAge = f('maxAgeSec');
  const minHolders = f('minHolders'), maxTop10 = f('maxTop10Pct');
  const minVol5 = f('minVolume5mSol'), maxDev = f('maxDevHoldingPct'), maxSniper = f('maxSniperPct');
  const minProg = f('minBondingProgress'), maxProg = f('maxBondingProgress');
  const status = q.get('status'), category = q.get('category');
  const text = (q.get('q') || '').trim().toLowerCase();
  const creator = q.get('creator');

  let arr = [];
  for (const t of tokens.values()) {
    const s = t.sum;
    if (!s) continue;
    const age = (now - s.createdAt) / 1000;
    if (status && s.status !== status) continue;
    if (category && !s.categories.includes(category)) continue;
    if (creator && s.creator !== creator) continue;
    if (minMc !== null && s.marketCapSol < minMc) continue;
    if (maxMc !== null && s.marketCapSol > maxMc) continue;
    if (minAge !== null && age < minAge) continue;
    if (maxAge !== null && age > maxAge) continue;
    if (minHolders !== null && s.holders < minHolders) continue;
    if (maxTop10 !== null && s.top10Pct > maxTop10) continue;
    if (minVol5 !== null && s.volumeSol.m5 < minVol5) continue;
    if (maxDev !== null && s.devHoldingPct > maxDev) continue;
    if (maxSniper !== null && s.sniperPct > maxSniper) continue;
    if (minProg !== null && s.bondingProgress < minProg) continue;
    if (maxProg !== null && s.bondingProgress > maxProg) continue;
    if (bool(q.get('hasTwitter')) && !s.socials.twitter) continue;
    if (bool(q.get('hasTelegram')) && !s.socials.telegram) continue;
    if (bool(q.get('hasWebsite')) && !s.socials.website) continue;
    if (text && !(s.name.toLowerCase().includes(text) || s.symbol.toLowerCase().includes(text) || s.mint.toLowerCase().startsWith(text) || (s.creator || '').toLowerCase().startsWith(text))) continue;
    arr.push(s);
  }
  const sorts = {
    createdAt: (s) => s.createdAt,
    marketCap: (s) => s.marketCapSol,
    volume5m: (s) => s.volumeSol.m5,
    volume1h: (s) => s.volumeSol.h1,
    change5m: (s) => s.changePct.m5,
    holders: (s) => s.holders,
    lastTrade: (s) => s.lastTradeAt,
    progress: (s) => s.bondingProgress,
  };
  const key = sorts[q.get('sort')] || sorts.createdAt;
  const dir = q.get('order') === 'asc' ? 1 : -1;
  arr.sort((a, b) => (key(a) - key(b)) * dir);
  const limit = clamp(parseInt(q.get('limit'), 10) || 50, 1, 200);
  const offset = decodeCursor(q.get('cursor'));
  const items = arr.slice(offset, offset + limit);
  return { items, nextCursor: offset + limit < arr.length ? encodeCursor(offset + limit) : null, total: arr.length };
}

// ---------------------------------------------------------------- HTTP server
function corsHeaders(req) {
  const origin = req.headers.origin;
  let allow = null;
  if (ALLOWED_ORIGINS.includes('*')) allow = '*';
  else if (origin && ALLOWED_ORIGINS.includes(origin)) allow = origin;
  const h = { 'Access-Control-Allow-Methods': 'GET,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', Vary: 'Origin' };
  if (allow) h['Access-Control-Allow-Origin'] = allow;
  return h;
}

function sendJson(req, res, code, body) {
  res.writeHead(code, { 'Content-Type': 'application/json', ...corsHeaders(req) });
  res.end(JSON.stringify(body));
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders(req));
    return res.end();
  }
  if (req.method !== 'GET') return sendJson(req, res, 405, { error: { code: 'method_not_allowed', message: 'Only GET is supported' } });
  let u;
  try {
    u = new URL(req.url, 'http://localhost');
  } catch {
    return sendJson(req, res, 400, { error: { code: 'bad_url', message: 'Bad URL' } });
  }
  const parts = u.pathname.split('/').filter(Boolean);
  const q = u.searchParams;
  const notFound = (what) => sendJson(req, res, 404, { error: { code: 'not_found', message: what + ' not found' } });

  try {
    if (!parts.length) return sendJson(req, res, 200, { name: 'memescope-backend', phase: 2, docs: 'See docs/api-contract.md', health: '/health' });
    if (parts[0] === 'health') return sendJson(req, res, 200, { status: 'ok', ...statusObject() });
    if (parts[0] === 'sol-price') return sendJson(req, res, 200, solPrice);

    if (parts[0] === 'coins' && parts.length === 1) return sendJson(req, res, 200, listCoins(q));

    if (parts[0] === 'coins' && parts.length >= 2) {
      const t = tokens.get(parts[1]);
      if (!t) return notFound('Coin');
      if (parts.length === 2) {
        queueChain(t, true);
        pumpChain();
        const others = [...(byCreator.get(t.creator) || [])].filter((m) => m !== t.mint).map((m) => tokens.get(m)?.sum).filter(Boolean);
        return sendJson(req, res, 200, { ...t.sum, description: t.description, uri: t.uri, creatorOtherCoins: others.slice(0, 50) });
      }
      if (parts[2] === 'trades') {
        const limit = clamp(parseInt(q.get('limit'), 10) || 50, 1, TRADE_KEEP);
        const all = [...t.trades].reverse();
        const offset = decodeCursor(q.get('cursor'));
        return sendJson(req, res, 200, { items: all.slice(offset, offset + limit), nextCursor: offset + limit < all.length ? encodeCursor(offset + limit) : null });
      }
      if (parts[2] === 'holders') {
        const limit = clamp(parseInt(q.get('limit'), 10) || 50, 1, 100);
        const offset = decodeCursor(q.get('cursor'));
        const items = holdersList(t, limit + 1, offset);
        const hasMore = items.length > limit;
        return sendJson(req, res, 200, { items: items.slice(0, limit), nextCursor: hasMore ? encodeCursor(offset + limit) : null, totalHolders: t.holderCount });
      }
      if (parts[2] === 'candles') {
        const c = candles(t, q.get('tf') || '1m');
        if (!c) return sendJson(req, res, 400, { error: { code: 'bad_timeframe', message: 'tf must be 1m, 5m, 15m or 1h' } });
        return sendJson(req, res, 200, { tf: q.get('tf') || '1m', items: c });
      }
      return notFound('Endpoint');
    }

    if (parts[0] === 'creators' && parts.length === 2) {
      if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(parts[1])) return sendJson(req, res, 400, { error: { code: 'bad_wallet', message: 'Not a Solana address' } });
      const cursor = q.get('cursor');
      if (cursor && !/^[1-9A-HJ-NP-Za-km-z]{64,100}$/.test(cursor)) return sendJson(req, res, 400, { error: { code: 'bad_cursor', message: 'Bad cursor' } });
      creatorResponse(parts[1], cursor)
        .then((body) => sendJson(req, res, 200, body))
        .catch((e) => {
          console.error('[creators] error', e);
          sendJson(req, res, 500, { error: { code: 'internal', message: 'Something went wrong' } });
        });
      return;
    }

    if (parts[0] === 'search') {
      const text = (q.get('q') || '').trim().toLowerCase();
      if (text.length < 2) return sendJson(req, res, 200, { coins: [], creators: [] });
      const coins = [];
      for (const t of tokens.values()) {
        if (t.name.toLowerCase().includes(text) || t.symbol.toLowerCase().includes(text) || t.mint.toLowerCase().startsWith(text)) coins.push(t.sum);
        if (coins.length >= 20) break;
      }
      const creators = [...byCreator.keys()].filter((c) => c.toLowerCase().startsWith(text)).slice(0, 5).map((w) => ({ wallet: w, coinsSeen: byCreator.get(w).size }));
      return sendJson(req, res, 200, { coins, creators });
    }

    return notFound('Endpoint');
  } catch (e) {
    console.error('[http] error', e);
    return sendJson(req, res, 500, { error: { code: 'internal', message: 'Something went wrong' } });
  }
});

// ---------------------------------------------------------------- WebSocket server for the website
const wss = new WebSocketServer({
  server,
  path: '/ws',
  perMessageDeflate: true,
  verifyClient: ({ origin }) => ALLOWED_ORIGINS.includes('*') || !origin || ALLOWED_ORIGINS.includes(origin),
});

wss.on('connection', (ws) => {
  const c = { ws, feed: true, coins: new Set(), alive: true };
  clients.add(c);
  ws.on('pong', () => (c.alive = true));
  ws.on('close', () => clients.delete(c));
  ws.on('error', () => clients.delete(c));

  safeSend(c, { type: 'system_status', ...statusObject() });
  safeSend(c, { type: 'sol_price', ...solPrice });
  const recent = [...tokens.values()].filter((t) => t.sum).sort((a, b) => b.lastTradeAt - a.lastTradeAt).slice(0, 1000).map((t) => t.sum);
  safeSend(c, { type: 'snapshot', channel: 'feed', coins: recent });

  ws.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (m.type === 'ping') return safeSend(c, { type: 'pong', ts: Date.now() });
    if (m.type === 'subscribe') {
      if (m.channel === 'feed') c.feed = true;
      if (m.channel === 'coin' && typeof m.mint === 'string') {
        if (c.coins.size >= 20) return safeSend(c, { type: 'error', message: 'Max 20 coin subscriptions' });
        const t = tokens.get(m.mint);
        if (!t) return safeSend(c, { type: 'error', message: 'Unknown coin', mint: m.mint });
        c.coins.add(m.mint);
        ensureSubscribed(t); // coins a visitor is watching get live trades
        queueChain(t, true);
        pumpChain();
        safeSend(c, { type: 'coin_snapshot', coin: t.sum, trades: [...t.trades].reverse().slice(0, 50), holders: holdersList(t, 20) });
      }
    } else if (m.type === 'unsubscribe') {
      if (m.channel === 'feed') c.feed = false;
      if (m.channel === 'coin') c.coins.delete(m.mint);
    }
  });
});

setInterval(() => {
  for (const c of clients) {
    if (!c.alive) {
      c.ws.terminate();
      clients.delete(c);
      continue;
    }
    c.alive = false;
    try {
      c.ws.ping();
    } catch {}
  }
}, 30_000);

// ---------------------------------------------------------------- saved state (history survives restarts if DATA_DIR is a persistent disk)
const r4 = (x) => (Number.isFinite(x) ? Number(x.toFixed(4)) : 0);

function stateJson() {
  const list = [...tokens.values()]
    .sort((a, b) => b.lastTradeAt - a.lastTradeAt)
    .slice(0, SAVE_MAX_TOKENS)
    .map((t) => ({
      mint: t.mint, name: t.name, symbol: t.symbol, uri: t.uri, creator: t.creator, createdAt: t.createdAt,
      image: t.image, description: t.description, twitter: t.twitter, telegram: t.telegram, website: t.website,
      metaLive: t.meta.status === 'live', mcap: t.mcap, initialMcap: t.initialMcap, ath: t.ath, vTokens: t.vTokens, vSol: t.vSol,
      lastTradeAt: t.lastTradeAt, tradeCount: t.tradeCount, creatorMax: t.creatorMax, gradExt: t.gradExt, idleVerified: t.idleVerified,
      chain: t.chain.status === 'done' ? t.chain : undefined,
      slots: t.slots.slice(-SAVE_MAX_SLOTS).map((x) => [x.k, r4(x.o), r4(x.h), r4(x.l), r4(x.c), x.b, x.s, r4(x.v)]),
    }));
  return JSON.stringify({ v: 1, savedAt: Date.now(), day: stats.day, tradesToday: stats.tradesToday, onchainCallsToday: chainStats.callsToday, solPrice, tokens: list });
}

let saving = false;
async function saveState() {
  if (saving) return;
  saving = true;
  try {
    const data = stateJson();
    await fs.promises.mkdir(DATA_DIR, { recursive: true });
    await fs.promises.writeFile(STATE_FILE + '.tmp', data);
    await fs.promises.rename(STATE_FILE + '.tmp', STATE_FILE);
  } catch (e) {
    console.warn('[state] save failed:', e.message);
  } finally {
    saving = false;
  }
}

function saveStateSync() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(STATE_FILE + '.tmp', stateJson());
    fs.renameSync(STATE_FILE + '.tmp', STATE_FILE);
  } catch (e) {
    console.warn('[state] save failed:', e.message);
  }
}

function loadState() {
  let j;
  try {
    j = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    console.log('[state] no saved state found, starting empty (' + STATE_FILE + ')');
    return;
  }
  if (!j || j.v !== 1 || !Array.isArray(j.tokens)) return;
  const now = Date.now();
  if (j.day === stats.day && Number.isFinite(j.tradesToday)) {
    stats.tradesToday = j.tradesToday; // so a restart does not reset the daily cost cap
    if (Number.isFinite(j.onchainCallsToday)) chainStats.callsToday = j.onchainCallsToday;
    if (stats.tradesToday >= DAILY_TRADE_CAP) stats.capped = true;
  }
  if (j.solPrice && j.solPrice.usd) solPrice = { ...j.solPrice, status: 'stale', reason: 'Restored from last run, refreshing' };
  let n = 0;
  for (const o of j.tokens) {
    if (!o || !o.mint || tokens.has(o.mint) || now - o.createdAt > TOKEN_TTL_MS) continue;
    o.slots = (o.slots || []).map((a) => ({ k: a[0], o: a[1], h: a[2], l: a[3], c: a[4], b: a[5], s: a[6], v: a[7] }));
    const t = makeToken(o, now);
    registerToken(t);
    t.sum = buildSummary(t, now);
    if (t.uri && !o.metaLive) enqueueMeta(t);
    if (streamWanted() && now - t.lastTradeAt < IDLE_UNSUB_MS && now - t.createdAt < ACTIVE_MAX_AGE_MS) ensureSubscribed(t);
    n++;
  }
  console.log('[state] restored ' + n + ' coins from ' + new Date(j.savedAt).toISOString() + ' (holders and trade lists start empty after a restart)');
}

// ---------------------------------------------------------------- start
loadState();
server.listen(PORT, () => console.log('[server] listening on port', PORT));
console.log('[config] trade stream: ' + tradeStreamState() + ', daily cap ' + DAILY_TRADE_CAP + ', key ' + (API_KEY ? 'set' : 'NOT set') + ', state file ' + STATE_FILE);
connectUpstream();
pollSolPrice();
setInterval(pollSolPrice, 30_000);
setTimeout(() => {
  pollDex();
  setInterval(pollDex, DEX_POLL_MS);
}, 5000);
setInterval(saveState, SAVE_MS);

function shutdown() {
  console.log('[server] shutting down');
  saveStateSync();
  try {
    up && up.close();
  } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
