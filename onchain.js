// On-chain analysis via Helius RPC (needs HELIUS_API_KEY).
// - Bundled wallets: % of supply bought by other wallets in the same slot as the Pump.fun create transaction.
// - Creator history: Pump.fun create transactions signed by a wallet, verified from transaction logs.
// Every result is derived from real transactions; nothing is guessed. A daily call cap protects the Helius plan.

const KEY = process.env.HELIUS_API_KEY || '';
const RPC_URL = process.env.HELIUS_RPC_URL || (KEY ? `https://mainnet.helius-rpc.com/?api-key=${KEY}` : '');
const DAILY_CAP = Number(process.env.ONCHAIN_DAILY_CALLS) || 20_000;
const CONCURRENCY = Number(process.env.ONCHAIN_CONCURRENCY) || 3;
export const PUMP_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const TOTAL_SUPPLY = 1_000_000_000;

export const chainStats = { callsToday: 0, day: new Date().toISOString().slice(0, 10), capped: false, errors: 0, lastOkAt: 0 };
export const onchainEnabled = () => Boolean(RPC_URL);
export const onchainCapped = () => chainStats.capped;
export const dailyCap = DAILY_CAP;

export class CapError extends Error {}

function rollDay() {
  const d = new Date().toISOString().slice(0, 10);
  if (d !== chainStats.day) {
    chainStats.day = d;
    chainStats.callsToday = 0;
    chainStats.capped = false;
  }
}

// simple concurrency limiter
let active = 0;
const waiting = [];
async function slot(fn) {
  if (active >= CONCURRENCY) await new Promise((r) => waiting.push(r));
  active++;
  try {
    return await fn();
  } finally {
    active--;
    waiting.shift()?.();
  }
}

export async function rpc(method, params, ms = 10_000) {
  if (!RPC_URL) throw new Error('HELIUS_API_KEY is not set');
  rollDay();
  if (chainStats.callsToday >= DAILY_CAP) {
    chainStats.capped = true;
    throw new CapError('Daily on-chain analysis limit reached');
  }
  chainStats.callsToday++;
  return slot(async () => {
    for (let attempt = 0; attempt < 3; attempt++) {
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), ms);
      try {
        const res = await fetch(RPC_URL, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
          signal: ctl.signal,
        });
        if (res.status === 429) {
          await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
          continue;
        }
        if (!res.ok) throw new Error('RPC HTTP ' + res.status);
        const j = await res.json();
        if (j.error) throw new Error('RPC ' + (j.error.message || 'error'));
        chainStats.lastOkAt = Date.now();
        return j.result;
      } catch (e) {
        if (attempt === 2) {
          chainStats.errors++;
          throw e;
        }
      } finally {
        clearTimeout(timer);
      }
    }
    chainStats.errors++;
    throw new Error('RPC rate limited');
  });
}

const getTx = (sig) => rpc('getTransaction', [sig, { encoding: 'jsonParsed', maxSupportedTransactionVersion: 1, commitment: 'confirmed' }]);
const sigsFor = (addr, opts) => rpc('getSignaturesForAddress', [addr, { commitment: 'confirmed', ...opts }]);

/** True when the Pump.fun program itself logged a Create / CreateV2 instruction (tracks the invoke stack). */
export function isPumpCreate(tx) {
  if (!tx || tx.meta?.err) return false;
  const stack = [];
  for (const line of tx.meta?.logMessages || []) {
    let m = line.match(/^Program (\w+) invoke \[\d+\]$/);
    if (m) { stack.push(m[1]); continue; }
    m = line.match(/^Program (\w+) (success|failed)/);
    if (m) { stack.pop(); continue; }
    if (/^Program log: Instruction: Create(V2)?$/.test(line) && stack[stack.length - 1] === PUMP_PROGRAM) return true;
  }
  return false;
}

const signersOf = (tx) => new Set((tx.transaction?.message?.accountKeys || []).filter((k) => k.signer).map((k) => (typeof k === 'string' ? k : k.pubkey)));
const feePayerOf = (tx) => {
  const k = tx.transaction?.message?.accountKeys?.[0];
  return typeof k === 'string' ? k : k?.pubkey;
};

/** Token amount gained per owner for one mint in a transaction. */
export function gainsByOwner(tx, mint) {
  const pre = new Map(), post = new Map();
  for (const b of tx.meta?.preTokenBalances || []) if (b.mint === mint && b.owner) pre.set(b.owner, (pre.get(b.owner) || 0) + Number(b.uiTokenAmount?.uiAmount || 0));
  for (const b of tx.meta?.postTokenBalances || []) if (b.mint === mint && b.owner) post.set(b.owner, (post.get(b.owner) || 0) + Number(b.uiTokenAmount?.uiAmount || 0));
  const out = new Map();
  for (const [o, v] of post) {
    const d = v - (pre.get(o) || 0);
    if (d > 0) out.set(o, d);
  }
  return out;
}

/** The mint created in a Pump.fun create transaction (the mint that appears with ~full supply and no prior balance). */
export function createdMint(tx) {
  const preMints = new Set((tx.meta?.preTokenBalances || []).map((b) => b.mint));
  const totals = new Map();
  for (const b of tx.meta?.postTokenBalances || []) if (!preMints.has(b.mint)) totals.set(b.mint, (totals.get(b.mint) || 0) + Number(b.uiTokenAmount?.uiAmount || 0));
  for (const [mint, v] of totals) if (v >= TOTAL_SUPPLY * 0.5) return mint;
  return null;
}

/**
 * Bundled wallets for a coin: other wallets (signers, not the creator) that bought in the same slot as the create transaction.
 * Returns { bundledPct, bundleWallets, launchSlot, createSignature, slotTxs }.
 */
export async function analyzeLaunch(mint, creator) {
  let all = [];
  let before;
  for (let page = 0; page < 3; page++) {
    const batch = await sigsFor(mint, { limit: 1000, before });
    all = all.concat(batch || []);
    if (!batch || batch.length < 1000) break;
    before = batch[batch.length - 1].signature;
    if (page === 2) throw new Error('Too many transactions to locate the launch');
  }
  if (!all.length) throw new Error('No transactions found yet');
  const oldest = all[all.length - 1];
  const createTx = await getTx(oldest.signature);
  if (!isPumpCreate(createTx) || createdMint(createTx) !== mint) throw new Error('Launch transaction could not be verified');
  const launchSlot = oldest.slot;
  const sameSlot = all.filter((s) => s.slot === launchSlot && !s.err).slice(0, 40);
  const wallets = new Map();
  for (const s of sameSlot) {
    const tx = s.signature === oldest.signature ? createTx : await getTx(s.signature);
    if (!tx || tx.meta?.err) continue;
    const signers = signersOf(tx);
    for (const [owner, gain] of gainsByOwner(tx, mint)) {
      if (owner === creator || !signers.has(owner)) continue; // the bonding curve is not a signer, the creator is reported separately
      wallets.set(owner, (wallets.get(owner) || 0) + gain);
    }
  }
  let tokens = 0;
  for (const v of wallets.values()) tokens += v;
  return {
    bundledPct: Number(((tokens / TOTAL_SUPPLY) * 100).toFixed(2)),
    bundleWallets: wallets.size,
    launchSlot,
    createSignature: oldest.signature,
    slotTxs: sameSlot.length,
  };
}

/** Pump.fun launches signed by a wallet, from its recent transactions. Paginate with the returned cursor. */
export async function creatorLaunches(wallet, cursor, pageSize = 50) {
  const sigs = (await sigsFor(wallet, { limit: pageSize, before: cursor || undefined })) || [];
  const launches = [];
  for (const s of sigs) {
    if (s.err) continue;
    const tx = await getTx(s.signature);
    if (!tx || feePayerOf(tx) !== wallet || !isPumpCreate(tx)) continue;
    const mint = createdMint(tx);
    if (mint) launches.push({ mint, signature: s.signature, slot: s.slot, time: (s.blockTime || tx.blockTime || 0) * 1000 });
  }
  return { launches, scanned: sigs.length, nextCursor: sigs.length === pageSize ? sigs[sigs.length - 1].signature : null };
}
