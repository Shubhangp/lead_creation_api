'use strict';

const LENDER_RATE_LIMITS = {
    SML: 100,
    FREO: 120,
    OVLY: 160,
    LendingPlate: 150,
    ZYPE: 120,
    FINTIFI: 150,
    FATAKPAY: 120,
    FATAKPAYPL: 150,
    RAMFINCROP: 150,
    MyMoneyMantra: 100,
    MPOKKET: 150,
    INDIALENDS: 150,
    CRMPaisa: 150,
    CreditPluse: 150,
    CreditSea: 150,
    CreditLinks: 200,
    CreditLinksGold: 250,
    kamakshimoney: 75,
    rulemudraoffer: 60,
    CreditHaat: 150,
};

const DEFAULT_RATE_LIMIT = 130;
const DEFAULT_LIVE_SHARE = 60;            // percent
const SETTINGS_KEY = 'lenderRateLimits';
const REFRESH_MS = 60 * 1000;

// Cached dashboard settings: { limits: { OVLY: 250, ... }, liveSharePercent: 60, updatedAt, updatedBy }
let settings = { limits: {}, liveSharePercent: DEFAULT_LIVE_SHARE, updatedAt: null, updatedBy: null };
const listeners = new Set();

function envLimit(lender) {
    const v = process.env[`RATE_LIMIT_${String(lender).toUpperCase()}`];
    if (v === undefined || v === '') return null;
    const n = parseInt(v, 10);
    return Number.isNaN(n) || n <= 0 ? null : n;
}

/** Total leads/min for a lender (live + bulk). */
function getRateLimit(lender) {
    const fromDashboard = settings.limits[lender];
    if (Number.isFinite(fromDashboard) && fromDashboard > 0) return fromDashboard;
    return envLimit(lender) || LENDER_RATE_LIMITS[lender] || DEFAULT_RATE_LIMIT;
}

function rateLimitSource(lender) {
    if (Number.isFinite(settings.limits[lender]) && settings.limits[lender] > 0) return 'dashboard';
    if (envLimit(lender)) return 'env';
    return LENDER_RATE_LIMITS[lender] ? 'default' : 'fallback';
}

function getLiveSharePercent() {
    const p = Number(settings.liveSharePercent);
    return Number.isFinite(p) && p >= 1 && p <= 99 ? p : DEFAULT_LIVE_SHARE;
}

/**
 * Leads/min a lender's pool may use. pool: 'live' (main API flow) or 'bulk'
 * (distribution + lead upload). Each pool gets at least 1/min.
 */
function getPoolRate(lender, pool) {
    const total = getRateLimit(lender);
    const live = Math.max(1, Math.round((total * getLiveSharePercent()) / 100));
    if (pool === 'live') return live;
    return Math.max(1, total - live);
}

function onRateLimitsChanged(fn) { listeners.add(fn); return () => listeners.delete(fn); }

function applySettings(next) {
    const before = JSON.stringify(settings);
    settings = {
        limits: { ...(next && next.limits) },
        liveSharePercent: next && next.liveSharePercent !== undefined ? next.liveSharePercent : DEFAULT_LIVE_SHARE,
        updatedAt: (next && next.updatedAt) || null,
        updatedBy: (next && next.updatedBy) || null,
    };
    if (JSON.stringify(settings) !== before) {
        listeners.forEach((fn) => { try { fn(); } catch (e) { console.error('[RateLimits] listener failed:', e.message); } });
    }
}

// Lazy-required so this file stays usable by scripts/tests without DynamoDB.
let AppSettings = null;
function store() {
    if (!AppSettings) AppSettings = require('../models/appSettingsModel');
    return AppSettings;
}

async function loadRateLimitSettings() {
    try {
        const v = await store().get(SETTINGS_KEY);
        if (v) applySettings(v);
    } catch (e) {
        console.error('[RateLimits] could not load dashboard settings (using defaults/env):', e.message);
    }
    return settings;
}

/**
 * Save from the dashboard. limits: { LENDER: number|null } — null/'' removes the
 * dashboard value so env/default applies again. liveSharePercent: 1–99.
 */
async function saveRateLimitSettings({ limits = {}, liveSharePercent }, updatedBy = null) {
    const clean = { ...settings.limits };
    for (const [lender, raw] of Object.entries(limits || {})) {
        if (raw === null || raw === '' || raw === undefined) { delete clean[lender]; continue; }
        const n = Number(raw);
        if (!Number.isInteger(n) || n < 1 || n > 100000) {
            throw Object.assign(new Error(`${lender}: limit must be a whole number between 1 and 100000`), { status: 400 });
        }
        clean[lender] = n;
    }
    let share = settings.liveSharePercent;
    if (liveSharePercent !== undefined && liveSharePercent !== null && liveSharePercent !== '') {
        share = Number(liveSharePercent);
        if (!Number.isFinite(share) || share < 1 || share > 99) {
            throw Object.assign(new Error('Main-flow share must be between 1 and 99 %'), { status: 400 });
        }
    }
    const value = { limits: clean, liveSharePercent: share, updatedAt: new Date().toISOString(), updatedBy };
    await store().set(SETTINGS_KEY, value);
    applySettings(value);
    return settings;
}

/** Every known lender with its effective numbers (for the dashboard). */
function describeRateLimits(extraLenders = []) {
    const names = new Set([...Object.keys(LENDER_RATE_LIMITS), ...Object.keys(settings.limits), ...extraLenders]);
    return {
        liveSharePercent: getLiveSharePercent(),
        defaultLiveSharePercent: DEFAULT_LIVE_SHARE,
        updatedAt: settings.updatedAt,
        updatedBy: settings.updatedBy,
        lenders: [...names].sort((a, b) => a.localeCompare(b)).map((lender) => ({
            lender,
            ratePerMinute: getRateLimit(lender),
            source: rateLimitSource(lender),
            codeDefault: LENDER_RATE_LIMITS[lender] || DEFAULT_RATE_LIMIT,
            envValue: envLimit(lender),
            live: getPoolRate(lender, 'live'),
            bulk: getPoolRate(lender, 'bulk'),
        })),
    };
}

// Keep in sync with dashboard edits made from another process / instance.
let refreshTimer = null;
function startRateLimitRefresh() {
    if (refreshTimer) return;
    loadRateLimitSettings();
    refreshTimer = setInterval(loadRateLimitSettings, REFRESH_MS);
    if (refreshTimer.unref) refreshTimer.unref();
}

module.exports = {
    LENDER_RATE_LIMITS, DEFAULT_RATE_LIMIT, DEFAULT_LIVE_SHARE,
    getRateLimit, getPoolRate, getLiveSharePercent,
    loadRateLimitSettings, saveRateLimitSettings, describeRateLimits,
    onRateLimitsChanged, startRateLimitRefresh,
};
