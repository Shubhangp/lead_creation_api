'use strict';

const express = require('express');
const multer = require('multer');
const ExcelJS = require('exceljs');
const XLSX = require('xlsx');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const ProcessLead = require('../models/processLeadModel');
const PushJob = require('../models/pushJobModel');
const PushJobItem = require('../models/pushJobItemModel');
const { categorizeLenderResult } = require('../config/lenderResultCategorizer');
const Lead = require('../models/leadModel');
const { getRateLimit } = require('../config/lenderRateLimits');
const { acquireLenderSlots } = require('../utils/lenderRateLimiter');

const {
    sendToSML,
    sendToFreo,
    sendToZYPE,
    sendToLendingPlate,
    sendToFINTIFI,
    sendToFATAKPAY,
    sendToFATAKPAYPL,
    sendToOVLY,
    sendToRAMFINCROP,
    sendToMyMoneyMantra,
    sendToMpokket,
    sendToIndiaLends,
    sendToCrmPaisa,
    sendToCreditPulse,
    sendToCreditSea,
    sendToCreditLinks,
    sendToCreditLinksGold,
    sendToKamakshiMoney,
    sendToCreditHaat,
} = require('../services/lenderService');

// ============================================================================
// MULTER
// ============================================================================

const upload = multer({
    storage: multer.diskStorage({
        destination: os.tmpdir(),
        filename: (_req, file, cb) => {
            const u = `process-lead-${Date.now()}-${Math.random().toString(36).slice(2)}`;
            cb(null, `${u}${path.extname(file.originalname)}`);
        },
    }),
    limits: { fileSize: 200 * 1024 * 1024 },
    fileFilter: (_req, file, cb) => {
        const ok =
            file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
            file.mimetype === 'application/vnd.ms-excel' ||
            file.originalname.match(/\.(xlsx|xls)$/i);
        ok ? cb(null, true) : cb(new Error('Only .xlsx and .xls files are allowed'), false);
    },
});

// ============================================================================
// COLUMN MAP
// ============================================================================

const HEADER_MAP = {
    fullname: 'fullName', full_name: 'fullName', name: 'fullName', 'full name': 'fullName',
    firstname: 'firstName', first_name: 'firstName', 'first name': 'firstName',
    lastname: 'lastName', last_name: 'lastName', 'last name': 'lastName',
    phone: 'phone', mobile: 'phone', contact: 'phone', phonenumber: 'phone',
    phone_number: 'phone', 'phone number': 'phone', 'mobile number': 'phone',
    email: 'email', 'email address': 'email', emailaddress: 'email',
    source: 'source', leadsource: 'source', lead_source: 'source', 'lead source': 'source',
    pannumber: 'panNumber', pan_number: 'panNumber', pan: 'panNumber',
    'pan number': 'panNumber', 'pan no': 'panNumber',
    age: 'age',
    dateofbirth: 'dateOfBirth', date_of_birth: 'dateOfBirth', dob: 'dateOfBirth',
    'date of birth': 'dateOfBirth', birthdate: 'dateOfBirth',
    gender: 'gender', sex: 'gender',
    jobtype: 'jobType', job_type: 'jobType', 'job type': 'jobType',
    occupation: 'jobType', employment: 'jobType',
    businesstype: 'businessType', business_type: 'businessType',
    'business type': 'businessType', business: 'businessType',
    salary: 'salary', income: 'salary', 'monthly salary': 'salary', 'monthly income': 'salary',
    creditscore: 'creditScore', credit_score: 'creditScore', 'credit score': 'creditScore',
    cibilscore: 'cibilScore', cibil_score: 'cibilScore', cibil: 'cibilScore', 'cibil score': 'cibilScore',
    address: 'address', 'full address': 'address',
    pincode: 'pincode', pin: 'pincode', 'pin code': 'pincode',
    'zip code': 'pincode', zipcode: 'pincode',
    consent: 'consent',
    createdat: 'createdAt', created_at: 'createdAt', 'created at': 'createdAt',
    'created date': 'createdAt', createddate: 'createdAt', date: 'createdAt',
    timestamp: 'createdAt', 'entry date': 'createdAt', entrydate: 'createdAt',
    // Written by admin/lead_upload step 1 when the row matched a lead we
    // already hold. Carries that lead's id so the push step reuses it instead
    // of creating a duplicate row in the leads table.
    existingleadid: 'existingLeadId', existing_lead_id: 'existingLeadId',
    'existing lead id': 'existingLeadId',
};

// ============================================================================
// HELPERS
// ============================================================================

function normaliseHeader(raw) {
    return String(raw ?? '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function parseBoolean(v) {
    if (typeof v === 'boolean') return v;
    if (typeof v === 'number') return v !== 0;
    const s = String(v).trim().toLowerCase();
    return s === 'true' || s === '1' || s === 'yes' || s === 'y';
}

function parseDateOnly(v) {
    if (!v) return null;
    if (v instanceof Date) return isNaN(v) ? null : v.toISOString().split('T')[0];
    const s = String(v).trim();
    const d = new Date(s);
    return isNaN(d) ? null : d.toISOString().split('T')[0];
}

function parseDateTimeISO(v) {
    if (!v) return null;
    if (v instanceof Date) return isNaN(v) ? null : v.toISOString();
    const s = String(v).trim();
    const d = new Date(s);
    return isNaN(d) ? null : d.toISOString();
}

// Always produce full ISO strings — DynamoDB GSI BETWEEN is lexicographic.
// "2025-01-01" < "2025-01-01T10:30:00.000Z" so date-only strings miss every row.
function normaliseDateRange(startDate, endDate) {
    const start = startDate.includes('T') ? startDate : `${startDate}T00:00:00.000Z`;
    const end = endDate.includes('T') ? endDate : `${endDate}T23:59:59.999Z`;
    return { start, end };
}

function rowToLeadData(rawRow) {
    const lead = {};
    for (const [rawKey, cellValue] of Object.entries(rawRow)) {
        const fieldName = HEADER_MAP[normaliseHeader(rawKey)];
        if (!fieldName) continue;

        const isEmpty =
            cellValue === undefined || cellValue === null || cellValue === '' ||
            (typeof cellValue === 'object' && !(cellValue instanceof Date) && Object.keys(cellValue).length === 0);
        if (isEmpty) continue;

        if (fieldName === 'consent') {
            lead[fieldName] = parseBoolean(cellValue);
        } else if (fieldName === 'dateOfBirth') {
            lead[fieldName] = parseDateOnly(cellValue);
        } else if (fieldName === 'createdAt') {
            lead[fieldName] = parseDateTimeISO(cellValue);
        } else if (['age', 'creditScore', 'cibilScore'].includes(fieldName)) {
            const n = Number(cellValue);
            lead[fieldName] = isNaN(n) ? null : n;
        } else if (fieldName === 'salary') {
            // Always string — lender services expect string (RAMFINCROP, FINTIFI etc.)
            lead[fieldName] = cellValue != null ? String(cellValue).trim() : null;
        } else if (fieldName === 'panNumber') {
            lead[fieldName] = String(cellValue).trim().toUpperCase();
        } else {
            lead[fieldName] = String(cellValue).trim();
        }
    }
    return lead;
}

// ============================================================================
// EXCEL STREAM
// ============================================================================

async function* streamExcelChunks(filePath, chunkSize = 500) {
    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
        entries: 'emit', sharedStrings: 'cache',
        hyperlinks: 'ignore', styles: 'ignore', worksheets: 'emit',
    });

    let headers = null;
    let chunk = [];

    for await (const worksheet of workbook) {
        for await (const row of worksheet) {
            if (row.number === 1) {
                headers = {};
                row.eachCell({ includeEmpty: false }, (cell, col) => {
                    headers[col] = String(cell.value ?? '').trim();
                });
                continue;
            }
            if (!headers) continue;

            const raw = {};
            row.eachCell({ includeEmpty: false }, (cell, col) => {
                const h = headers[col];
                if (h) raw[h] = cell.value;
            });

            if (Object.keys(raw).length === 0) continue;
            chunk.push({ rowNum: row.number, raw });

            if (chunk.length >= chunkSize) { yield chunk; chunk = []; }
        }
        break;
    }
    if (chunk.length > 0) yield chunk;
}

// ============================================================================
// LENDER MAP
// ============================================================================

const LENDER_MAP = {
    SML: sendToSML,
    FREO: sendToFreo,
    OVLY: sendToOVLY,
    LendingPlate: sendToLendingPlate,
    ZYPE: sendToZYPE,
    FINTIFI: sendToFINTIFI,
    FATAKPAY: sendToFATAKPAY,
    FATAKPAYPL: sendToFATAKPAYPL,
    RAMFINCROP: sendToRAMFINCROP,
    MyMoneyMantra: sendToMyMoneyMantra,
    MPOKKET: sendToMpokket,
    INDIALENDS: sendToIndiaLends,
    CRMPaisa: sendToCrmPaisa,
    CreditPluse: sendToCreditPulse,
    CreditSea: sendToCreditSea,
    CreditLinks: sendToCreditLinks,
    CreditLinksGold: sendToCreditLinksGold,
    kamakshimoney: sendToKamakshiMoney,
    CreditHaat: sendToCreditHaat,
};

// ============================================================================
// CONCURRENCY POOL
// ============================================================================

async function runWithConcurrency(tasks, concurrency) {
    const results = new Array(tasks.length);
    let index = 0;
    async function worker() {
        while (index < tasks.length) {
            const i = index++;
            try { results[i] = await tasks[i](); }
            catch (e) { results[i] = { error: e.message }; }
        }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, tasks.length) }, worker));
    return results;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================================================
// PER-LENDER RATE LIMITER (token bucket)
// ----------------------------------------------------------------------------
// Uses the shared per-lender limiter in utils/lenderRateLimiter.js (same bucket
// as the live lead API), so all senders together stay within the lender's
// leads-per-minute limit, with sends spread evenly across the minute.
// ============================================================================

// Build one limiter per lender for a job. Persisted for the whole push so the
// rate is honoured across every page, not reset per page.
function buildLenderLimiters(lenders) {
    const limiters = {};
    lenders.forEach((l) => {
        const rpm = getRateLimit(l);
        // Shared with the live lead API, so their combined traffic stays within the cap.
        limiters[l] = { take: (n, shouldAbort) => acquireLenderSlots(l, n, shouldAbort) };
        console.log(`[Push] Rate limit for ${l}: ${rpm} leads/min`);
    });
    return limiters;
}

// ============================================================================
// LENDER DISPATCH — one independent worker per lender
// ============================================================================

const LENDER_BATCH = 50;

// Lender response → the same status category the stats dashboard uses.
function categoryOf(lender, result, error) {
    try { return categorizeLenderResult(lender, result, error) || (error ? 'error' : 'other'); }
    catch (_) { return error ? 'error' : 'other'; }
}

/**
 * Sends the job's queued leads (push_job_items) to ONE lender, from that
 * lender's own cursor, at that lender's own rate limit. Runs alongside the
 * other lenders' workers, so a slow lender never holds the fast ones back.
 * Progress (cursor + counts) is checkpointed after every batch.
 */
async function runLenderWorker(jobId, lender, job, state, limiter, stop) {
    const prev = (job.lenderResults && job.lenderResults[lender]) || {};
    const r = {
        lender,
        status: 'running',
        total: prev.total || 0,
        successCount: prev.successCount || 0,
        failCount: prev.failCount || 0,
        categories: { ...(prev.categories || {}) },
        startedAt: prev.startedAt || new Date().toISOString(),
    };
    let cursor = job.lenderCursors && job.lenderCursors[lender] !== undefined ? job.lenderCursors[lender] : -1;
    state.cursors[lender] = cursor;

    const sendFn = LENDER_MAP[lender];
    if (!sendFn) {
        r.status = 'skipped';
        r.message = 'Not configured';
        state.finished.add(lender);
        await PushJob.updateLender(jobId, lender, r).catch(() => {});
        return r;
    }

    let consecutiveErrors = 0;
    while (!stop()) {
        try {
            const items = await PushJobItem.readAfter(jobId, cursor, LENDER_BATCH);
            if (items.length === 0) {
                if (state.saveDone || state.fatal) {
                    // The save loop finished — one more read catches its last page.
                    const tail = await PushJobItem.readAfter(jobId, cursor, 1);
                    if (tail.length === 0) break;
                    continue;
                }
                await sleep(1000); // queue empty for now; the save loop is still preparing leads
                continue;
            }

            if (limiter) await limiter.take(items.length, stop); // lender's leads/min cap
            if (stop()) break;

            const settled = await Promise.allSettled(
                items.map((it) => sendFn({ ...it.lead, leadId: it.lead.leadId }))
            );
            settled.forEach((s) => {
                if (s.status === 'fulfilled') r.successCount++;
                else r.failCount++;
                const cat = categoryOf(lender, s.status === 'fulfilled' ? s.value : undefined, s.status === 'rejected' ? s.reason : null);
                r.categories[cat] = (r.categories[cat] || 0) + 1;
            });
            r.total += items.length;
            cursor = items[items.length - 1].seq;
            state.cursors[lender] = cursor;
            await PushJob.updateLender(jobId, lender, r, cursor);
            consecutiveErrors = 0;
        } catch (err) {
            consecutiveErrors++;
            console.error(`[Push ${jobId}] ${lender} worker error (${consecutiveErrors}):`, err.message);
            if (consecutiveErrors >= 20) {
                r.status = 'error';
                r.message = err.message;
                break;
            }
            await sleep(Math.min(30000, 1000 * consecutiveErrors));
        }
    }

    if (r.status === 'running') {
        r.status = isLenderStopped(jobId, lender) ? 'stopped'
            : (state.cancelled || cancelledJobIds.has(jobId)) ? 'cancelled'
            : state.fatal ? 'error'
            : 'done';
    }
    r.finishedAt = new Date().toISOString();
    state.finished.add(lender);
    await PushJob.updateLender(jobId, lender, r, cursor).catch((e) =>
        console.error(`[Push ${jobId}] ${lender} final update failed:`, e.message));
    console.log(`[Push ${jobId}] ${lender} ${r.status}: sent=${r.total} ok=${r.successCount} fail=${r.failCount}`);
    return r;
}

// ============================================================================
// SAVE ONE PAGE OF LEADS TO leads TABLE
//
// Rows carrying `existingLeadId` were matched to an existing lead by the
// admin/lead_upload dedup step. Their source2/updatedAt were already stamped
// there, so here they are only resolved to a dispatchable lead object — no new
// row is written and no counter is bumped. They are still handed to every
// selected lender alongside the freshly created ones.
// ============================================================================

const SAVE_CONCURRENCY = 20;

async function savePageToLeadsTable(leads) {
    const results = await runWithConcurrency(
        leads.map((pl) => async () => {
            try {
                const leadData = {
                    source: pl.source,
                    fullName: pl.fullName || '',
                    firstName: pl.firstName || null,
                    lastName: pl.lastName || null,
                    phone: pl.phone,
                    email: pl.email,
                    age: pl.age || null,
                    dateOfBirth: pl.dateOfBirth || null,
                    gender: pl.gender || null,
                    panNumber: pl.panNumber,
                    jobType: pl.jobType || null,
                    businessType: pl.businessType || null,
                    salary: pl.salary != null ? String(pl.salary) : null,
                    creditScore: pl.creditScore || null,
                    cibilScore: pl.cibilScore || null,
                    address: pl.address || null,
                    pincode: pl.pincode || null,
                    consent: pl.consent ?? true,
                };
                if (pl.existingLeadId) {
                    return {
                        ok: true,
                        reused: true,
                        lead: Lead.buildExistingLeadForDispatch(leadData, pl.existingLeadId),
                    };
                }

                const created = await Lead.createFast(leadData);
                return { ok: true, lead: created };
            } catch (err) {
                const isDuplicate =
                    err.code === 'DUPLICATE_PHONE' ||
                    err.code === 'DUPLICATE_PAN' ||
                    err.name === 'ConditionalCheckFailedException';
                if (!isDuplicate) console.error(`[Push] Unexpected error saving ${pl.processLeadId}:`, err.message);
                return {
                    ok: false,
                    processLeadId: pl.processLeadId,
                    phone: pl.phone,
                    panNumber: pl.panNumber,
                    reason: err.code || err.name || err.message,
                };
            }
        }),
        SAVE_CONCURRENCY
    );

    // `saved` = rows newly written to the leads table (counters apply).
    // `reused` = rows that resolved to an existing lead (dispatch only).
    const saved = results.filter((r) => r.ok && !r.reused).map((r) => r.lead);
    const reused = results.filter((r) => r.ok && r.reused).map((r) => r.lead);

    // ── Keep counters in sync (createFast does not touch them) ──────────────
    // All-time per-source counter + daily counters, bucketed to minimize writes.
    if (saved.length) {
        const sourceIncrements = {};   // source -> count
        const dailyIncrements  = {};   // "source|YYYY-MM-DD" -> count
        for (const lead of saved) {
            const src = lead.source;
            if (!src) continue;
            sourceIncrements[src] = (sourceIncrements[src] || 0) + 1;
            const day = String(lead.createdAt || '').slice(0, 10);
            if (day) {
                const dk = `${src}|${day}`;
                dailyIncrements[dk] = (dailyIncrements[dk] || 0) + 1;
            }
        }
        await Promise.all(
            Object.entries(sourceIncrements).map(([src, cnt]) =>
                Lead.incrementCounterBy(src, cnt).catch((err) =>
                    console.error(`[Push] all-time counter update failed (${src}):`, err.message)
                )
            )
        );
        await Lead.bumpDailyCountersBulk(dailyIncrements).catch((err) =>
            console.error('[Push] Daily counter bulk update failed:', err.message)
        );
    }

    return {
        saved,
        reused,
        failed: results.filter((r) => !r.ok),
    };
}

// ============================================================================
// BACKGROUND PUSH PROCESSOR
//
//  • Save loop: pages process_leads (PAGE_SIZE rows), creates/reuses the leads-
//    table rows and appends every dispatchable lead to push_job_items with an
//    increasing seq. Checkpointed after every page (lastKey + nextSeq).
//  • One worker per lender reads push_job_items from its own cursor at its own
//    rate limit. A slow lender (e.g. 30 leads/min) no longer makes the fast
//    ones wait page after page — the old loop moved at the slowest lender's pace.
//  • The save loop stays at most SAVE_AHEAD leads ahead of the FASTEST lender,
//    so stopping a job never leaves a large pile of saved-but-unsent leads.
//  • Everything lives in DynamoDB (push_jobs + push_job_items): the page can be
//    closed, the server can restart, and each lender resumes from its cursor.
// ============================================================================

const PAGE_SIZE = 500;   // leads fetched from process_leads per save-loop page
const SAVE_AHEAD = 1000; // max leads prepared beyond the fastest lender's position

// In-memory guard so the same jobId's background loop never runs twice in this
// process (e.g. boot-resume racing a manual /resume). Single-instance app.
const activeJobIds = new Set();

// Jobs an admin asked to stop; checked before every lender batch. The DB
// status ('cancelled') is the durable copy, re-read every few seconds.
const cancelledJobIds = new Set();

// Per-lender stops: jobId -> Set(lender).
const stoppedLendersByJob = new Map();
const isLenderStopped = (jobId, lender) => stoppedLendersByJob.get(jobId)?.has(lender) || false;
const markLenderStoppedInMemory = (jobId, lender) => {
    if (!stoppedLendersByJob.has(jobId)) stoppedLendersByJob.set(jobId, new Set());
    stoppedLendersByJob.get(jobId).add(lender);
};

async function runSaveLoop(jobId, job, state, { source, startDate, endDate, lenders }, shouldStop) {
    let lastKey = job.lastKey ? JSON.parse(job.lastKey) : null;
    const totals = {
        savedToLeads: job.savedToLeads || 0,
        reusedLeads: job.reusedLeads || 0,
        failedToSave: job.failedToSave || 0,
        queuedForLenders: job.queuedForLenders || 0,
        queueWriteFailed: job.queueWriteFailed || 0,
    };
    let pageNumber = 0;

    // Already past the first page and nothing left → the previous run finished saving.
    if (job.nextSeq > 0 && !job.lastKey) { state.saveDone = true; return; }

    do {
        // Wait while we're far enough ahead of the fastest live lender.
        for (;;) {
            if (shouldStop()) return;
            const live = lenders.filter((l) => !isLenderStopped(jobId, l) && !state.finished.has(l));
            if (live.length === 0) return;
            const fastest = Math.max(...live.map((l) => state.cursors[l] ?? -1));
            if (state.nextSeq - 1 - fastest < SAVE_AHEAD) break;
            await sleep(500);
        }

        pageNumber++;
        const { items: page, lastEvaluatedKey } = await ProcessLead.findBySourceAndDateRange(
            source, startDate, endDate, { limit: PAGE_SIZE, lastKey }
        );
        if (page.length === 0) break;

        const { saved, reused, failed } = await savePageToLeadsTable(page);
        const toDispatch = saved.concat(reused);
        const writeFailed = toDispatch.length ? await PushJobItem.appendMany(jobId, state.nextSeq, toDispatch) : 0;

        state.nextSeq += toDispatch.length;
        totals.savedToLeads += saved.length;
        totals.reusedLeads += reused.length;
        totals.failedToSave += failed.length;
        totals.queuedForLenders += toDispatch.length - writeFailed;
        totals.queueWriteFailed += writeFailed;
        if (writeFailed) console.error(`[Push ${jobId}] ${writeFailed} leads could not be queued for lenders on page ${pageNumber}`);

        lastKey = lastEvaluatedKey || null;
        await PushJob.checkpoint(jobId, {
            lastKey: lastKey ? JSON.stringify(lastKey) : null,
            nextSeq: state.nextSeq,
            ...totals,
        });
        console.log(`[Push ${jobId}] Page ${pageNumber}: saved=${saved.length} reused=${reused.length} failed=${failed.length} | queued total=${totals.queuedForLenders}`);
    } while (lastKey);

    state.saveDone = true;
    await PushJob.update(jobId, { saveDone: true, lastKey: null });
}

async function runPushInBackground(jobId, { source, startDate, endDate, lenders }) {
    if (activeJobIds.has(jobId)) {
        console.log(`[Push ${jobId}] Already running in this process — skipping duplicate launch`);
        return;
    }
    activeJobIds.add(jobId);
    let watcher = null;

    try {
        console.log(`[Push ${jobId}] Starting | source=${source} | ${startDate} → ${endDate} | lenders=${lenders.join(',')}`);

        const job = await PushJob.get(jobId);
        if (!job) {
            console.error(`[Push ${jobId}] Job record not found — cannot run`);
            return;
        }
        if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
            console.log(`[Push ${jobId}] Already ${job.status} — skipping`);
            return;
        }

        const state = {
            cancelled: false,
            fatal: null,
            saveDone: !!job.saveDone,
            nextSeq: job.nextSeq || 0,
            cursors: {},
            finished: new Set(),
        };
        (job.stoppedLenders || []).forEach((l) => markLenderStoppedInMemory(jobId, l));

        const shouldStop = () => state.cancelled || !!state.fatal || cancelledJobIds.has(jobId);
        const lenderStop = (l) => () => shouldStop() || isLenderStopped(jobId, l);

        // Pick up a cancel / lender stop written by another request or before a restart.
        watcher = setInterval(async () => {
            try {
                const fresh = await PushJob.get(jobId);
                if (fresh && fresh.status === 'cancelled') state.cancelled = true;
                (fresh?.stoppedLenders || []).forEach((l) => markLenderStoppedInMemory(jobId, l));
            } catch (_) { /* next tick */ }
        }, 5000);

        const limiters = buildLenderLimiters(lenders);
        const saver = state.saveDone
            ? Promise.resolve()
            : runSaveLoop(jobId, job, state, { source, startDate, endDate, lenders }, shouldStop)
                .catch((err) => { state.fatal = err; console.error(`[Push ${jobId}] Save loop failed:`, err); });
        const workers = lenders.map((l) => runLenderWorker(jobId, l, job, state, limiters[l], lenderStop(l)));

        await saver;
        // If the save loop stopped early (e.g. every lender was stopped), let the
        // workers see "no more leads are coming".
        state.saveDone = true;
        const results = await Promise.all(workers);
        const finalResults = Object.fromEntries(results.map((r) => [r.lender, r]));

        if (state.fatal) {
            await PushJob.update(jobId, { lenderResults: finalResults }).catch(() => {});
            await PushJob.markFailed(jobId, state.fatal.message);
            return;
        }

        const allStopped = lenders.every((l) => isLenderStopped(jobId, l));
        if (state.cancelled || cancelledJobIds.has(jobId) || allStopped) {
            if (allStopped) await PushJob.markCancelled(jobId);
            await PushJob.update(jobId, { lenderResults: finalResults });
            console.log(`[Push ${jobId}] ⛔ Stopped by admin`);
            return;
        }

        await PushJob.markCompleted(jobId, finalResults);
        console.log(`[Push ${jobId}] Done.`);
    } catch (err) {
        console.error(`[Push ${jobId}] Fatal:`, err);
        await PushJob.markFailed(jobId, err.message).catch((markErr) => {
            console.error(`[Push ${jobId}] Also failed to mark job as failed:`, markErr.message);
        });
    } finally {
        if (watcher) clearInterval(watcher);
        activeJobIds.delete(jobId);
        cancelledJobIds.delete(jobId);
        stoppedLendersByJob.delete(jobId);
    }
}

// ============================================================================
// RESUME INCOMPLETE JOBS ON SERVER STARTUP
//
// Call this once in app.js / server.js after DB is ready:
//   require('./controllers/processLeadController').resumeIncompletePushJobs();
//
// It finds all jobs still in "processing" state that were interrupted by a
// restart, and re-launches their background loop from the checkpoint.
// ============================================================================

exports.resumeIncompletePushJobs = async () => {
    try {
        const activeJobs = await PushJob.findActiveJobs();

        if (activeJobs.length === 0) {
            console.log('[ProcessLead] No interrupted push jobs found on startup.');
            return;
        }

        console.log(`[ProcessLead] Found ${activeJobs.length} interrupted push job(s) — auto-resuming from checkpoint...`);

        // Stagger the relaunches slightly so a crash that interrupted many jobs
        // at once doesn't slam DynamoDB/lender APIs the instant the server boots.
        activeJobs.forEach((job, i) => {
            setTimeout(() => {
                console.log(`[ProcessLead] Auto-resuming job ${job.jobId} (source=${job.source})`);
                runPushInBackground(job.jobId, {
                    source: job.source,
                    startDate: job.startDate,
                    endDate: job.endDate,
                    lenders: job.lenders,
                }).catch((err) => {
                    console.error(`[ProcessLead] Auto-resume failed for job ${job.jobId}:`, err.message);
                });
            }, i * 2000);
        });
    } catch (err) {
        console.error('[ProcessLead] resumeIncompletePushJobs failed:', err.message);
    }
};

// ============================================================================
// ROUTE HANDLERS
// ============================================================================

/**
 * POST /api/v1/process-leads/upload
 */
/**
 * Shared processing routine. Reads an xlsx file from disk, streams it row by
 * row and bulk-inserts into the process_leads table. Used by both the classic
 * single-shot upload and the chunked upload flow.
 */
async function processExcelFile(tempFilePath, source, fileName = null) {
    const uploadBatch = `${source}-${Date.now()}`;
    const startedAt = new Date().toISOString();
    const summary = { total: 0, succeeded: 0, failed: 0, skipped: 0 };

    for await (const chunk of streamExcelChunks(tempFilePath, 2000)) {
        const leadsData = [];
        for (const { raw } of chunk) {
            summary.total++;
            const leadData = rowToLeadData(raw);
            if (Object.keys(leadData).length === 0) { summary.skipped++; continue; }
            leadData.source = leadData.source || source;
            leadData.uploadBatch = uploadBatch;
            leadsData.push(leadData);
        }
        if (leadsData.length > 0) {
            const { successful, failed } = await ProcessLead.createBulk(leadsData);
            summary.succeeded += successful.length;
            summary.failed += failed.length;
        }
    }

    if (summary.succeeded > 0) {
        await ProcessLead.incrementCounterBy(source, summary.succeeded).catch((e) =>
            console.error('[ProcessLead] Counter error:', e.message)
        );
    }

    // History entry for admin/lead_upload (best-effort — never fails the upload).
    await PushJob.recordUpload(uploadBatch, {
        status: 'completed', source, fileName,
        total: summary.total, succeeded: summary.succeeded, failed: summary.failed, skipped: summary.skipped,
        startedAt, completedAt: new Date().toISOString(),
    }).catch((e) => console.error('[ProcessLead] Upload history write failed:', e.message));

    return { summary, uploadBatch };
}

exports.uploadProcessLeads = [
    upload.single('file'),
    async (req, res) => {
        const tempFilePath = req.file?.path;
        try {
            if (!req.file) return res.status(400).json({ success: false, message: 'No file uploaded' });
            if (!req.body.source) {
                fs.unlink(tempFilePath, () => { });
                return res.status(400).json({ success: false, message: 'source is required' });
            }

            const source = req.body.source;
            const { summary, uploadBatch } = await processExcelFile(tempFilePath, source, req.body.fileName || req.file.originalname);

            return res.status(200).json({
                success: true,
                message: `Upload complete. ${summary.succeeded} saved, ${summary.failed} failed, ${summary.skipped} skipped.`,
                summary, uploadBatch, source,
            });
        } catch (err) {
            console.error('[ProcessLead] Upload error:', err);
            return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
        } finally {
            if (tempFilePath) fs.unlink(tempFilePath, (e) => { if (e) console.warn('[ProcessLead] Temp file delete failed:', e.message); });
        }
    },
];

// ============================================================================
// CHUNKED UPLOAD
// ----------------------------------------------------------------------------
// Platforms in front of this API (e.g. Vercel / some proxies) reject a single
// request body larger than a few MB with HTTP 413. To upload large xlsx files
// the frontend slices the file into small chunks and sends them one by one;
// the backend appends each chunk to a temp file and processes it once the last
// chunk has arrived. Each individual request stays well under the size limit.
// ============================================================================

// Per-upload temp directory. We append chunks sequentially to a single file.
const CHUNK_UPLOAD_DIR = path.join(os.tmpdir(), 'process-lead-chunks');

function chunkFilePath(uploadId) {
    // Strip anything that isn't a safe id char to avoid path traversal.
    const safeId = String(uploadId).replace(/[^a-zA-Z0-9_-]/g, '');
    if (!safeId) return null;
    return path.join(CHUNK_UPLOAD_DIR, `${safeId}.part`);
}

/**
 * POST /api/v1/process-leads/upload/chunk
 *
 * Headers:
 *   x-upload-id     unique id for this upload session (generated by client)
 *   x-chunk-index   0-based index of this chunk
 *   x-total-chunks  total number of chunks
 * Body: raw binary slice of the file (application/octet-stream)
 *
 * Appends the chunk to the upload's temp file. Chunks MUST be sent in order.
 */
exports.uploadProcessLeadsChunk = [
    express.raw({ type: '*/*', limit: '20mb' }),
    async (req, res) => {
        try {
            const uploadId = req.headers['x-upload-id'];
            const chunkIndex = parseInt(req.headers['x-chunk-index'], 10);
            const totalChunks = parseInt(req.headers['x-total-chunks'], 10);

            if (!uploadId || Number.isNaN(chunkIndex) || Number.isNaN(totalChunks)) {
                return res.status(400).json({ success: false, message: 'Missing x-upload-id / x-chunk-index / x-total-chunks headers' });
            }
            if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
                return res.status(400).json({ success: false, message: 'Empty chunk body' });
            }

            const filePath = chunkFilePath(uploadId);
            if (!filePath) return res.status(400).json({ success: false, message: 'Invalid x-upload-id' });

            await fs.promises.mkdir(CHUNK_UPLOAD_DIR, { recursive: true });

            // First chunk truncates/creates the file; later chunks append.
            const flag = chunkIndex === 0 ? 'w' : 'a';
            await fs.promises.writeFile(filePath, req.body, { flag });

            return res.status(200).json({
                success: true,
                message: `Chunk ${chunkIndex + 1}/${totalChunks} received`,
                received: req.body.length,
            });
        } catch (err) {
            console.error('[ProcessLead] Chunk upload error:', err);
            return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
        }
    },
];

/**
 * POST /api/v1/process-leads/upload/complete
 *
 * Body (JSON): { uploadId, source }
 *
 * Processes the fully-assembled temp file the same way the classic upload does,
 * then deletes it.
 */
exports.completeProcessLeadsUpload = async (req, res) => {
    const { uploadId, source, fileName } = req.body || {};
    const filePath = uploadId ? chunkFilePath(uploadId) : null;
    try {
        if (!uploadId || !filePath) return res.status(400).json({ success: false, message: 'uploadId is required' });
        if (!source) return res.status(400).json({ success: false, message: 'source is required' });
        if (!fs.existsSync(filePath)) {
            return res.status(404).json({ success: false, message: 'No uploaded chunks found for this uploadId' });
        }

        const { summary, uploadBatch } = await processExcelFile(filePath, source, fileName || null);

        return res.status(200).json({
            success: true,
            message: `Upload complete. ${summary.succeeded} saved, ${summary.failed} failed, ${summary.skipped} skipped.`,
            summary, uploadBatch, source,
        });
    } catch (err) {
        console.error('[ProcessLead] Complete upload error:', err);
        return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    } finally {
        if (filePath) fs.unlink(filePath, () => { });
    }
};

/**
 * POST /api/v1/process-leads/push
 *
 * Body: { source, startDate, endDate, lenders[] }
 *
 * Returns 202 with jobId immediately.
 * Job state is persisted in DynamoDB — frontend can close, EC2 can restart,
 * the job continues from its last checkpoint.
 */
exports.pushProcessLeads = async (req, res) => {
    try {
        const { source, startDate, endDate, lenders } = req.body;

        if (!source || !startDate || !endDate || !Array.isArray(lenders) || lenders.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'source, startDate, endDate, and lenders[] are required',
            });
        }
        if (isNaN(Date.parse(startDate)) || isNaN(Date.parse(endDate))) {
            return res.status(400).json({ success: false, message: 'Invalid date format. Use "2025-01-01" or full ISO string.' });
        }
        if (new Date(startDate) > new Date(endDate)) {
            return res.status(400).json({ success: false, message: 'startDate must be before endDate' });
        }

        const unknown = lenders.filter((l) => !LENDER_MAP[l]);
        if (unknown.length > 0) {
            return res.status(400).json({
                success: false,
                message: `Unknown lender(s): ${unknown.join(', ')}. Valid: ${Object.keys(LENDER_MAP).join(', ')}`,
            });
        }

        // Normalise dates — GSI needs full ISO strings
        const { start: normStart, end: normEnd } = normaliseDateRange(startDate, endDate);

        console.log(`[Push] source=${source} | ${normStart} → ${normEnd} | lenders=${lenders.join(',')}`);

        const totalCount = await ProcessLead.countBySourceAndDateRange(source, normStart, normEnd);
        console.log(`[Push] Count: ${totalCount}`);

        if (totalCount === 0) {
            return res.status(404).json({
                success: false,
                message: 'No leads found in process_leads for this source + date range.',
                hint: 'Check: (1) source is case-sensitive, (2) date range covers upload time, (3) upload completed.',
                queried: { source, startDate: normStart, endDate: normEnd },
            });
        }

        const jobId = uuidv4();

        // Persist job to DynamoDB BEFORE responding — so it survives a restart
        await PushJob.create(jobId, {
            source,
            startDate: normStart,
            endDate: normEnd,
            lenders,
            totalFetched: totalCount,
        });

        res.status(202).json({
            success: true,
            message: `Push started. ${totalCount} leads will be processed in the background.`,
            jobId,
            totalLeads: totalCount,
            pollUrl: `/api/v1/process-leads/push-jobs/${jobId}`,
        });

        // Launch background loop AFTER response is sent
        setImmediate(() =>
            runPushInBackground(jobId, {
                source,
                startDate: normStart,
                endDate: normEnd,
                lenders,
            })
        );

    } catch (err) {
        console.error('[ProcessLead] Push error:', err);
        res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    }
};

/**
 * GET /api/v1/process-leads/push-jobs/:jobId
 * Poll job status — reads from DynamoDB, works even after server restart.
 */
exports.getPushJobStatus = async (req, res) => {
    try {
        const job = await PushJob.get(req.params.jobId);
        if (!job) return res.status(404).json({ success: false, message: 'Job not found' });
        res.status(200).json({ success: true, data: job });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * POST /api/v1/process-leads/push-jobs/:jobId/resume
 *
 * Call this if the server restarted mid-push.
 * Reads the checkpointed lastKey from DynamoDB and continues from there.
 * Frontend can call this automatically when it polls and sees status=processing
 * but suspects the server restarted (e.g. progress stalled for 60+ seconds).
 */
exports.resumePushJob = async (req, res) => {
    try {
        const job = await PushJob.get(req.params.jobId);
        if (!job) return res.status(404).json({ success: false, message: 'Job not found' });

        if (job.status === 'completed') {
            return res.status(200).json({ success: true, message: 'Job already completed', data: job });
        }
        if (job.status === 'failed') {
            return res.status(200).json({ success: true, message: 'Job previously failed — create a new push', data: job });
        }
        if (job.status === 'cancelled') {
            return res.status(200).json({ success: true, message: 'Job was stopped by an admin — create a new push', data: job });
        }

        // status === 'processing' — re-launch from checkpoint
        res.status(202).json({
            success: true,
            message: 'Job resumed from checkpoint.',
            jobId: job.jobId,
            pollUrl: `/api/v1/process-leads/push-jobs/${job.jobId}`,
        });

        setImmediate(() =>
            runPushInBackground(job.jobId, {
                source: job.source,
                startDate: job.startDate,
                endDate: job.endDate,
                lenders: job.lenders,
            })
        );

    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * POST /api/v1/process-leads/push-jobs/:jobId/cancel
 *
 * Stop a running push. Lender batches already in flight finish (a request
 * that has left the server can't be recalled), but nothing further is sent.
 * Leads already saved/sent stay as they are.
 */
exports.cancelPushJob = async (req, res) => {
    try {
        const { jobId } = req.params;
        const job = await PushJob.get(jobId);
        if (!job) return res.status(404).json({ success: false, message: 'Job not found' });

        if (job.status !== 'processing') {
            return res.status(409).json({
                success: false,
                message: `Job is already ${job.status} — nothing to stop.`,
                data: job,
            });
        }

        // In-memory flag first so the running loop stops at its very next check.
        if (activeJobIds.has(jobId)) cancelledJobIds.add(jobId);

        const updated = await PushJob.markCancelled(jobId);
        if (!updated) {
            cancelledJobIds.delete(jobId);
            const latest = await PushJob.get(jobId);
            return res.status(409).json({
                success: false,
                message: `Job finished (${latest?.status}) before it could be stopped.`,
                data: latest,
            });
        }

        console.log(`[Push ${jobId}] Cancel requested by admin`);
        res.status(200).json({
            success: true,
            message: 'Push stopped. Nothing more will be sent to lenders for this job.',
            data: updated,
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * POST /api/v1/process-leads/push-jobs/:jobId/lenders/:lender/stop
 *
 * Stop ONE lender of a running push. The other lenders keep receiving leads.
 * The lender's batch already in flight finishes; nothing further is sent to it.
 * Stopping the last remaining lender ends the whole job (status 'cancelled').
 */
exports.stopPushJobLender = async (req, res) => {
    try {
        const { jobId, lender } = req.params;
        const job = await PushJob.get(jobId);
        if (!job) return res.status(404).json({ success: false, message: 'Job not found' });

        if (job.status !== 'processing') {
            return res.status(409).json({ success: false, message: `Job is already ${job.status} — nothing to stop.`, data: job });
        }
        if (!(job.lenders || []).includes(lender)) {
            return res.status(400).json({ success: false, message: `${lender} is not part of this job.`, data: job });
        }
        if ((job.stoppedLenders || []).includes(lender)) {
            return res.status(409).json({ success: false, message: `${lender} is already stopped for this job.`, data: job });
        }

        // In-memory flag first so the running loop skips this lender at its next check.
        if (activeJobIds.has(jobId)) markLenderStoppedInMemory(jobId, lender);

        const updated = await PushJob.stopLender(jobId, lender);
        if (!updated) {
            const latest = await PushJob.get(jobId);
            return res.status(409).json({ success: false, message: `Could not stop ${lender} — job is ${latest?.status}.`, data: latest });
        }

        // Last lender standing → stop the whole job.
        const remaining = (updated.lenders || []).filter((l) => !(updated.stoppedLenders || []).includes(l));
        let data = updated;
        if (remaining.length === 0) {
            if (activeJobIds.has(jobId)) cancelledJobIds.add(jobId);
            data = (await PushJob.markCancelled(jobId)) || (await PushJob.get(jobId));
        }

        console.log(`[Push ${jobId}] Lender ${lender} stopped by admin (remaining: ${remaining.join(',') || 'none'})`);
        res.status(200).json({
            success: true,
            message: remaining.length
                ? `${lender} stopped. Still sending to: ${remaining.join(', ')}.`
                : `${lender} stopped. That was the last lender, so the whole push is stopped.`,
            data,
        });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * GET /api/v1/process-leads/push-jobs?status=processing
 * Lists push jobs that are still running, so the dashboard can offer Stop
 * even after the page was reloaded or opened on another machine.
 */
exports.listActivePushJobs = async (_req, res) => {
    try {
        const jobs = await PushJob.findActiveJobs();
        jobs.sort((a, b) => String(b.startedAt || '').localeCompare(String(a.startedAt || '')));
        res.status(200).json({ success: true, data: jobs });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * GET /api/v1/process-leads/history?type=PUSH_JOB|UPLOAD&limit=200
 * Push jobs and uploads, newest first — the History tab of admin/lead_upload.
 */
exports.getHistory = async (req, res) => {
    try {
        const type = ['PUSH_JOB', 'UPLOAD'].includes(req.query.type) ? req.query.type : undefined;
        const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 200, 1), 1000);
        const items = await PushJob.list({ type, limit });
        res.status(200).json({ success: true, data: items });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * GET /api/v1/process-leads/lenders
 */
exports.getAvailableLenders = (_req, res) => {
    res.status(200).json({ success: true, lenders: Object.keys(LENDER_MAP) });
};

/**
 * GET /api/v1/process-leads/count?source=FREO&startDate=2025-01-01&endDate=2025-01-31
 */
exports.getLeadCount = async (req, res) => {
    try {
        const { source, startDate, endDate } = req.query;
        if (!source || !startDate || !endDate) {
            return res.status(400).json({ success: false, message: 'source, startDate, endDate are required' });
        }
        const { start: normStart, end: normEnd } = normaliseDateRange(startDate, endDate);
        const count = await ProcessLead.countBySourceAndDateRange(source, normStart, normEnd);
        res.status(200).json({ success: true, source, startDate: normStart, endDate: normEnd, count });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Server error', error: err.message });
    }
};

/**
 * POST /api/v1/process-leads/dedup-check
 *
 * Body (JSON): { phones: string[], pans: string[], source: string,
 *                lookbackDays?: number }
 *
 * NOTHING IS REJECTED HERE. A phone/PAN we already hold no longer removes the
 * row from the upload — the row still goes to process_leads and is still pushed
 * to the selected lenders. What a match does is stamp the EXISTING lead row:
 *
 *   existing lead has no source2                        → set source2 = source,
 *                                                          updatedAt = now
 *   existing lead has source2, updatedAt older than
 *   `lookbackDays` (default 30)                         → same re-stamp
 *   existing lead has source2, updatedAt inside the
 *   window                                              → left untouched
 *
 * The match itself ignores createdAt — the 30-day window applies to updatedAt
 * only, so an old lead that has never been re-supplied still gets stamped.
 *
 * The response maps every matched phone/PAN to the leadId it matched. The
 * caller writes that leadId onto the row (column `existingLeadId`) so the push
 * step dispatches it to lenders under the existing lead instead of creating a
 * second row in the leads table.
 */
const DEDUP_CONCURRENCY = 64;

exports.dedupCheckLeads = async (req, res) => {
    try {
        const body = req.body || {};
        const lookbackDays = Number(body.lookbackDays) > 0
            ? Number(body.lookbackDays)
            : Lead.SOURCE2_LOOKBACK_DAYS;
        const source = body.source ? String(body.source).trim() : '';

        // Normalise + de-duplicate the input lists so we issue one query per
        // distinct value (phones as strings, PANs upper-cased).
        const phones = Array.from(new Set(
            (Array.isArray(body.phones) ? body.phones : [])
                .map((p) => (p == null ? '' : String(p).trim()))
                .filter(Boolean)
        ));
        const pans = Array.from(new Set(
            (Array.isArray(body.pans) ? body.pans : [])
                .map((p) => (p == null ? '' : String(p).trim().toUpperCase()))
                .filter(Boolean)
        ));

        if (phones.length === 0 && pans.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Provide at least one of phones[] or pans[]',
            });
        }

        if (!source) {
            return res.status(400).json({
                success: false,
                message: 'source is required — it is what gets written to source2',
            });
        }

        // One lead can be hit by both its phone and its PAN in the same batch;
        // stamp it once only.
        const stamped = new Set();
        const counts = { tagged: 0, skipped: 0, errors: 0 };

        const resolve = async (lead) => {
            if (!lead) return null;
            if (stamped.has(lead.leadId)) return lead.leadId;
            stamped.add(lead.leadId);
            const outcome = await Lead.tagSource2(lead, source, lookbackDays);
            if (outcome === 'tagged') counts.tagged++;
            else if (outcome === 'error') counts.errors++;
            else counts.skipped++;
            return lead.leadId;
        };

        const phoneResults = await runWithConcurrency(
            phones.map((phone) => async () => ({
                phone,
                leadId: await resolve(await Lead.findAnyByPhone(phone)),
            })),
            DEDUP_CONCURRENCY
        );
        const panResults = await runWithConcurrency(
            pans.map((pan) => async () => ({
                pan,
                leadId: await resolve(await Lead.findAnyByPanNumber(pan)),
            })),
            DEDUP_CONCURRENCY
        );

        // phone/PAN -> existing leadId, for the rows the caller must tag.
        const phoneLeadIds = {};
        phoneResults
            .filter((r) => r && !r.error && r.leadId)
            .forEach((r) => { phoneLeadIds[r.phone] = r.leadId; });
        const panLeadIds = {};
        panResults
            .filter((r) => r && !r.error && r.leadId)
            .forEach((r) => { panLeadIds[r.pan] = r.leadId; });

        return res.status(200).json({
            success: true,
            lookbackDays,
            source,
            checkedPhones: phones.length,
            checkedPans: pans.length,
            matchedPhones: Object.keys(phoneLeadIds),
            matchedPans: Object.keys(panLeadIds),
            phoneLeadIds,
            panLeadIds,
            source2Tagged: counts.tagged,
            source2Skipped: counts.skipped,
            source2Errors: counts.errors,
        });
    } catch (err) {
        console.error('[ProcessLead] Dedup-check error:', err);
        return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    }
};

/**
 * GET /api/v1/process-leads/template
 */
exports.downloadTemplate = (_req, res) => {
    const headers = [
        'source', 'fullName', 'firstName', 'lastName', 'phone', 'email', 'panNumber',
        'dateOfBirth', 'age', 'gender', 'jobType', 'businessType', 'salary',
        'creditScore', 'cibilScore', 'address', 'pincode', 'consent', 'createdAt',
    ];
    const sampleRow = {
        source: 'FREO', fullName: 'Ramesh Kumar', firstName: 'Ramesh', lastName: 'Kumar',
        phone: '9876543210', email: 'ramesh@example.com', panNumber: 'ABCDE1234F',
        dateOfBirth: '1990-05-15', age: 34, gender: 'Male', jobType: 'SALARIED',
        businessType: '', salary: 55000, creditScore: 720, cibilScore: '',
        address: '123 MG Road, Bengaluru', pincode: '560001', consent: true,
        createdAt: '2024-03-15T10:30:00.000Z',
    };
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet([sampleRow], { header: headers });
    ws['!cols'] = headers.map((h) => ({ wch: Math.max(h.length + 4, 18) }));
    XLSX.utils.book_append_sheet(wb, ws, 'ProcessLeads');
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Disposition', 'attachment; filename="process_leads_template.xlsx"');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buffer);
};