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
const Lead = require('../models/leadModel');

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
// LENDER DISPATCH
// ============================================================================

const LENDER_BATCH = 50;
const LENDER_CONC = 3;
const LENDER_DELAY = 300;

async function dispatchToLender(lender, leads) {
    const sendFn = LENDER_MAP[lender];
    if (!sendFn) return { lender, status: 'skipped', message: 'Not configured', total: leads.length };

    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < leads.length; i += LENDER_BATCH * LENDER_CONC) {
        const round = leads.slice(i, i + LENDER_BATCH * LENDER_CONC);
        const batches = [];
        for (let j = 0; j < round.length; j += LENDER_BATCH) batches.push(round.slice(j, j + LENDER_BATCH));

        const tasks = batches.map((batch) => async () =>
            Promise.allSettled(batch.map((lead) => sendFn({ ...lead, leadId: lead.leadId })))
        );

        const roundResults = await runWithConcurrency(tasks, LENDER_CONC);
        roundResults.forEach((batchResult) => {
            if (batchResult?.error) { failCount += LENDER_BATCH; return; }
            batchResult.forEach((r) => r.status === 'fulfilled' ? successCount++ : failCount++);
        });

        if (i + LENDER_BATCH * LENDER_CONC < leads.length) await sleep(LENDER_DELAY);
    }

    return { lender, status: 'done', total: leads.length, successCount, failCount };
}

// ============================================================================
// SAVE ONE PAGE OF LEADS TO leads TABLE
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

    return {
        saved: results.filter((r) => r.ok).map((r) => r.lead),
        failed: results.filter((r) => !r.ok),
    };
}

// ============================================================================
// BACKGROUND PUSH PROCESSOR
//
// Key design decisions that make this survive frontend close / EC2 restarts:
//
//  1. Job state lives in DynamoDB (PushJob model), NOT in process memory.
//     If the process dies and restarts, the job record is still there.
//
//  2. Pagination is checkpointed after every page. ProcessLead.findBySourceAndDateRange
//     accepts a lastKey token. After each page is saved+dispatched, the lastKey
//     is written back to DynamoDB. A restart calls resumePushJob() which reads
//     the lastKey and continues from exactly where it left off.
//
//  3. The push loop processes PAGE_SIZE leads at a time — never loads the full
//     dataset into memory. Safe for 100k+ leads on t2.micro.
//
//  4. Lender dispatch happens per-page too. All lenders receive each page before
//     moving to the next page. This means lenders are always fed in order and
//     no lead is dispatched twice even on resume.
// ============================================================================

const PAGE_SIZE = 500; // leads fetched from process_leads per loop iteration

async function runPushInBackground(jobId, { source, startDate, endDate, lenders }) {
    console.log(`[Push ${jobId}] Starting | source=${source} | ${startDate} → ${endDate}`);

    // Read existing job to check for a checkpoint (resume after restart)
    let job = await PushJob.get(jobId);
    if (!job) {
        console.error(`[Push ${jobId}] Job record not found in DynamoDB — cannot run`);
        return;
    }

    // If already completed or failed do not re-run
    if (job.status === 'completed' || job.status === 'failed') {
        console.log(`[Push ${jobId}] Already ${job.status} — skipping`);
        return;
    }

    try {
        // Restore checkpoint if this is a resume after restart
        let lastKey = job.lastKey ? JSON.parse(job.lastKey) : null;
        let totalSaved = job.savedToLeads || 0;
        let totalFailed = job.failedToSave || 0;
        let allSavedLeads = [];   // accumulates per-page for lender dispatch

        const lenderAccumulators = {};
        lenders.forEach((l) => {
            lenderAccumulators[l] = { lender: l, status: 'running', total: 0, successCount: 0, failCount: 0 };
        });

        // Restore partial lender results if resuming
        if (job.lenderResults && Object.keys(job.lenderResults).length > 0) {
            Object.entries(job.lenderResults).forEach(([l, r]) => {
                if (lenderAccumulators[l]) {
                    lenderAccumulators[l].total = r.total || 0;
                    lenderAccumulators[l].successCount = r.successCount || 0;
                    lenderAccumulators[l].failCount = r.failCount || 0;
                }
            });
        }

        let pageNumber = 0;

        // ── Main pagination loop ───────────────────────────────────────────────
        do {
            pageNumber++;
            console.log(`[Push ${jobId}] Page ${pageNumber} | lastKey=${lastKey ? 'set' : 'null'}`);

            // Fetch one page from process_leads via GSI
            const { items: page, lastEvaluatedKey } = await ProcessLead.findBySourceAndDateRange(
                source, startDate, endDate,
                { limit: PAGE_SIZE, lastKey }
            );

            if (page.length === 0) {
                console.log(`[Push ${jobId}] Empty page — done`);
                break;
            }

            // Save this page to the leads table
            const { saved, failed } = await savePageToLeadsTable(page);

            totalSaved += saved.length;
            totalFailed += failed.length;

            console.log(`[Push ${jobId}] Page ${pageNumber}: saved=${saved.length} failed=${failed.length} | running total saved=${totalSaved}`);

            // Dispatch this page to every selected lender
            if (saved.length > 0) {
                const lenderResults = await Promise.allSettled(
                    lenders.map((lender) => dispatchToLender(lender, saved))
                );
                lenders.forEach((lender, i) => {
                    const r = lenderResults[i];
                    if (r.status === 'fulfilled') {
                        lenderAccumulators[lender].total += r.value.total || 0;
                        lenderAccumulators[lender].successCount += r.value.successCount || 0;
                        lenderAccumulators[lender].failCount += r.value.failCount || 0;
                    } else {
                        lenderAccumulators[lender].failCount += saved.length;
                        console.error(`[Push ${jobId}] Lender ${lender} error on page ${pageNumber}:`, r.reason?.message);
                    }
                });
            }

            // ── CHECKPOINT — write progress to DynamoDB ────────────────────────
            // If the process dies here, the next start resumes from lastEvaluatedKey.
            lastKey = lastEvaluatedKey || null;
            await PushJob.checkpoint(
                jobId,
                lastKey ? JSON.stringify(lastKey) : null,
                totalSaved,
                totalFailed
            );
            // Also persist rolling lender totals so poll response is always fresh
            await PushJob.update(jobId, {
                lenderResults: Object.fromEntries(
                    Object.entries(lenderAccumulators).map(([l, r]) => [l, { ...r, status: 'running' }])
                ),
            });

            await sleep(0); // yield event loop between pages

        } while (lastKey); // lastKey null = no more pages

        // ── Mark job complete ─────────────────────────────────────────────────
        const finalLenderResults = Object.fromEntries(
            Object.entries(lenderAccumulators).map(([l, r]) => [l, { ...r, status: 'done' }])
        );

        await PushJob.markCompleted(jobId, finalLenderResults);

        console.log(`[Push ${jobId}] Done. Saved=${totalSaved} Failed=${totalFailed}`);

    } catch (err) {
        console.error(`[Push ${jobId}] Fatal:`, err);
        await PushJob.markFailed(jobId, err.message);
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
    // We can't scan (no Scan) so resume is triggered explicitly by a re-push
    // from the frontend on the same jobId, OR you can store a list of active
    // jobIds in a single known DynamoDB key like "JOB#ACTIVE_LIST" and read it here.
    // For most use cases, the frontend polls and sees status=processing, then
    // calls GET /push-jobs/:jobId/resume if the process restarted.
    console.log('[ProcessLead] Server started. Call GET /api/v1/process-leads/push-jobs/:jobId/resume to resume any interrupted job.');
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
async function processExcelFile(tempFilePath, source) {
    const uploadBatch = `${source}-${Date.now()}`;
    const summary = { total: 0, succeeded: 0, failed: 0, skipped: 0 };

    for await (const chunk of streamExcelChunks(tempFilePath, 500)) {
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
            const { summary, uploadBatch } = await processExcelFile(tempFilePath, source);

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
    const { uploadId, source } = req.body || {};
    const filePath = uploadId ? chunkFilePath(uploadId) : null;
    try {
        if (!uploadId || !filePath) return res.status(400).json({ success: false, message: 'uploadId is required' });
        if (!source) return res.status(400).json({ success: false, message: 'source is required' });
        if (!fs.existsSync(filePath)) {
            return res.status(404).json({ success: false, message: 'No uploaded chunks found for this uploadId' });
        }

        const { summary, uploadBatch } = await processExcelFile(filePath, source);

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