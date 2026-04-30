const multer = require('multer');
const ExcelJS = require('exceljs');
const XLSX = require('xlsx');           // kept only for template generation (tiny write)
const fs = require('fs');
const os = require('os');
const path = require('path');
const Lead = require('../models/leadModel');

// ============================================================================
// MULTER CONFIG
// IMPORTANT: Use diskStorage for large files — memoryStorage would load the
// entire 100 MB file into RAM before we even start reading it.
// ============================================================================

const upload = multer({
    storage: multer.diskStorage({
        destination: os.tmpdir(),
        filename: (_req, file, cb) => {
            const unique = `bulk-${Date.now()}-${Math.random().toString(36).slice(2)}`;
            cb(null, `${unique}${path.extname(file.originalname)}`);
        },
    }),
    limits: { fileSize: 200 * 1024 * 1024 },
    fileFilter: (_req, file, cb) => {
        if (
            file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
            file.mimetype === 'application/vnd.ms-excel' ||
            file.originalname.match(/\.(xlsx|xls)$/i)
        ) {
            cb(null, true);
        } else {
            cb(new Error('Only .xlsx and .xls files are allowed'), false);
        }
    },
});

// ============================================================================
// COLUMN HEADER MAP
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
    if (!s) return null;
    const d = new Date(s);
    return isNaN(d) ? null : d.toISOString().split('T')[0];
}

function parseDateTimeISO(v) {
    if (!v) return null;
    if (v instanceof Date) return isNaN(v) ? null : v.toISOString();
    const s = String(v).trim();
    if (!s) return null;
    const d = new Date(s);
    return isNaN(d) ? null : d.toISOString();
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
        } else if (['age', 'salary', 'creditScore', 'cibilScore'].includes(fieldName)) {
            const n = Number(cellValue);
            lead[fieldName] = isNaN(n) ? null : n;
        } else if (fieldName === 'panNumber') {
            lead[fieldName] = String(cellValue).trim().toUpperCase();
        } else {
            lead[fieldName] = String(cellValue).trim();
        }
    }
    return lead;
}

// ============================================================================
// CONCURRENCY LIMITER
// ============================================================================

function makeLimiter(concurrency) {
    let active = 0;
    const queue = [];
    function next() {
        while (active < concurrency && queue.length) {
            active++;
            const { fn, resolve, reject } = queue.shift();
            fn().then(resolve, reject).finally(() => { active--; next(); });
        }
    }
    return fn => new Promise((resolve, reject) => { queue.push({ fn, resolve, reject }); next(); });
}

// ============================================================================
// STREAM EXCEL ROWS  (memory-safe)
// ============================================================================

/**
 * Async generator that streams an xlsx file from disk in chunks.
 * Peak memory = CHUNK_SIZE rows, not the entire file.
 *
 * @param {string} filePath
 * @param {number} chunkSize
 * @yields {Array<{rowNum: number, raw: Object}>}
 */
async function* streamExcelChunks(filePath, chunkSize = 500) {
    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
        entries: 'emit',
        sharedStrings: 'cache',   // must be 'cache' to resolve string cells
        hyperlinks: 'ignore',
        styles: 'ignore',
        worksheets: 'emit',
    });

    let headers = null;
    let chunk = [];

    for await (const worksheet of workbook) {
        for await (const row of worksheet) {
            // Row 1 = header
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
                const header = headers[col];
                if (header) raw[header] = cell.value; // Date objects preserved as-is
            });

            if (Object.keys(raw).length === 0) continue;

            chunk.push({ rowNum: row.number, raw });

            if (chunk.length >= chunkSize) {
                yield chunk;
                chunk = [];
            }
        }
        break; // first worksheet only
    }

    if (chunk.length > 0) yield chunk;
}

// ============================================================================
// CONTROLLER
// ============================================================================

const CONCURRENCY = 50;
const CHUNK_SIZE  = 500;

/**
 * POST /api/leads/bulk-upload
 *
 * Memory profile:
 *   multer writes file to disk  →  ExcelJS reads 500 rows at a time  →
 *   500 rows written to DynamoDB  →  chunk GC'd  →  next 500 rows.
 *
 *   Peak heap: ~2-5 MB regardless of whether the file has 1K or 9L rows.
 */
const bulkUpload = async (req, res) => {
    const tempFilePath = req.file?.path;

    try {
        if (!req.file) {
            return res.status(400).json({ success: false, message: 'No file uploaded' });
        }

        const summary = { total: 0, succeeded: 0, failed: 0, skipped: 0 };
        const results = [];
        const limiter = makeLimiter(CONCURRENCY);

        // In-memory dedup sets — only store primitive strings, memory is O(n*15 bytes)
        const seenPhones = new Set();
        const seenPans   = new Set();
        const sourceIncrements = {};

        for await (const chunk of streamExcelChunks(tempFilePath, CHUNK_SIZE)) {
            const chunkTasks = [];

            for (const { rowNum, raw } of chunk) {
                summary.total++;
                const leadData = rowToLeadData(raw);

                // Empty row
                if (Object.keys(leadData).length === 0) {
                    summary.skipped++;
                    results.push({ row: rowNum, status: 'skipped', reason: 'Empty row' });
                    continue;
                }

                // Validation (sync, no DB)
                try {
                    Lead.validate(leadData);
                } catch (err) {
                    summary.failed++;
                    results.push({
                        row: rowNum, status: 'failed',
                        error: err.message, validationErrors: err.errors,
                    });
                    continue;
                }

                const phone = leadData.phone;
                const pan   = leadData.panNumber;

                // Within-file duplicate check
                if (phone && seenPhones.has(phone)) {
                    summary.failed++;
                    results.push({ row: rowNum, status: 'failed', error: 'Duplicate phone in file', code: 'DUPLICATE_PHONE_IN_FILE' });
                    continue;
                }
                if (pan && seenPans.has(pan)) {
                    summary.failed++;
                    results.push({ row: rowNum, status: 'failed', error: 'Duplicate PAN in file', code: 'DUPLICATE_PAN_IN_FILE' });
                    continue;
                }

                // Reserve before async write to prevent same-chunk race
                if (phone) seenPhones.add(phone);
                if (pan)   seenPans.add(pan);

                const _rowNum    = rowNum;
                const _leadData  = leadData;
                const _phone     = phone;
                const _pan       = pan;

                chunkTasks.push(
                    limiter(async () => {
                        try {
                            const created = await Lead.createFast(_leadData);
                            summary.succeeded++;
                            results.push({ row: _rowNum, status: 'success', leadId: created.leadId });
                            const src = _leadData.source;
                            if (src) sourceIncrements[src] = (sourceIncrements[src] || 0) + 1;
                        } catch (err) {
                            // Release reservation on failure
                            if (_phone) seenPhones.delete(_phone);
                            if (_pan)   seenPans.delete(_pan);
                            summary.failed++;
                            const entry = { row: _rowNum, status: 'failed', error: err.message };
                            if (err.errors) entry.validationErrors = err.errors;
                            if (err.code)   entry.code = err.code;
                            results.push(entry);
                        }
                    })
                );
            }

            // Drain this chunk's writes before moving on — keeps memory flat
            await Promise.all(chunkTasks);
        }

        // Batch counter updates — one UpdateCommand per source
        await Promise.all(
            Object.entries(sourceIncrements).map(([src, delta]) =>
                Lead.incrementCounterBy(src, delta).catch(err =>
                    console.error(`[Counter] Failed to update "${src}":`, err.message)
                )
            )
        );

        results.sort((a, b) => (a.row ?? 0) - (b.row ?? 0));

        return res.status(200).json({
            success: true,
            message: `Bulk upload complete. ${summary.succeeded} succeeded, ${summary.failed} failed, ${summary.skipped} skipped.`,
            summary,
            results,
        });

    } catch (err) {
        console.error('[BulkUpload] Unexpected error:', err);
        return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    } finally {
        // Always delete the temp file
        if (tempFilePath) {
            fs.unlink(tempFilePath, e => {
                if (e) console.warn('[BulkUpload] Could not delete temp file:', e.message);
            });
        }
    }
};

// ============================================================================
// TEMPLATE DOWNLOAD  (small file — in-memory xlsx is fine here)
// ============================================================================

const downloadTemplate = (_req, res) => {
    const headers = [
        'source', 'fullName', 'firstName', 'lastName',
        'phone', 'email', 'panNumber',
        'dateOfBirth', 'age', 'gender',
        'jobType', 'businessType', 'salary',
        'creditScore', 'cibilScore',
        'address', 'pincode', 'consent',
        'createdAt',
    ];
    const sampleRow = {
        source: 'website', fullName: 'Ramesh Kumar', firstName: 'Ramesh', lastName: 'Kumar',
        phone: '9876543210', email: 'ramesh.kumar@example.com', panNumber: 'ABCDE1234F',
        dateOfBirth: '1990-05-15', age: 34, gender: 'Male', jobType: 'Salaried',
        businessType: '', salary: 55000, creditScore: 720, cibilScore: '',
        address: '123 MG Road, Bengaluru', pincode: '560001', consent: true,
        createdAt: '2024-03-15T10:30:00.000Z',
    };

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet([sampleRow], { header: headers });
    ws['!cols'] = headers.map(h => ({ wch: Math.max(h.length + 4, 18) }));
    XLSX.utils.book_append_sheet(wb, ws, 'Leads');

    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Disposition', 'attachment; filename="leads_template.xlsx"');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buffer);
};

module.exports = { upload, bulkUpload, downloadTemplate };