const multer = require('multer');
const XLSX = require('xlsx');
const Lead = require('../models/leadModel');

// ============================================================================
// MULTER CONFIG — memory storage (no disk writes)
// ============================================================================

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 }, // 10 MB
    fileFilter: (req, file, cb) => {
        const allowed = [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
        ];
        if (
            allowed.includes(file.mimetype) ||
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
// Normalises any reasonable header variant to our model field name.
// ============================================================================

const HEADER_MAP = {
    // fullName
    fullname: 'fullName',
    full_name: 'fullName',
    name: 'fullName',
    'full name': 'fullName',

    // firstName
    firstname: 'firstName',
    first_name: 'firstName',
    'first name': 'firstName',

    // lastName
    lastname: 'lastName',
    last_name: 'lastName',
    'last name': 'lastName',

    // phone
    phone: 'phone',
    mobile: 'phone',
    contact: 'phone',
    phonenumber: 'phone',
    phone_number: 'phone',
    'phone number': 'phone',
    'mobile number': 'phone',

    // email
    email: 'email',
    'email address': 'email',
    emailaddress: 'email',

    // source
    source: 'source',
    leadsource: 'source',
    lead_source: 'source',
    'lead source': 'source',

    // panNumber
    pannumber: 'panNumber',
    pan_number: 'panNumber',
    pan: 'panNumber',
    'pan number': 'panNumber',
    'pan no': 'panNumber',

    // age
    age: 'age',

    // dateOfBirth
    dateofbirth: 'dateOfBirth',
    date_of_birth: 'dateOfBirth',
    dob: 'dateOfBirth',
    'date of birth': 'dateOfBirth',
    birthdate: 'dateOfBirth',

    // gender
    gender: 'gender',
    sex: 'gender',

    // jobType
    jobtype: 'jobType',
    job_type: 'jobType',
    'job type': 'jobType',
    occupation: 'jobType',
    employment: 'jobType',

    // businessType
    businesstype: 'businessType',
    business_type: 'businessType',
    'business type': 'businessType',
    business: 'businessType',

    // salary
    salary: 'salary',
    income: 'salary',
    'monthly salary': 'salary',
    'monthly income': 'salary',

    // creditScore
    creditscore: 'creditScore',
    credit_score: 'creditScore',
    'credit score': 'creditScore',

    // cibilScore
    cibilscore: 'cibilScore',
    cibil_score: 'cibilScore',
    cibil: 'cibilScore',
    'cibil score': 'cibilScore',

    // address
    address: 'address',
    'full address': 'address',

    // pincode
    pincode: 'pincode',
    pin: 'pincode',
    'pin code': 'pincode',
    'zip code': 'pincode',
    zipcode: 'pincode',

    // consent
    consent: 'consent',
};

// ============================================================================
// HELPERS
// ============================================================================

function normaliseHeader(raw) {
    return String(raw).trim().toLowerCase().replace(/\s+/g, ' ');
}

function parseBoolean(value) {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    const s = String(value).trim().toLowerCase();
    return s === 'true' || s === '1' || s === 'yes' || s === 'y';
}

function parseDateCell(value) {
    if (!value) return null;
    // XLSX stores dates as serial numbers
    if (typeof value === 'number') {
        const d = XLSX.SSF.parse_date_code(value);
        if (d) {
            const month = String(d.m).padStart(2, '0');
            const day = String(d.d).padStart(2, '0');
            return `${d.y}-${month}-${day}`;
        }
    }
    const s = String(value).trim();
    if (!s) return null;
    const parsed = new Date(s);
    return isNaN(parsed.getTime()) ? null : parsed.toISOString().split('T')[0];
}

/**
 * Convert a raw xlsx row object (keyed by column headers) into a
 * normalised lead data object, mapping flexible header names to model fields.
 */
function rowToLeadData(rawRow) {
    const lead = {};

    for (const [rawKey, cellValue] of Object.entries(rawRow)) {
        const normKey = normaliseHeader(rawKey);
        const fieldName = HEADER_MAP[normKey];
        if (!fieldName || cellValue === undefined || cellValue === null || cellValue === '') {
            continue;
        }

        if (fieldName === 'consent') {
            lead[fieldName] = parseBoolean(cellValue);
        } else if (fieldName === 'dateOfBirth') {
            lead[fieldName] = parseDateCell(cellValue);
        } else if (fieldName === 'age' || fieldName === 'salary' || fieldName === 'creditScore' || fieldName === 'cibilScore') {
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
// CONTROLLER METHODS
// ============================================================================

/**
 * POST /api/leads/bulk-upload
 *
 * Accepts a multipart form with a single file field named "file".
 * Returns a detailed result object:
 * {
 *   total, succeeded, failed, skipped,
 *   results: [{ row, status, leadId?, error?, data }]
 * }
 */
const bulkUpload = async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, message: 'No file uploaded' });
        }

        // Parse xlsx from memory buffer
        const workbook = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: false });
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) {
            return res.status(400).json({ success: false, message: 'No sheets found in the file' });
        }

        const sheet = workbook.Sheets[sheetName];
        const rawRows = XLSX.utils.sheet_to_json(sheet, { defval: '' });

        if (!rawRows || rawRows.length === 0) {
            return res.status(400).json({ success: false, message: 'Sheet is empty or has no data rows' });
        }

        const MAX_ROWS = 5000;
        if (rawRows.length > MAX_ROWS) {
            return res.status(400).json({
                success: false,
                message: `File contains ${rawRows.length} rows. Maximum allowed is ${MAX_ROWS}.`,
            });
        }

        const results = [];
        let succeeded = 0;
        let failed = 0;
        let skipped = 0;

        // Process rows sequentially to avoid DynamoDB hot-partition issues.
        // For massive volumes, switch to a controlled-concurrency batch (e.g. p-limit).
        for (let i = 0; i < rawRows.length; i++) {
            const rawRow = rawRows[i];
            const rowNum = i + 2; // 1-indexed, +1 for header row
            const leadData = rowToLeadData(rawRow);

            // Skip entirely blank rows
            if (Object.keys(leadData).length === 0) {
                skipped++;
                results.push({ row: rowNum, status: 'skipped', reason: 'Empty row', data: rawRow });
                continue;
            }

            try {
                const created = await Lead.create(leadData);
                succeeded++;
                results.push({ row: rowNum, status: 'success', leadId: created.leadId, data: leadData });
            } catch (err) {
                failed++;
                const errorEntry = {
                    row: rowNum,
                    status: 'failed',
                    error: err.message,
                    data: leadData,
                };
                if (err.errors) errorEntry.validationErrors = err.errors;
                if (err.code) errorEntry.code = err.code;
                results.push(errorEntry);
            }
        }

        return res.status(200).json({
            success: true,
            message: `Bulk upload complete. ${succeeded} succeeded, ${failed} failed, ${skipped} skipped.`,
            summary: {
                total: rawRows.length,
                succeeded,
                failed,
                skipped,
            },
            results,
        });
    } catch (err) {
        console.error('[BulkUpload] Unexpected error:', err);
        return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
    }
};

/**
 * GET /api/leads/bulk-upload/template
 * Returns a pre-built xlsx template the user can download and fill in.
 */
const downloadTemplate = (req, res) => {
    const headers = [
        'source', 'fullName', 'firstName', 'lastName',
        'phone', 'email', 'panNumber',
        'dateOfBirth', 'age', 'gender',
        'jobType', 'businessType', 'salary',
        'creditScore', 'cibilScore',
        'address', 'pincode', 'consent',
    ];

    const sampleRow = {
        source: 'website',
        fullName: 'Ramesh Kumar',
        firstName: 'Ramesh',
        lastName: 'Kumar',
        phone: '9876543210',
        email: 'ramesh.kumar@example.com',
        panNumber: 'ABCDE1234F',
        dateOfBirth: '1990-05-15',
        age: 34,
        gender: 'Male',
        jobType: 'Salaried',
        businessType: '',
        salary: 55000,
        creditScore: 720,
        cibilScore: '',
        address: '123 MG Road, Bengaluru',
        pincode: '560001',
        consent: true,
    };

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet([sampleRow], { header: headers });

    // Column widths for readability
    ws['!cols'] = headers.map(h => ({ wch: Math.max(h.length + 4, 18) }));

    XLSX.utils.book_append_sheet(wb, ws, 'Leads');

    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Disposition', 'attachment; filename="leads_template.xlsx"');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buffer);
};

module.exports = { upload, bulkUpload, downloadTemplate };