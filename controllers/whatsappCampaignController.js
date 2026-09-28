'use strict';

// controllers/whatsappCampaignController.js

const path = require('path');
const csv  = require('csv-parse/sync');
const XLSX = require('xlsx');
const { v4: uuidv4 } = require('uuid');

const WhatsappCampaign = require('../models/whatsappCampaignModel');
const { LEAD_SOURCES_DEFAULT } = require('../config/registry');
const { isS3Configured, uploadFileToS3, getPresignedDownloadUrl } = require('../utils/s3Upload');

const PHONE_COLUMN_CANDIDATES = [
  'phone_number', 'phonenumber', 'phone', 'mobile', 'mobile_number', 'mobilenumber', 'phonenumber',
];

// Parse a csv/xls/xlsx buffer into an array of row objects.
function parseFile(buffer, originalFilename) {
  const ext = path.extname(originalFilename).toLowerCase();

  if (ext === '.csv') {
    const content = buffer.toString('utf-8');
    return csv.parse(content, { columns: true, skip_empty_lines: true, trim: true });
  }

  const workbook = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const sheetName = workbook.SheetNames[0];
  return XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { defval: null });
}

// Case-insensitive match against common phone-number header names.
function findPhoneColumn(rows) {
  if (!rows.length) return null;
  const headers = Object.keys(rows[0]);
  return headers.find(h => PHONE_COLUMN_CANDIDATES.includes(h.toLowerCase().trim())) || null;
}

// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/v1/whatsapp-campaigns/partners
 * Superadmin only.
 */
exports.getPartners = async (req, res) => {
  try {
    if (req.user.role !== 'superadmin') {
      return res.status(403).json({ success: false, error: 'Superadmin access required.' });
    }
    return res.json({ success: true, partners: LEAD_SOURCES_DEFAULT });
  } catch (err) {
    console.error('[getPartners] Error:', err);
    return res.status(500).json({ success: false, error: 'Failed to fetch partners.' });
  }
};

/**
 * POST /api/v1/whatsapp-campaigns/upload
 * multipart/form-data: file, source? (superadmin only)
 */
exports.uploadCampaign = async (req, res) => {
  try {
    const file = req.file;
    if (!file) {
      return res.status(400).json({ success: false, error: 'No file provided' });
    }

    let source;
    if (req.user.role === 'superadmin') {
      source = req.body.source;
      if (!source) {
        return res.status(400).json({ success: false, error: 'source is required for superadmin uploads' });
      }
    } else {
      source = req.user.source;
    }

    let rows;
    try {
      rows = parseFile(file.buffer, file.originalname);
    } catch (parseErr) {
      console.error('[uploadCampaign] Parse error:', parseErr);
      return res.status(400).json({ success: false, error: 'Could not parse file. Please check the format.' });
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ success: false, error: 'No rows found in file' });
    }

    const phoneColumn = findPhoneColumn(rows);
    if (!phoneColumn) {
      return res.status(400).json({ success: false, error: 'No phone number column found in file' });
    }

    const totalRecords = rows.length;

    const key = `whatsapp-campaigns/${source}/${uuidv4()}-${file.originalname}`;

    try {
      await uploadFileToS3(file.buffer, key, file.mimetype);
    } catch (s3Err) {
      console.error('[uploadCampaign] S3 upload error:', s3Err);
      const isNotConfigured = /not configured/i.test(s3Err.message || '');
      return res.status(isNotConfigured ? 503 : 500).json({ success: false, error: s3Err.message });
    }

    const campaign = await WhatsappCampaign.create({
      source,
      fileName:         file.originalname,
      fileKey:          key,
      fileSize:         file.size,
      totalRecords,
      messagesSent:     0,
      delivered:        0,
      ctr:              0,
      status:           'Completed',
      uploadedByUserId: req.user.userId,
      uploadedByName:   req.user.name,
    });

    return res.json({ success: true, campaign });
  } catch (err) {
    console.error('[uploadCampaign] Error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Upload failed' });
  }
};

/**
 * GET /api/v1/whatsapp-campaigns
 * Query: ?source=X (superadmin only, optional filter)
 */
exports.listCampaigns = async (req, res) => {
  try {
    const { role, source: userSource } = req.user;

    let campaigns;
    if (role === 'superadmin') {
      campaigns = req.query.source
        ? await WhatsappCampaign.findBySource(req.query.source)
        : await WhatsappCampaign.findAll();
    } else {
      campaigns = await WhatsappCampaign.findBySource(userSource);
    }

    return res.json({ success: true, campaigns });
  } catch (err) {
    console.error('[listCampaigns] Error:', err);
    return res.status(500).json({ success: false, error: 'Failed to fetch campaigns.' });
  }
};

/**
 * PATCH /api/v1/whatsapp-campaigns/:campaignId
 * Body: { messagesSent?, delivered?, ctr?, status? }
 */
exports.updateCampaign = async (req, res) => {
  try {
    const { campaignId } = req.params;
    const { role, source: userSource } = req.user;

    const existing = await WhatsappCampaign.findById(campaignId);
    if (!existing) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    if (role !== 'superadmin' && existing.source !== userSource) {
      return res.status(403).json({ success: false, error: 'Access denied.' });
    }

    const { messagesSent, delivered, ctr, status } = req.body || {};
    const updates = {};
    if (messagesSent !== undefined) updates.messagesSent = Number(messagesSent);
    if (delivered !== undefined)    updates.delivered    = Number(delivered);
    if (ctr !== undefined)          updates.ctr           = Number(ctr);
    if (status !== undefined)       updates.status        = status;

    const updated = await WhatsappCampaign.update(campaignId, updates);
    return res.json({ success: true, campaign: updated });
  } catch (err) {
    console.error('[updateCampaign] Error:', err);
    return res.status(500).json({ success: false, error: 'Failed to update campaign.' });
  }
};

/**
 * GET /api/v1/whatsapp-campaigns/:campaignId/download-url
 */
exports.getDownloadUrl = async (req, res) => {
  try {
    const { campaignId } = req.params;
    const { role, source: userSource } = req.user;

    const record = await WhatsappCampaign.findById(campaignId);
    if (!record) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    if (role !== 'superadmin' && record.source !== userSource) {
      return res.status(403).json({ success: false, error: 'Access denied.' });
    }

    if (!isS3Configured()) {
      return res.status(503).json({ success: false, error: 'S3 is not configured. Set AWS_S3_BUCKET_NAME in config.env.' });
    }

    const url = await getPresignedDownloadUrl(record.fileKey);
    return res.json({ success: true, url });
  } catch (err) {
    console.error('[getDownloadUrl] Error:', err);
    return res.status(500).json({ success: false, error: 'Failed to generate download URL.' });
  }
};
