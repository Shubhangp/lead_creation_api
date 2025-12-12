const ApiKey = require('../models/apiKeyModel');
const ApiKeyUAT = require('../models/apiKeyUATModel');
const crypto = require('crypto');

exports.createApiKey = async (req, res) => {
  const { sourceName } = req.body;

  if (!sourceName) {
    return res.status(400).json({ message: 'Source name is required.' });
  }

  try {
    const existingApiKey = await ApiKey.findBySourceName(sourceName);
    if (existingApiKey) {
      return res.status(400).json({ message: 'API key already exists for this source.' });
    }
    const apiKey = crypto.randomBytes(32).toString('hex');
    const newApiKey = await ApiKey.create({
      sourceName,
      apiKey,
    });
    res.status(201).json({
      status: 'success',
      data: { sourceName, apiKey },
    });
  } catch (error) {
    console.error('Error creating API key:', error.message);
    res.status(500).json({ message: 'Server error while creating API key.' });
  }
};

exports.createApiKeyUAT = async (req, res) => {
  const { sourceName } = req.body;

  if (!sourceName) {
    return res.status(400).json({ message: 'Source name is required.' });
  }

  try {
    const existingApiKey = await ApiKeyUAT.findBySourceName(sourceName);
    if (existingApiKey) {
      return res.status(400).json({ message: 'API key already exists for this source.' });
    }

    const apiKey = crypto.randomBytes(32).toString('hex');
    const newApiKey = await ApiKeyUAT.create({
      sourceName,
      apiKey,
    });
    res.status(201).json({
      status: 'success',
      data: { sourceName, apiKey },
    });
  } catch (error) {
    console.error('Error creating API key:', error.message);
    res.status(500).json({ message: 'Server error while creating API key.' });
  }
};