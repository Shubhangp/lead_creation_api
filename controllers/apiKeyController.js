const ApiKey = require('../models/apiKeyModel');
const crypto = require('crypto');

exports.createApiKey = async (req, res) => {
  const { sourceName } = req.body;

  if (!sourceName) {
    return res.status(400).json({ message: 'Source name is required.' });
  }

  try {
    const existingApiKey = await ApiKey.findOne({ sourceName });

    if (existingApiKey) {
      return res.status(400).json({ message: 'API key already exists for this source.' });
    }

    const apiKey = crypto.randomBytes(32).toString('hex');

    const newApiKey = new ApiKey({
      sourceName,
      apiKey,
    });

    await newApiKey.save();

    res.status(201).json({
      status: 'success',
      data: { sourceName, apiKey },
    });
  } catch (error) {
    console.error('Error creating API key:', error.message);
    res.status(500).json({ message: 'Server error while creating API key.' });
  }
};