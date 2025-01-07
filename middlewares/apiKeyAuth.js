const ApiKey = require('../models/apiKeyModel');

// API Key Authentication Middleware
const apiKeyAuth = async (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  const source = req.body.source;

  if (!apiKey || !source) {
    return res.status(400).json({ message: 'API Key and Source are required.' });
  }

  try {
    const storedApiKey = await ApiKey.findOne({ sourceName: source });

    if (!storedApiKey) {
      return res.status(404).json({ message: 'Source not found or invalid API key.' });
    }

    if (apiKey !== storedApiKey.apiKey) {
      return res.status(403).json({ message: 'Invalid API key.' });
    }

    next();
  } catch (error) {
    console.error('Error validating API key:', error.message);
    res.status(500).json({ message: 'Server error while validating API key.' });
  }
};

module.exports = apiKeyAuth;