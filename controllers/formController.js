const axios = require('axios');
const ApiKey = require('../models/apiKeyModel');

exports.createLead = async (req, res) => {
  const formData = req.body;
  const { source } = formData;

  if (!source) {
    return res.status(400).json({ error: 'Source is required' });
  }

  try {
    const storedApiKey = await ApiKey.findOne({ sourceName: source });

    if (!storedApiKey) {
      return res.status(404).json({ error: 'Invalid source' });
    }

    const response = await axios.post('https://lead.ratecut.in/api/v1/leads', formData, {
      headers: {
        'x-api-key': storedApiKey.apiKey,
        'Content-Type': 'application/json',
      },
    });

    res.status(200).json({
      message: 'Lead submitted successfully',
      data: response.data,
    });
  } catch (err) {
    console.error('Error submitting lead:', err.response?.data || err.message);
    res.status(500).json({ error: 'Failed to submit lead' });
  }
};