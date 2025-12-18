const axios = require('axios');
const CryptoJS = require('crypto-js');
const ApiKey = require('../models/apiKeyModel');

// IMPORTANT: Store this securely - ideally in environment variables
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY || 'your-32-character-secret-key-here';

// Decryption function
const decryptData = (encryptedData) => {
  try {
    const bytes = CryptoJS.AES.decrypt(encryptedData, ENCRYPTION_KEY);
    const decryptedString = bytes.toString(CryptoJS.enc.Utf8);
    
    if (!decryptedString) {
      throw new Error('Decryption failed - invalid key or corrupted data');
    }
    
    return JSON.parse(decryptedString);
  } catch (error) {
    console.error('Decryption error:', error.message);
    throw new Error('Failed to decrypt data');
  }
};

exports.createLead = async (req, res) => {
  try {
    const { encryptedData, source } = req.body;

    // Validate required fields
    if (!encryptedData) {
      return res.status(400).json({ error: 'Encrypted data is required' });
    }

    if (!source) {
      return res.status(400).json({ error: 'Source is required' });
    }

    // Decrypt the form data
    let formData;
    try {
      formData = decryptData(encryptedData);
    } catch (decryptError) {
      return res.status(400).json({ 
        error: 'Invalid encrypted data',
        message: decryptError.message 
      });
    }

    // Verify source matches
    if (formData.source !== source) {
      return res.status(400).json({ 
        error: 'Source mismatch between encrypted data and request' 
      });
    }

    // Fetch API key for the source
    const storedApiKey = await ApiKey.findBySourceName(source);

    if (!storedApiKey) {
      return res.status(404).json({ error: 'Invalid source' });
    }

    // Submit to external API
    const response = await axios.post(
      'https://lead.ratecut.in/api/v1/leads', 
      formData, 
      {
        headers: {
          'x-api-key': storedApiKey.apiKey,
          'Content-Type': 'application/json',
        },
      }
    );

    res.status(200).json({
      message: 'Lead submitted successfully',
      // data: response.data,
    });
  } catch (err) {
    console.error('Error submitting lead:', err.response?.data || err.message);
    
    // Handle different error scenarios
    if (err.response) {
      // External API error
      res.status(err.response.status || 500).json({ 
        error: 'Failed to submit lead',
        message: err.response.data?.message || 'External API error'
      });
    } else {
      // Internal server error
      res.status(500).json({ 
        error: 'Failed to submit lead',
        message: 'Internal server error'
      });
    }
  }
};