const CryptoJS = require('crypto-js');
const LenderRequest = require('../models/lenderRequestModel');

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

exports.createLenderRequest = async (req, res) => {
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

    // Validate decrypted data structure
    if (!formData.companyName || !formData.email || !formData.contactNumber) {
      return res.status(400).json({
        error: 'Missing required fields',
        message: 'Company name, email, and contact number are required'
      });
    }

    // Prepare lender request data
    const lenderRequestData = {
      companyName: formData.companyName,
      website: formData.website || '',
      email: formData.email,
      contactNumber: formData.contactNumber
    };

    // Store in DynamoDB
    let savedRequest;
    try {
      savedRequest = await LenderRequest.create(lenderRequestData);
    } catch (dbError) {
      // Handle duplicate email error
      if (dbError.code === 'DUPLICATE_EMAIL') {
        return res.status(409).json({
          error: 'Duplicate entry',
          message: 'A request with this email already exists'
        });
      }

      // Handle validation errors
      if (dbError.errors) {
        return res.status(400).json({
          error: 'Validation failed',
          message: dbError.errors
        });
      }

      // Re-throw other database errors
      throw dbError;
    }

    res.status(201).json({
      message: 'Lender request submitted successfully',
      data: {
        lenderRequestId: savedRequest.lenderRequestId,
        companyName: savedRequest.companyName,
        email: savedRequest.email,
        status: savedRequest.status,
        createdAt: savedRequest.createdAt
      }
    });

  } catch (err) {
    console.error('Error submitting lender request:', err.message);

    // Handle different error scenarios
    if (err.response) {
      // External API error
      res.status(err.response.status || 500).json({
        error: 'Failed to submit lender request',
        message: err.response.data?.message || 'External API error'
      });
    } else {
      // Internal server error
      res.status(500).json({
        error: 'Failed to submit lender request',
        message: 'Internal server error'
      });
    }
  }
};

// Get lender request by ID
exports.getLenderRequestById = async (req, res) => {
  try {
    const { lenderRequestId } = req.params;

    const lenderRequest = await LenderRequest.findById(lenderRequestId);

    if (!lenderRequest) {
      return res.status(404).json({
        error: 'Not found',
        message: 'Lender request not found'
      });
    }

    res.status(200).json({
      message: 'Lender request retrieved successfully',
      data: lenderRequest
    });

  } catch (err) {
    console.error('Error retrieving lender request:', err.message);
    res.status(500).json({
      error: 'Failed to retrieve lender request',
      message: 'Internal server error'
    });
  }
};

// Get all lender requests (paginated)
exports.getAllLenderRequests = async (req, res) => {
  try {
    const { limit, lastEvaluatedKey } = req.query;

    const options = {};
    if (limit) options.limit = parseInt(limit);
    if (lastEvaluatedKey) options.lastEvaluatedKey = JSON.parse(lastEvaluatedKey);

    const result = await LenderRequest.findAll(options);

    res.status(200).json({
      message: 'Lender requests retrieved successfully',
      data: result.items,
      lastEvaluatedKey: result.lastEvaluatedKey
    });

  } catch (err) {
    console.error('Error retrieving lender requests:', err.message);
    res.status(500).json({
      error: 'Failed to retrieve lender requests',
      message: 'Internal server error'
    });
  }
};

// Update lender request status
exports.updateLenderRequestStatus = async (req, res) => {
  try {
    const { lenderRequestId } = req.params;
    const { status } = req.body;

    if (!status) {
      return res.status(400).json({
        error: 'Status is required'
      });
    }

    if (!['pending', 'approved', 'rejected'].includes(status)) {
      return res.status(400).json({
        error: 'Invalid status',
        message: 'Status must be: pending, approved, or rejected'
      });
    }

    const updatedRequest = await LenderRequest.updateStatus(lenderRequestId, status);

    res.status(200).json({
      message: 'Lender request status updated successfully',
      data: updatedRequest
    });

  } catch (err) {
    console.error('Error updating lender request status:', err.message);

    if (err.message === 'Lender request not found') {
      return res.status(404).json({
        error: 'Not found',
        message: err.message
      });
    }

    res.status(500).json({
      error: 'Failed to update lender request status',
      message: 'Internal server error'
    });
  }
};

// Get lender requests by status
exports.getLenderRequestsByStatus = async (req, res) => {
  try {
    const { status } = req.params;
    const { limit } = req.query;

    if (!['pending', 'approved', 'rejected'].includes(status)) {
      return res.status(400).json({
        error: 'Invalid status',
        message: 'Status must be: pending, approved, or rejected'
      });
    }

    const options = {};
    if (limit) options.limit = parseInt(limit);

    const requests = await LenderRequest.findByStatus(status, options);

    res.status(200).json({
      message: 'Lender requests retrieved successfully',
      data: requests
    });

  } catch (err) {
    console.error('Error retrieving lender requests by status:', err.message);
    res.status(500).json({
      error: 'Failed to retrieve lender requests',
      message: 'Internal server error'
    });
  }
};

// Delete lender request
exports.deleteLenderRequest = async (req, res) => {
  try {
    const { lenderRequestId } = req.params;

    await LenderRequest.deleteById(lenderRequestId);

    res.status(200).json({
      message: 'Lender request deleted successfully'
    });

  } catch (err) {
    console.error('Error deleting lender request:', err.message);
    res.status(500).json({
      error: 'Failed to delete lender request',
      message: 'Internal server error'
    });
  }
};