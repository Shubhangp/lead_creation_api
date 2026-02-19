const LeadSuccess = require('../models/leadSuccessModel');

exports.getLeadSuccessByLeadId = async (req, res) => {
  try {
    const { leadId } = req.params;

    if (!leadId) {
      return res.status(400).json({
        success: false,
        message: 'Lead ID is required'
      });
    }

    // Find the lead success record
    const successRecord = await LeadSuccess.findByLeadId(leadId);

    if (!successRecord) {
      return res.status(404).json({
        success: false,
        message: 'Lead success record not found'
      });
    }

    // Get all accepted lenders (lenders with true status)
    const lenderFields = [
      'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
      'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 
      'CRMPaisa', 'SML', 'MPOKKET'
    ];

    const acceptedLenders = lenderFields
      .filter(lender => successRecord[lender] === true)
      .map(lender => ({
        name: lender,
        status: 'accepted',
        acceptedAt: successRecord.createdAt
      }));

    // Return response in the format expected by the calling controller
    res.status(200).json({
      success: true,
      acceptedLenders: acceptedLenders,
      data: {
        successId: successRecord.successId,
        leadId: successRecord.leadId,
        source: successRecord.source,
        phone: successRecord.phone,
        email: successRecord.email,
        panNumber: successRecord.panNumber,
        fullName: successRecord.fullName,
        acceptedLenders: acceptedLenders,
        createdAt: successRecord.createdAt
      }
    });
  } catch (error) {
    console.error('Get lead success error:', error);
    res.status(500).json({
      success: false,
      message: 'Internal server error',
      error: error.message
    });
  }
};

exports.updateLenderStatus = async (req, res) => {
  try {
    const { leadId, lenderName } = req.params;
    const { status } = req.body;

    if (typeof status !== 'boolean') {
      return res.status(400).json({
        success: false,
        message: 'Status must be a boolean value'
      });
    }

    // Find the success record first
    const successRecord = await LeadSuccess.findByLeadId(leadId);

    if (!successRecord) {
      return res.status(404).json({
        success: false,
        message: 'Lead success record not found'
      });
    }

    // Update the lender status
    const updatedRecord = await LeadSuccess.updateLenderStatus(
      successRecord.successId,
      lenderName,
      status
    );

    res.status(200).json({
      success: true,
      message: 'Lender status updated successfully',
      data: updatedRecord
    });
  } catch (error) {
    console.error('Update lender status error:', error);
    
    if (error.message.includes('Invalid lender name')) {
      return res.status(400).json({
        success: false,
        message: error.message
      });
    }

    res.status(500).json({
      success: false,
      message: 'Internal server error',
      error: error.message
    });
  }
};