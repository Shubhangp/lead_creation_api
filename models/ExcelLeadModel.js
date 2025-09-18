const mongoose = require('mongoose');

const excelLeadSchema = new mongoose.Schema({
    source: {
        type: String,
        required: true,
    },
    fullName: {
        type: String,
        required: true,
        minlength: 1,
        maxlength: 100,
    },
    firstName: {
        type: String,
        minlength: 1,
        maxlength: 50,
    },
    lastName: {
        type: String,
        minlength: 1,
        maxlength: 50,
    },
    phone: {
        type: String,
        required: true,
        validate: {
            validator: async function (phone) {
                // Define sources that should skip uniqueness check
                const sourcesWithoutUniquenessCheck = ['FREO_FEB'];
                
                // Skip uniqueness check for specific sources
                if (sourcesWithoutUniquenessCheck.includes(this.source)) {
                    return true;
                }

                const thirtyDaysAgo = new Date();
                thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

                const existingLead = await this.constructor.findOne({
                    phone: phone,
                    createdAt: { $gte: thirtyDaysAgo },
                    _id: { $ne: this._id }
                });

                return !existingLead;
            },
            message: 'Phone number already exists within the last 30 days.'
        }
    },
    email: {
        type: String,
        required: true,
        match: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}$/
    },
    age: {
        type: Number,
        min: 18,
        max: 120,
    },
    dateOfBirth: {
        type: Date,
        validate: {
            validator: function (value) {
                return value <= new Date();
            },
            message: 'Date of birth cannot be in the future.',
        },
    },
    gender: {
        type: String,
    },
    panNumber: {
        type: String,
        required: true,
        match: /^[A-Z]{5}[0-9]{4}[A-Z]$/,
        validate: {
            validator: async function (panNumber) {
                // Define sources that should skip uniqueness check
                const sourcesWithoutUniquenessCheck = ['FREO_FEB'];
                
                // Skip uniqueness check for specific sources
                if (sourcesWithoutUniquenessCheck.includes(this.source)) {
                    return true;
                }

                const thirtyDaysAgo = new Date();
                thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

                const existingLead = await this.constructor.findOne({
                    panNumber: panNumber,
                    createdAt: { $gte: thirtyDaysAgo },
                    _id: { $ne: this._id }
                });

                return !existingLead;
            },
            message: 'PAN number already exists within the last 30 days.'
        }
    },
    jobType: {
        type: String,
    },
    businessType: {
        type: String
    },
    salary: {
        type: String,
    },
    creditScore: {
        type: Number,
        min: 300,
        max: 900,
    },
    cibilScore: {
        type: Boolean,
    },
    address: {
        type: String
    },
    pincode: {
        type: String,
    },
    consent: {
        type: Boolean,
        required: true,
    },
    createdAt: {
        type: Date,
        default: Date.now,
    },
});

// Add indexes to optimize the uniqueness queries
excelLeadSchema.index({ phone: 1, createdAt: 1 });
excelLeadSchema.index({ panNumber: 1, createdAt: 1 });

// Updated static methods with source awareness
excelLeadSchema.statics.isPhoneUniqueInLast30Days = async function(phone, excludeId = null, source = null) {
  // Skip uniqueness check for specific sources
  const sourcesWithoutUniquenessCheck = ['FREO_FEB'];
  if (source && sourcesWithoutUniquenessCheck.includes(source)) {
    return true;
  }

  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  
  const query = {
    phone: phone,
    createdAt: { $gte: thirtyDaysAgo }
  };
  
  if (excludeId) {
    query._id = { $ne: excludeId };
  }
  
  const existingLead = await this.findOne(query);
  return !existingLead;
};

excelLeadSchema.statics.isPanUniqueInLast30Days = async function(panNumber, excludeId = null, source = null) {
  // Skip uniqueness check for specific sources
  const sourcesWithoutUniquenessCheck = ['FREO_FEB'];
  if (source && sourcesWithoutUniquenessCheck.includes(source)) {
    return true;
  }

  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  
  const query = {
    panNumber: panNumber,
    createdAt: { $gte: thirtyDaysAgo }
  };
  
  if (excludeId) {
    query._id = { $ne: excludeId };
  }
  
  const existingLead = await this.findOne(query);
  return !existingLead;
};

// Enhanced generic method with source awareness
excelLeadSchema.statics.isFieldUniqueInLast30Days = async function(fieldName, fieldValue, excludeId = null, source = null) {
  // Skip uniqueness check for specific sources
  const sourcesWithoutUniquenessCheck = ['FREO_FEB'];
  if (source && sourcesWithoutUniquenessCheck.includes(source)) {
    return true;
  }

  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  
  const query = {
    [fieldName]: fieldValue,
    createdAt: { $gte: thirtyDaysAgo }
  };
  
  if (excludeId) {
    query._id = { $ne: excludeId };
  }
  
  const existingLead = await this.findOne(query);
  return !existingLead;
};

// Alternative: Source-specific uniqueness (only check within same source)
excelLeadSchema.statics.isPhoneUniqueInSourceLast30Days = async function(phone, source, excludeId = null) {
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  
  const query = {
    phone: phone,
    source: source,
    createdAt: { $gte: thirtyDaysAgo }
  };
  
  if (excludeId) {
    query._id = { $ne: excludeId };
  }
  
  const existingLead = await this.findOne(query);
  return !existingLead;
};

module.exports = mongoose.model('ExcelLead', excelLeadSchema);