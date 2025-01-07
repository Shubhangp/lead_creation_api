const mongoose = require('mongoose');

const apiKeySchema = new mongoose.Schema({
  sourceName: {
    type: String,
    required: true,
    unique: true,
  },
  apiKey: {
    type: String,
    required: true,
    unique: true,
  },
});

module.exports = mongoose.model('ApiKey', apiKeySchema);