const mongoose = require('mongoose');

const leadSchema = new mongoose.Schema({
    source: {
        type: String,
        required: true,
    },
    fullName: {
        type: String,
        required: true,
        minlength: 2,
        maxlength: 100,
    },
    firstName: {
        type: String,
        minlength: 2,
        maxlength: 50,
    },
    lastName: {
        type: String,
        minlength: 2,
        maxlength: 50,
    },
    phone: {
        type: String,
        required: true,
    },
    email: {
        type: String,
        required: true,
        match: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/
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
        unique: true,
        match: /^[A-Z]{5}[0-9]{4}[A-Z]$/,
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
        max: 850,
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

module.exports = mongoose.model('Lead', leadSchema);