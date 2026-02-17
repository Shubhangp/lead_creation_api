const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');

const TABLE_NAME = 'lender_users';

/**
 * Lender User Model
 *
 * DynamoDB Table: lender_users
 * Primary Key  : userId (String)
 * GSI 1        : email-index          → email (PK)
 * GSI 2        : source-index         → source (PK)
 *
 * Item shape:
 * {
 *   userId       : uuid
 *   email        : string (unique)
 *   passwordHash : string (bcrypt)
 *   name         : string            — display name  e.g. "Arjun Mehta"
 *   lenderName   : string            — brand name    e.g. "CashKuber"
 *   source       : string            — must match leads.source / lead_success.source
 *   role         : string            — "lender" | "admin"
 *   isActive     : boolean
 *   createdAt    : ISO string
 *   updatedAt    : ISO string
 * }
 */
class User {

  // ──────────────────────────────────────────────────────────────
  // VALIDATION
  // ──────────────────────────────────────────────────────────────

  static validate(data) {
    const errors = [];

    if (!data.email)      errors.push('Email is required');
    if (!data.source)     errors.push('Source is required');
    if (!data.lenderName) errors.push('Lender name is required');
    if (!data.name)       errors.push('Name is required');

    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    if (data.email && !emailRegex.test(data.email)) {
      errors.push('Invalid email format');
    }

    if (errors.length > 0) {
      const err = new Error('Validation failed');
      err.errors = errors;
      throw err;
    }
  }

  // ──────────────────────────────────────────────────────────────
  // CREATE
  // ──────────────────────────────────────────────────────────────

  /**
   * Register a new lender user.
   * @param {Object} userData  - { email, password, name, lenderName, source, role? }
   */
  static async create(userData) {
    this.validate(userData);

    // Prevent duplicate emails
    const existing = await this.findByEmail(userData.email);
    if (existing) {
      const err = new Error('Email already registered');
      err.code = 'DUPLICATE_EMAIL';
      throw err;
    }

    const saltRounds = 10;
    const passwordHash = await bcrypt.hash(userData.password, saltRounds);
    const now = new Date().toISOString();

    const item = {
      userId:       uuidv4(),
      email:        userData.email.toLowerCase().trim(),
      passwordHash,
      name:         userData.name,
      lenderName:   userData.lenderName,
      source:       userData.source,           // ties user to leads data
      role:         userData.role || 'lender',
      isActive:     true,
      createdAt:    now,
      updatedAt:    now,
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item,
    }));

    const { passwordHash: _omit, ...safeItem } = item;
    return safeItem;
  }

  // ──────────────────────────────────────────────────────────────
  // FIND
  // ──────────────────────────────────────────────────────────────

  static async findById(userId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { userId },
    }));
    return result.Item || null;
  }

  static async findByEmail(email) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'email-index',
      KeyConditionExpression: 'email = :email',
      ExpressionAttributeValues: { ':email': email.toLowerCase().trim() },
      Limit: 1,
    }));
    return result.Items?.[0] || null;
  }

  static async findBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-index',
      KeyConditionExpression: '#src = :source',
      ExpressionAttributeNames: { '#src': 'source' },
      ExpressionAttributeValues: { ':source': source },
    }));
    return result.Items || [];
  }

  // ──────────────────────────────────────────────────────────────
  // AUTH HELPERS
  // ──────────────────────────────────────────────────────────────

  /**
   * Verify a plain-text password against stored hash.
   */
  static async verifyPassword(plainPassword, passwordHash) {
    return bcrypt.compare(plainPassword, passwordHash);
  }

  /**
   * Returns the user record stripped of sensitive fields.
   */
  static sanitize(user) {
    if (!user) return null;
    const { passwordHash, ...safe } = user;
    return safe;
  }

  // ──────────────────────────────────────────────────────────────
  // UPDATE
  // ──────────────────────────────────────────────────────────────

  static async updatePassword(userId, newPassword) {
    const saltRounds = 10;
    const passwordHash = await bcrypt.hash(newPassword, saltRounds);
    const now = new Date().toISOString();

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { userId },
      UpdateExpression: 'SET passwordHash = :hash, updatedAt = :now',
      ExpressionAttributeValues: {
        ':hash': passwordHash,
        ':now':  now,
      },
      ReturnValues: 'ALL_NEW',
    }));

    return User.sanitize(result.Attributes);
  }

  static async deactivate(userId) {
    const now = new Date().toISOString();
    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { userId },
      UpdateExpression: 'SET isActive = :false, updatedAt = :now',
      ExpressionAttributeValues: {
        ':false': false,
        ':now':   now,
      },
      ReturnValues: 'ALL_NEW',
    }));
    return User.sanitize(result.Attributes);
  }
}

module.exports = User;