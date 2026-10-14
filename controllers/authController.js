const jwt    = require('jsonwebtoken');
const User   = require('../models/userModel');

const JWT_SECRET  = 'ratecut-super-secure-jwt-secret-key-here';
const JWT_EXPIRES = process.env.JWT_EXPIRES || '7d';

/**
 * Helper — issue a signed JWT
 */
function signToken(user) {
  return jwt.sign(
    {
      userId:     user.userId,
      email:      user.email,
      source:     user.source,
      lenderName: user.lenderName,
      role:       user.role,
    },
    JWT_SECRET,
    { expiresIn: JWT_EXPIRES }
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/auth/login
// Body: { email, password }
// ─────────────────────────────────────────────────────────────────────────────
async function login(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({
        success: false,
        message: 'Email and password are required.',
      });
    }

    // 1. Find user
    const user = await User.findByEmail(email);
    if (!user) {
      return res.status(401).json({ success: false, message: 'Invalid email or password.' });
    }

    // 2. Check active
    if (!user.isActive) {
      return res.status(401).json({ success: false, message: 'Account is deactivated. Contact support.' });
    }

    // 3. Verify password
    const valid = await User.verifyPassword(password, user.passwordHash);
    if (!valid) {
      return res.status(401).json({ success: false, message: 'Invalid email or password.' });
    }

    // 4. Issue token
    const safeUser = User.sanitize(user);
    const token    = signToken(safeUser);

    return res.status(200).json({
      success: true,
      message: 'Login successful.',
      token,
      user: safeUser,
    });
  } catch (err) {
    console.error('[auth/login]', err);
    return res.status(500).json({ success: false, message: 'Login failed. Please try again.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/auth/register   (admin only — protect this route in production)
// Body: { email, password, name, lenderName, source, role? }
// ─────────────────────────────────────────────────────────────────────────────
async function register(req, res) {
  try {
    const { email, password, name, lenderName, source, role } = req.body;

    if (!email || !password || !name || !lenderName || !source) {
      return res.status(400).json({
        success: false,
        message: 'email, password, name, lenderName, and source are all required.',
      });
    }

    if (password.length < 6) {
      return res.status(400).json({ success: false, message: 'Password must be at least 6 characters.' });
    }

    // Role guard: only a superadmin may create admin accounts; everyone else
    // can only create partner (lender) logins. Superadmins are never created via API.
    const requestedRole = role || 'lender';
    const allowedRoles  = req.user?.role === 'superadmin' ? ['lender', 'admin'] : ['lender'];
    if (!allowedRoles.includes(requestedRole)) {
      return res.status(403).json({
        success: false,
        message: `You are not allowed to create a user with role "${requestedRole}".`,
      });
    }

    const newUser = await User.create({ email, password, name, lenderName, source, role: requestedRole });

    return res.status(201).json({
      success: true,
      message: 'User registered successfully.',
      user: newUser,
    });
  } catch (err) {
    if (err.code === 'DUPLICATE_EMAIL') {
      return res.status(409).json({ success: false, message: err.message });
    }
    if (err.errors) {
      return res.status(400).json({ success: false, message: err.message, errors: err.errors });
    }
    console.error('[auth/register]', err);
    return res.status(500).json({ success: false, message: 'Registration failed.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/auth/users   (admin / superadmin only)
// ─────────────────────────────────────────────────────────────────────────────
async function listUsers(req, res) {
  try {
    const users = await User.listAll();
    return res.status(200).json({ success: true, users });
  } catch (err) {
    console.error('[auth/listUsers]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch users.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/auth/me    (requires authenticate middleware)
// ─────────────────────────────────────────────────────────────────────────────
async function me(req, res) {
  // req.user is already attached by authenticate middleware
  return res.status(200).json({
    success: true,
    user: req.user,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// PATCH /api/auth/password   (requires authenticate middleware)
// Body: { currentPassword, newPassword }
// ─────────────────────────────────────────────────────────────────────────────
async function updatePassword(req, res) {
  try {
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ success: false, message: 'currentPassword and newPassword are required.' });
    }
    if (newPassword.length < 6) {
      return res.status(400).json({ success: false, message: 'New password must be at least 6 characters.' });
    }

    // Re-fetch to get passwordHash (sanitize strips it)
    const user = await User.findById(req.user.userId);
    const valid = await User.verifyPassword(currentPassword, user.passwordHash);
    if (!valid) {
      return res.status(401).json({ success: false, message: 'Current password is incorrect.' });
    }

    const updated = await User.updatePassword(req.user.userId, newPassword);
    return res.status(200).json({ success: true, message: 'Password updated successfully.', user: updated });
  } catch (err) {
    console.error('[auth/updatePassword]', err);
    return res.status(500).json({ success: false, message: 'Password update failed.' });
  }
}

module.exports = { login, register, listUsers, me, updatePassword };