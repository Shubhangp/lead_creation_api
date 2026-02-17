const jwt = require('jsonwebtoken');
const User = require('../models/userModel');

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) throw new Error('JWT_SECRET env variable is not set');

async function authenticate(req, res, next) {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        message: 'No token provided. Please login.',
      });
    }

    const token = authHeader.split(' ')[1];

    let decoded;
    try {
      decoded = jwt.verify(token, JWT_SECRET);
    } catch (err) {
      const message =
        err.name === 'TokenExpiredError'
          ? 'Session expired. Please login again.'
          : 'Invalid token. Please login.';
      return res.status(401).json({ success: false, message });
    }

    // Fetch fresh user from DB (catches deactivation after token issued)
    const user = await User.findById(decoded.userId);
    if (!user) {
      return res.status(401).json({ success: false, message: 'User not found.' });
    }
    if (!user.isActive) {
      return res.status(401).json({ success: false, message: 'Account is deactivated.' });
    }

    // Attach stripped user to request
    req.user = User.sanitize(user);
    next();
  } catch (err) {
    console.error('[auth] middleware error:', err);
    return res.status(500).json({ success: false, message: 'Authentication error.' });
  }
}

function requireAdmin(req, res, next) {
  if (req.user?.role !== 'admin') {
    return res.status(403).json({
      success: false,
      message: 'Admin access required.',
    });
  }
  next();
}

module.exports = { authenticate, requireAdmin };