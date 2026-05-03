const jwt = require('jsonwebtoken');
const { errors } = require('@conduit/shared');

const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

/**
 * JWT authentication middleware.
 * Validates the Bearer token and attaches decoded payload to req.user.
 */
module.exports = function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    throw new errors.UnauthorizedError('Missing or malformed Authorization header');
  }

  const token = authHeader.split(' ')[1];

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    req.headers['x-tenant-id'] = req.user.tenantId || 'default';
    next();
  } catch (err) {
    throw new errors.UnauthorizedError('Invalid or expired token');
  }
};
