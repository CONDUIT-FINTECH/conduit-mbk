class AppError extends Error {
  constructor(message, statusCode = 500, code = 'INTERNAL_ERROR', details = null) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
    this.timestamp = new Date().toISOString();
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      error: this.code,
      message: this.message,
      details: this.details,
      timestamp: this.timestamp,
    };
  }
}

class ValidationError extends AppError {
  constructor(details) {
    super('Validation failed', 400, 'VALIDATION_ERROR', details);
  }
}

class DuplicateEventError extends AppError {
  constructor(existingEventId) {
    super('Duplicate event detected', 409, 'DUPLICATE_EVENT', { existingEventId });
  }
}

class UnauthorizedError extends AppError {
  constructor(message = 'Authentication required') {
    super(message, 401, 'UNAUTHORIZED');
  }
}

class RateLimitError extends AppError {
  constructor(retryAfter) {
    super('Rate limit exceeded', 429, 'RATE_LIMIT_EXCEEDED', { retryAfter });
  }
}

class ServiceUnavailableError extends AppError {
  constructor(service) {
    super(`Service unavailable: ${service}`, 503, 'SERVICE_UNAVAILABLE', { service });
  }
}

module.exports = {
  AppError,
  ValidationError,
  DuplicateEventError,
  UnauthorizedError,
  RateLimitError,
  ServiceUnavailableError,
};
