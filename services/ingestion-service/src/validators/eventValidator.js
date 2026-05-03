const Ajv = require('ajv');
const addFormats = require('ajv-formats');
const { schemas } = require('@conduit/shared');
const eventSchema = schemas.event;

const ajv = new Ajv({ allErrors: true, strict: true });
addFormats(ajv);

const validate = ajv.compile(eventSchema);

/**
 * Validates an event payload against the Conduit event schema.
 * @returns {{ valid: boolean, errors: object[] | null }}
 */
module.exports = function validateEvent(data) {
  const valid = validate(data);
  return { valid, errors: validate.errors };
};
