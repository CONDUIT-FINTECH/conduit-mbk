// @conduit/shared — barrel export
module.exports = {
  errors: require('./errors'),
  middleware: {
    correlationId: require('./middleware/correlationId'),
    errorHandler: require('./middleware/errorHandler'),
    requestLogger: require('./middleware/requestLogger'),
    healthCheck: require('./middleware/healthCheck'),
  },
  kafka: {
    ...require('./kafka/producer'),
    ...require('./kafka/consumer'),
    ...require('./kafka/singletonProducer'),
    TOPICS: require('./kafka/topics').TOPICS,
    CONSUMER_GROUPS: require('./kafka/topics').CONSUMER_GROUPS,
    SUBSCRIPTIONS: require('./kafka/topics').SUBSCRIPTIONS,
  },
  schemas: {
    event: require('./schemas/event.schema.json'),
  },
};
