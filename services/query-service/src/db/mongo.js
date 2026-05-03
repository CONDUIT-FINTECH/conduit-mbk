const mongoose = require('mongoose');

/**
 * ═══════════════════════════════════════════════════
 *  MongoDB Connection (Mongoose)
 * ═══════════════════════════════════════════════════
 *
 * Used for: ML Predictions, Audit Trail
 * Why Mongo: Schema-less documents for deeply nested ML
 *            payloads (feature vectors, confidence arrays, 
 *            anomaly scores) that don't fit relational models.
 */

const predictionSchema = new mongoose.Schema({
  tenantId:    { type: String, required: true, index: true },
  predictionId: { type: String, unique: true, required: true },
  modelId:     { type: String },
  anomalyScore: { type: Number },
  confidence:  { type: Number },
  labels:      [String],
  features:    { type: mongoose.Schema.Types.Mixed },
  metadata:    { type: mongoose.Schema.Types.Mixed },
  createdAt:   { type: Date, default: Date.now, index: true },
});

predictionSchema.index({ tenantId: 1, createdAt: -1 });

const Prediction = mongoose.model('Prediction', predictionSchema);

async function connectMongo() {
  const uri = process.env.QUERY_MONGO_URI || process.env.MONGO_URI || 'mongodb://localhost:27017/conduit_query';
  await mongoose.connect(uri, {
    maxPoolSize: parseInt(process.env.QUERY_MONGO_POOL_MAX || process.env.MONGO_POOL_MAX || '5', 10),
  });
  console.log('[MongoDB] Connected');
}

async function disconnectMongo() {
  await mongoose.disconnect();
}

module.exports = { Prediction, connectMongo, disconnectMongo };
