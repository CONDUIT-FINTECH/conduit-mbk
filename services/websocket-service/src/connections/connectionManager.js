const { WebSocketServer } = require('ws');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// All available channels clients can subscribe to
const AVAILABLE_CHANNELS = [
  'events:live',
  'metrics:dashboard',
  'incidents:alerts',
  'remediations:actions',
  'ml:predictions',
];

/**
 * ═══════════════════════════════════════════════════
 *  Connection Manager — WebSocket Server
 * ═══════════════════════════════════════════════════
 * 
 * Manages:
 *   - JWT-authenticated WebSocket connections
 *   - Per-client channel subscriptions
 *   - Heartbeat monitoring (stale client eviction)
 *   - Backpressure-aware broadcast (drop if client can't keep up)
 *   - Multi-tenant isolation
 */
class ConnectionManager {
  constructor(httpServer, jwtSecret) {
    this.clients = new Map(); // clientId → { ws, tenantId, subscriptions, lastPing }
    this.jwtSecret = jwtSecret;
    this.heartbeatInterval = null;

    // Backpressure: max bytes allowed in the WS send buffer before dropping
    this.maxBufferedAmount = parseInt(process.env.WS_MAX_BUFFERED_BYTES || '65536', 10); // 64KB

    this.wss = new WebSocketServer({
      server: httpServer,
      verifyClient: (info, cb) => this._authenticate(info, cb),
      maxPayload: 1024 * 64, // 64KB max inbound message
    });

    this.wss.on('connection', (ws, req) => this._onConnection(ws, req));
    this._startHeartbeat();
  }

  // ─── Authentication ─────────────────────────────
  _authenticate(info, callback) {
    try {
      const url = new URL(info.req.url, 'ws://localhost');
      const token = url.searchParams.get('token');

      if (!token) {
        callback(false, 401, 'Missing token');
        return;
      }

      const decoded = jwt.verify(token, this.jwtSecret);
      info.req.user = decoded;
      callback(true);
    } catch (err) {
      callback(false, 401, 'Invalid token');
    }
  }

  // ─── Connection Lifecycle ───────────────────────
  _onConnection(ws, req) {
    const clientId = crypto.randomUUID();
    const tenantId = req.user?.tenantId || 'default';

    this.clients.set(clientId, {
      ws,
      tenantId,
      subscriptions: new Set(),
      lastPing: Date.now(),
      connectedAt: Date.now(),
    });

    console.log(`[WS] + ${clientId} (tenant: ${tenantId}) | total: ${this.clients.size}`);

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this._handleMessage(clientId, msg);
      } catch {
        this._send(ws, { type: 'error', message: 'Invalid JSON' });
      }
    });

    ws.on('pong', () => {
      const client = this.clients.get(clientId);
      if (client) client.lastPing = Date.now();
    });

    ws.on('close', () => {
      this.clients.delete(clientId);
      console.log(`[WS] - ${clientId} | total: ${this.clients.size}`);
    });

    // Welcome message with available channels
    this._send(ws, {
      type: 'connected',
      clientId,
      availableChannels: AVAILABLE_CHANNELS,
      serverTime: new Date().toISOString(),
    });
  }

  // ─── Client Message Handling ────────────────────
  _handleMessage(clientId, msg) {
    const client = this.clients.get(clientId);
    if (!client) return;

    switch (msg.type) {
      case 'subscribe':
        if (msg.channels && Array.isArray(msg.channels)) {
          // Batch subscribe
          const subscribed = [];
          for (const ch of msg.channels) {
            if (AVAILABLE_CHANNELS.includes(ch)) {
              client.subscriptions.add(ch);
              subscribed.push(ch);
            }
          }
          this._send(client.ws, { type: 'subscribed', channels: subscribed });
        } else if (msg.channel && AVAILABLE_CHANNELS.includes(msg.channel)) {
          client.subscriptions.add(msg.channel);
          this._send(client.ws, { type: 'subscribed', channel: msg.channel });
        } else {
          this._send(client.ws, {
            type: 'error',
            message: `Unknown channel: ${msg.channel}`,
            availableChannels: AVAILABLE_CHANNELS,
          });
        }
        break;

      case 'unsubscribe':
        if (msg.channel) {
          client.subscriptions.delete(msg.channel);
          this._send(client.ws, { type: 'unsubscribed', channel: msg.channel });
        }
        break;

      case 'ping':
        this._send(client.ws, { type: 'pong', serverTime: new Date().toISOString() });
        break;

      default:
        this._send(client.ws, { type: 'error', message: `Unknown type: ${msg.type}` });
    }
  }

  // ─── Broadcast ──────────────────────────────────
  /**
   * Broadcast a pre-built message to all clients subscribed to a channel.
   * Filters by tenantId for multi-tenant isolation.
   * Implements backpressure: skips clients with full send buffers.
   *
   * @param {string} channel
   * @param {object} message - Pre-serialized envelope from SubscriptionEngine
   * @param {string|null} tenantId
   * @returns {number} Number of clients that received the message
   */
  broadcast(channel, message, tenantId = null) {
    let sent = 0;
    let dropped = 0;
    const serialized = JSON.stringify(message);

    for (const [, client] of this.clients) {
      if (!client.subscriptions.has(channel)) continue;
      if (tenantId && client.tenantId !== tenantId) continue;
      if (client.ws.readyState !== 1) continue; // WebSocket.OPEN

      // Backpressure check: skip if the client's outbound buffer is full
      if (client.ws.bufferedAmount > this.maxBufferedAmount) {
        dropped++;
        continue;
      }

      client.ws.send(serialized);
      sent++;
    }

    if (dropped > 0) {
      console.warn(`[WS] Backpressure: dropped ${dropped} clients on ${channel}`);
    }

    return sent;
  }

  // ─── Safe Send ──────────────────────────────────
  _send(ws, obj) {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify(obj));
    }
  }

  // ─── Heartbeat ──────────────────────────────────
  _startHeartbeat() {
    const INTERVAL = 30_000;
    const TIMEOUT = 60_000;

    this.heartbeatInterval = setInterval(() => {
      const now = Date.now();
      for (const [clientId, client] of this.clients) {
        if (now - client.lastPing > TIMEOUT) {
          console.log(`[WS] Terminating stale: ${clientId}`);
          client.ws.terminate();
          this.clients.delete(clientId);
        } else {
          client.ws.ping();
        }
      }
    }, INTERVAL);
  }

  // ─── Cleanup ────────────────────────────────────
  closeAll() {
    clearInterval(this.heartbeatInterval);
    for (const [, client] of this.clients) {
      client.ws.close(1001, 'Server shutting down');
    }
    this.clients.clear();
  }

  getStats() {
    const tenants = new Map();
    const channels = new Map();

    for (const [, client] of this.clients) {
      tenants.set(client.tenantId, (tenants.get(client.tenantId) || 0) + 1);
      for (const ch of client.subscriptions) {
        channels.set(ch, (channels.get(ch) || 0) + 1);
      }
    }

    return {
      totalConnections: this.clients.size,
      byTenant: Object.fromEntries(tenants),
      byChannel: Object.fromEntries(channels),
    };
  }
}

module.exports = { ConnectionManager, AVAILABLE_CHANNELS };
