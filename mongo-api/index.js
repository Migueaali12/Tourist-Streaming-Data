/**
 * REST API bridge: MongoDB -> Grafana (Infinity datasource).
 * Exposes read-only aggregation/find endpoints so Grafana never talks to Mongo directly.
 */
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const { buildPipeline } = require('./pipeline');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'tourism_analytics';
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || 'http://localhost:3000')
  .split(',')
  .map((o) => o.trim())
  .filter(Boolean);

const app = express();
app.use(cors({ origin: ALLOWED_ORIGINS }));
app.use(express.json({ limit: '1mb' }));

let client;
let db;
let dbReady = false;

MongoClient.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 })
  .then((c) => {
    client = c;
    db = c.db(DB_NAME);
    dbReady = true;
    console.log(`[mongo-api] connected to database '${DB_NAME}'`);
  })
  .catch((err) => {
    console.error('[mongo-api] MongoDB connection error:', err.message);
  });

// Fail fast on every data route when Mongo is not up yet.
const requireDb = (req, res, next) => {
  if (!dbReady) {
    return res.status(503).json({ error: 'MongoDB not connected yet' });
  }
  next();
};

app.get('/health', (req, res) => {
  res.status(dbReady ? 200 : 503).json({ status: dbReady ? 'ok' : 'starting', database: DB_NAME });
});

app.get('/collections', requireDb, async (req, res) => {
  try {
    const collections = await db.listCollections().toArray();
    res.json(collections.map((c) => c.name));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/aggregate', requireDb, async (req, res) => {
  try {
    const { collection, pipeline, timestamp_from, timestamp_to, domain_category } = req.body;
    if (!collection || !Array.isArray(pipeline)) {
      return res.status(400).json({ error: 'Required params: collection (string), pipeline (array)' });
    }

    const procPipe = buildPipeline({ pipeline, timestamp_from, timestamp_to, domain_category });
    const results = await db.collection(collection).aggregate(procPipe).toArray();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/find', requireDb, async (req, res) => {
  try {
    const { collection, filter = {}, projection = {}, limit = 100 } = req.body;
    if (!collection) {
      return res.status(400).json({ error: 'Required param: collection (string)' });
    }
    const results = await db
      .collection(collection)
      .find(filter, { projection })
      .limit(Math.min(parseInt(limit, 10) || 100, 1000))
      .toArray();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Infinity-friendly alias: accepts either a pipeline or a plain filter.
app.post('/query_custom', requireDb, async (req, res) => {
  try {
    const { collection, pipeline, filter, projection, limit } = req.body;
    if (!collection) {
      return res.status(400).json({ error: 'Required param: collection (string)' });
    }
    if (Array.isArray(pipeline)) {
      const results = await db.collection(collection).aggregate(pipeline).toArray();
      return res.json(results);
    }
    const results = await db
      .collection(collection)
      .find(filter || {}, { projection: projection || {} })
      .limit(Math.min(parseInt(limit, 10) || 100, 1000))
      .toArray();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Powers Grafana dashboard variables (country_code, domain_category, ...).
app.get('/distinct/:collection/:field', requireDb, async (req, res) => {
  try {
    const { collection, field } = req.params;
    if (!collection || !field) {
      return res.status(400).json({ error: 'Required params: collection, field' });
    }
    res.json(await db.collection(collection).distinct(field));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const server = app.listen(PORT, () => {
  console.log(`[mongo-api] listening on port ${PORT}`);
});

const shutdown = async () => {
  server.close();
  if (client) await client.close();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);