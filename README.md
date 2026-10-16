# Tourism Search Analytics: Real-Time Multi-Country Streaming Pipeline

[Español](README.es.md) | **English**

[![CI](https://github.com/Migueaali12/Tourist-Streaming-Data/actions/workflows/ci.yml/badge.svg)](https://github.com/Migueaali12/Tourist-Streaming-Data/actions/workflows/ci.yml)

Collects Google search results for tourism keywords across five countries, streams them through Kafka, stores them in MongoDB with geographic metadata, and exposes them to Grafana through an Express REST API.

The interesting part is not the data itself, it is the plumbing: a producer, a streaming buffer, a validated document store, and a query layer that lets a BI tool build multi-country comparisons without ever touching the database.

## Architecture

```
SerpAPI (google, hl/gl per country)
        |
        v
Kafka producer (Python, multi-country config)
        |  topic: tourism_analytics
        v
Kafka consumer (Python)  ->  normalises + enriches each result
        |                     country_code, country_name, currency, domain_category
        v
MongoDB (schema validator + indexes per country/keyword/time)
        |
        v
Express REST API (read-only: /aggregate, /find, /distinct, /health)
        |
        v
Grafana (Infinity datasource, country variable, 5 panels)
```

## Stack

- **Ingestion**: SerpAPI (Google Search results), Python
- **Streaming**: Apache Kafka 3.5 + ZooKeeper (bitnami images)
- **Storage**: MongoDB 7, JSON schema validation, compound indexes
- **Serving**: Node.js 20 + Express 4, MongoDB driver 6
- **Visualisation**: Grafana 11 with the Infinity datasource
- **Runtime**: Docker Compose (5 services, healthchecks, provisioning as code)
- **CI**: GitHub Actions (Node syntax check, JSON/YAML validation, `docker compose config`)

## Quick start

Requirements: Docker + Docker Compose, 4 GB RAM, ports 3000 / 8080 / 9092 / 27017 free, and a [SerpAPI key](https://serpapi.com/).

```bash
git clone https://github.com/Migueaali12/Tourist-Streaming-Data.git
cd Tourist-Streaming-Data
cp .env.example .env          # set SERPAPI_KEY and your own passwords
./start-analytics-stack.sh    # use --reset to wipe volumes, --logs to follow logs
```

Then generate traffic and explore it:

```bash
pip install -r requirements.txt
jupyter notebook               # run Kafka-producer-SerpAPI.ipynb, then Kafka-consumer-SerpAPI.ipynb
```

Services once the stack is up:

- Grafana: http://localhost:3000 (admin / value of `GRAFANA_ADMIN_PASSWORD`), dashboard **Tourism Analytics** is provisioned
- Express API: http://localhost:8080
- Kafka: `localhost:9092`, topic `tourism_analytics`
- MongoDB: `localhost:27017`, database `tourism_analytics`

## What the pipeline produces

The producer rotates over a per-country configuration (Ecuador, Spain, USA, Mexico, Germany) and attaches geographic metadata to every message:

| Country | Code | Language | Currency |
|---------|------|----------|----------|
| Ecuador | EC | es | USD |
| Spain | ES | es | EUR |
| USA | US | en | USD |
| Mexico | MX | es | MXN |
| Germany | DE | de | EUR |

The consumer flattens that metadata to the document root, validates it against the collection schema, and indexes the fields the dashboards filter on:

```json
{
  "id": "8f3c...",
  "type": "organic_result",
  "country_code": "EC",
  "country_name": "Ecuador",
  "currency": "USD",
  "domain": "booking.com",
  "domain_category": "hotel",
  "position": 1,
  "search_metadata": {
    "search_keyword": "hoteles boutique Quito",
    "search_timestamp": "2026-01-14T18:22:05.114Z",
    "total_results": 1840000
  }
}
```

Indexes: `search_metadata.search_timestamp`, `search_metadata.search_keyword`, `country_code`, `domain_category`, `position`, plus the compound `(country_code, search_metadata.search_timestamp)` used by every time-filtered dashboard query.

## API reference

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/health` | Liveness plus MongoDB connection state |
| GET | `/collections` | Collections available in the database |
| POST | `/aggregate` | Runs a MongoDB aggregation pipeline |
| POST | `/find` | Simple filtered read, capped at 1000 docs |
| POST | `/query_custom` | Accepts either a pipeline or a filter |
| GET | `/distinct/:collection/:field` | Unique values, used by Grafana template variables |

```bash
curl -s localhost:8080/health
# {"status":"ok","database":"tourism_analytics"}

curl -s localhost:8080/distinct/search_results/country_code
# ["EC","ES","US","MX","DE"]

curl -s -X POST localhost:8080/aggregate -H 'Content-Type: application/json' -d '{
  "collection": "search_results",
  "pipeline": [
    {"$match": {"country_code": "EC"}},
    {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
  ]
}'
```

Two details that make the Grafana integration work in practice:

- `$$ISO_FROM` / `$$ISO_TO` placeholders anywhere in a pipeline get replaced with the dashboard time range as ISO strings, so panels can filter on `search_metadata.search_timestamp`.
- Grafana sends `.*` for "All" and `{EC,ES,US}` for multi-select variables instead of field paths. The API detects both, maps them to real fields, and injects the matching `$match` stage before the pipeline reaches MongoDB.

## Custom queries in MongoDB

```javascript
// Competitive position per country
db.search_results.aggregate([
  { $match: { position: { $lte: 5 } } },
  { $group: {
      _id: { domain: "$domain", country: "$country_code" },
      appearances: { $sum: 1 },
      avg_position: { $avg: "$position" }
  }},
  { $sort: { appearances: -1 } }
])
```

## Project structure

```
.
├── docker-compose.yml                    # kafka, zookeeper, mongo, mongo-api, grafana
├── start-analytics-stack.sh              # brings the stack up and waits for health
├── .env.example                          # credentials and SerpAPI key template
├── requirements.txt                      # notebook dependencies
├── mongo-init/init-db.js                 # collections, validator, indexes
├── mongo-api/                            # Express REST API
│   ├── index.js                          # routes and MongoDB connection
│   ├── pipeline.js                       # Grafana variable/time normalisation (unit tested)
│   ├── test/pipeline.test.js             # node:test suite
│   ├── package.json
│   └── Dockerfile
├── grafana/
│   ├── provisioning/datasources/infinity.yml
│   └── dashboards/tourism-analytics.json
├── Kafka-producer-SerpAPI.ipynb          # multi-country producer
├── Kafka-consumer-SerpAPI.ipynb          # consumer, normalisation, MongoDB sink
└── Get_data_SerpAPI.ipynb                # minimal SerpAPI exploration
```

## Configuration

| Variable | Used by | Default |
|----------|---------|---------|
| `SERPAPI_KEY` | producer notebooks | none, required |
| `MONGO_ROOT_USER` / `MONGO_ROOT_PASSWORD` | MongoDB, API | `admin` / `password123` |
| `GRAFANA_ADMIN_PASSWORD` | Grafana | `admin123` |
| `ALLOWED_ORIGINS` | Express CORS allowlist | `http://localhost:3000` |
| `PORT`, `DB_NAME`, `MONGO_URI` | Express API | `8080`, `tourism_analytics` |

Defaults are for local development only.

## Limitations and next steps

- The SerpAPI free tier caps monthly searches, so the demo dataset is small by design; the pipeline is what is being demonstrated, not the volume.
- ZooKeeper + Kafka is a deliberate legacy-mode setup. KRaft mode would remove a service.
- The API layer ships with unit tests for the pipeline helpers (`npm test`, 7 cases, run in CI) but the HTTP routes themselves are not covered yet; a supertest suite with a seeded MongoDB fixture is the next step.
- Sentiment analysis and price extraction over the stored result snippets are planned but not implemented.

## Author

Miguel Alvarez, [migueaali.me](https://migueaali.me/) · [GitHub](https://github.com/Migueaali12)