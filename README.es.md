# Tourism Search Analytics: Pipeline de Streaming Multi-País en Tiempo Real

**Español** | [English](README.md)

[![CI](https://github.com/Migueaali12/Tourist-Streaming-Data/actions/workflows/ci.yml/badge.svg)](https://github.com/Migueaali12/Tourist-Streaming-Data/actions/workflows/ci.yml)

Recolecta resultados de búsqueda de Google para keywords de turismo en cinco países, los transmite por Kafka, los almacena en MongoDB con metadatos geográficos y los expone a Grafana mediante una API REST en Express.

Lo interesante no son los datos, es la tubería: un producer, un buffer de streaming, un almacén de documentos validado y una capa de consulta que permite a una herramienta de BI construir comparativas multi-país sin tocar nunca la base de datos.

## Arquitectura

```
SerpAPI (google, hl/gl por país)
        |
        v
Productor Kafka (Python, configuración multi-país)
        |  topic: tourism_analytics
        v
Consumidor Kafka (Python)  ->  normaliza y enriquece cada resultado
        |                     country_code, country_name, currency, domain_category
        v
MongoDB (validador de esquema + índices por país/keyword/tiempo)
        |
        v
API REST Express (solo lectura: /aggregate, /find, /distinct, /health)
        |
        v
Grafana (datasource Infinity, variable de país, 5 paneles)
```

## Stack

- **Ingesta**: SerpAPI (resultados de Google), Python
- **Streaming**: Apache Kafka 3.5 + ZooKeeper (imágenes bitnami)
- **Almacenamiento**: MongoDB 7, validación de esquema JSON, índices compuestos
- **Servicio**: Node.js 20 + Express 4, driver de MongoDB 6
- **Visualización**: Grafana 11 con el datasource Infinity
- **Ejecución**: Docker Compose (5 servicios, healthchecks, provisioning como código)
- **CI**: GitHub Actions (chequeo de sintaxis Node, validación JSON/YAML, `docker compose config`)

## Inicio rápido

Requisitos: Docker + Docker Compose, 4 GB de RAM, puertos 3000 / 8080 / 9092 / 27017 libres, y una [API key de SerpAPI](https://serpapi.com/).

```bash
git clone https://github.com/Migueaali12/Tourist-Streaming-Data.git
cd Tourist-Streaming-Data
cp .env.example .env          # define SERPAPI_KEY y tus propias contraseñas
./start-analytics-stack.sh    # --reset borra volúmenes, --logs sigue los logs
```

Después genera tráfico y explóralo:

```bash
pip install -r requirements.txt
jupyter notebook               # ejecuta Kafka-producer-SerpAPI.ipynb y luego Kafka-consumer-SerpAPI.ipynb
```

Servicios disponibles con el stack arriba:

- Grafana: http://localhost:3000 (admin / valor de `GRAFANA_ADMIN_PASSWORD`), el dashboard **Tourism Analytics** queda provisionado
- API Express: http://localhost:8080
- Kafka: `localhost:9092`, topic `tourism_analytics`
- MongoDB: `localhost:27017`, base `tourism_analytics`

## Qué produce el pipeline

El productor rota sobre una configuración por país (Ecuador, España, Estados Unidos, México, Alemania) y adjunta metadatos geográficos a cada mensaje:

| País | Código | Idioma | Moneda |
|------|--------|--------|--------|
| Ecuador | EC | es | USD |
| España | ES | es | EUR |
| Estados Unidos | US | en | USD |
| México | MX | es | MXN |
| Alemania | DE | de | EUR |

El consumidor aplana esos metadatos a la raíz del documento, los valida contra el esquema de la colección e indexa los campos que filtran los dashboards:

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

Índices: `search_metadata.search_timestamp`, `search_metadata.search_keyword`, `country_code`, `domain_category`, `position`, más el compuesto `(country_code, search_metadata.search_timestamp)` que usan todas las consultas con filtro temporal del dashboard.

## Referencia de la API

| Método | Endpoint | Propósito |
|--------|----------|-----------|
| GET | `/health` | Estado del servicio y de la conexión a MongoDB |
| GET | `/collections` | Colecciones disponibles |
| POST | `/aggregate` | Ejecuta un pipeline de agregación de MongoDB |
| POST | `/find` | Lectura filtrada simple, tope de 1000 documentos |
| POST | `/query_custom` | Acepta pipeline o filtro |
| GET | `/distinct/:collection/:field` | Valores únicos, usados por las variables de Grafana |

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

Dos detalles que hacen que la integración con Grafana funcione en la práctica:

- Los placeholders `$$ISO_FROM` / `$$ISO_TO` en cualquier etapa del pipeline se reemplazan por el rango temporal del dashboard en ISO, así los paneles pueden filtrar por `search_metadata.search_timestamp`.
- Grafana envía `.*` para "All" y `{EC,ES,US}` para variables multi-selección en lugar de rutas de campos. La API detecta ambos casos, los mapea a campos reales e inyecta el `$match` correspondiente antes de enviar el pipeline a MongoDB.

## Consultas personalizadas en MongoDB

```javascript
// Posición competitiva por país
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

## Estructura del proyecto

```
.
├── docker-compose.yml                    # kafka, zookeeper, mongo, mongo-api, grafana
├── start-analytics-stack.sh              # levanta el stack y espera salud
├── .env.example                          # plantilla de credenciales y API key
├── requirements.txt                      # dependencias de los notebooks
├── mongo-init/init-db.js                 # colecciones, validador, índices
├── mongo-api/                            # API REST Express
│   ├── index.js                          # rutas y conexión a MongoDB
│   ├── pipeline.js                       # normalización de variables/tiempo de Grafana (con tests)
│   ├── test/pipeline.test.js             # suite node:test
│   ├── package.json
│   └── Dockerfile
├── grafana/
│   ├── provisioning/datasources/infinity.yml
│   └── dashboards/tourism-analytics.json
├── Kafka-producer-SerpAPI.ipynb          # productor multi-país
├── Kafka-consumer-SerpAPI.ipynb          # consumidor, normalización, sink a MongoDB
└── Get_data_SerpAPI.ipynb                # exploración mínima de SerpAPI
```

## Configuración

| Variable | La usa | Default |
|----------|--------|---------|
| `SERPAPI_KEY` | notebooks del productor | ninguna, obligatoria |
| `MONGO_ROOT_USER` / `MONGO_ROOT_PASSWORD` | MongoDB, API | `admin` / `password123` |
| `GRAFANA_ADMIN_PASSWORD` | Grafana | `admin123` |
| `ALLOWED_ORIGINS` | allowlist CORS de Express | `http://localhost:3000` |
| `PORT`, `DB_NAME`, `MONGO_URI` | API Express | `8080`, `tourism_analytics` |

Los valores por defecto son solo para desarrollo local.

## Limitaciones y siguientes pasos

- El plan gratuito de SerpAPI limita las búsquedas mensuales, así que el dataset de demo es pequeño a propósito; lo que se demuestra es el pipeline, no el volumen.
- ZooKeeper + Kafka es un setup deliberadamente en modo legacy. El modo KRaft eliminaría un servicio.
- La capa de API incluye tests unitarios de los helpers del pipeline (`npm test`, 7 casos, corren en CI) pero las rutas HTTP todavía no están cubiertas; una suite con supertest y un fixture de MongoDB sembrado es el siguiente paso.
- El análisis de sentimiento y la extracción de precios sobre los snippets almacenados están planteados pero no implementados.

## Autor

Miguel Alvarez, [migueaali.me](https://migueaali.me/) · [GitHub](https://github.com/Migueaali12)