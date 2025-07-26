// Inicialización de MongoDB para Tourism Analytics
// Este script se ejecuta automáticamente al inicializar el contenedor

// Crear base de datos tourism_analytics
db = db.getSiblingDB('tourism_analytics');

// Crear usuario específico para Grafana
db.createUser({
  user: "grafana_user",
  pwd: "grafana_password",
  roles: [
    {
      role: "read",
      db: "tourism_analytics"
    }
  ]
});

// Crear colecciones con esquemas optimizados
db.createCollection("search_results", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["id", "type", "search_metadata"],
      properties: {
        id: { bsonType: "string" },
        type: { enum: ["organic_result", "related_search"] },
        search_metadata: {
          bsonType: "object",
          required: ["search_keyword", "search_timestamp"],
          properties: {
            search_keyword: { bsonType: "string" },
            search_timestamp: { bsonType: "string" },
            total_results: { bsonType: "number" }
          }
        }
      }
    }
  }
});

db.createCollection("related_searches");
db.createCollection("search_metadata");
db.createCollection("processing_stats");

// Crear índices optimizados para Grafana
db.search_results.createIndex({ "search_metadata.search_timestamp": 1 });
db.search_results.createIndex({ "search_metadata.search_keyword": 1 });
db.search_results.createIndex({ "domain_category": 1 });
db.search_results.createIndex({ "source": 1 });
db.search_results.createIndex({ "position": 1 });

db.related_searches.createIndex({ "search_metadata.search_timestamp": 1 });
db.related_searches.createIndex({ "query_category": 1 });

db.processing_stats.createIndex({ "timestamp": -1 });

// Insertar datos de ejemplo para testing (opcional)
db.search_results.insertOne({
  "id": "example-001",
  "type": "organic_result",
  "search_metadata": {
    "search_keyword": "turismo Ecuador 2025",
    "search_timestamp": new Date().toISOString(),
    "total_results": 2500000
  },
  "title": "Ecuador: Destino Turístico 2025",
  "source": "Tourism Ecuador",
  "position": 1,
  "domain": "ecuador.travel",
  "domain_category": "tourism",
  "processed_at": new Date().toISOString()
});

print("✅ Base de datos tourism_analytics inicializada correctamente");
print("✅ Usuario grafana_user creado");
print("✅ Colecciones e índices creados");
print("✅ Datos de ejemplo insertados"); 