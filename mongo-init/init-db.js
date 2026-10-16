// Runs once, the first time the MongoDB container initialises its data volume.
db = db.getSiblingDB('tourism_analytics');

// Time series container: one document per organic search result or related search.
db.createCollection('search_results', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['id', 'type', 'search_metadata'],
      properties: {
        id: { bsonType: 'string' },
        type: { enum: ['organic_result', 'related_search'] },
        country_code: { bsonType: 'string' },
        domain: { bsonType: 'string' },
        domain_category: { bsonType: 'string' },
        position: { bsonType: 'number' },
        search_metadata: {
          bsonType: 'object',
          required: ['search_keyword', 'search_timestamp'],
          properties: {
            search_keyword: { bsonType: 'string' },
            search_timestamp: { bsonType: 'string' },
            total_results: { bsonType: 'number' },
          },
        },
      },
    },
  },
});

db.createCollection('related_searches');
db.createCollection('processing_stats');

// Indexes: the ones the dashboards and the multi-country filters actually use.
db.search_results.createIndex({ 'search_metadata.search_timestamp': 1 });
db.search_results.createIndex({ 'search_metadata.search_keyword': 1 });
db.search_results.createIndex({ country_code: 1 });
db.search_results.createIndex({ domain_category: 1 });
db.search_results.createIndex({ position: 1 });
db.search_results.createIndex({ country_code: 1, 'search_metadata.search_timestamp': 1 });

db.related_searches.createIndex({ 'search_metadata.search_timestamp': 1 });
db.processing_stats.createIndex({ timestamp: -1 });

print('tourism_analytics initialised: collections and indexes created');