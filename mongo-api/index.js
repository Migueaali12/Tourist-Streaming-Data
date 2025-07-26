const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());

const mongoUri = process.env.MONGO_URI || 'mongodb://admin:password123@localhost:27017/?authSource=admin';
let db;

MongoClient.connect(mongoUri, { useUnifiedTopology: true })
  .then(client => {
    db = client.db();
    console.log('✅ Conectado a MongoDB');
  })
  .catch(err => {
    console.error('❌ Error conectando a MongoDB:', err);
  });

// Endpoint de salud
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// Listar colecciones disponibles
app.get('/collections', async (req, res) => {
  try {
    const collections = await db.listCollections().toArray();
    res.json(collections.map(c => c.name));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Ejecutar pipeline de agregación (POST /aggregate)
app.post('/aggregate', async (req, res) => {
  try {
    const { collection, pipeline, timestamp_from, timestamp_to, domain_category } = req.body;

    // 🔍 DEBUG: Log del pipeline recibido
    console.log('📥 Pipeline recibido:', JSON.stringify(pipeline, null, 2));

    if (!collection || !Array.isArray(pipeline)) {
      return res.status(400).json({ error: 'Parámetros requeridos: collection (string), pipeline (array)' });
    }

    // Convertir timestamps si vienen en ms
    const isoFrom = timestamp_from ? new Date(parseInt(timestamp_from)).toISOString() : null;
    const isoTo = timestamp_to   ? new Date(parseInt(timestamp_to)).toISOString()   : null;

    // Función recursiva para reemplazar placeholders especiales
    const processPipeline = (obj) => {
      if (Array.isArray(obj)) {
        return obj.map(processPipeline);
      } else if (obj && typeof obj === 'object') {
        const newObj = {};
        for (const key in obj) {
          const val = obj[key];
          if (val === '$$ISO_FROM') {
            newObj[key] = isoFrom;
          } else if (val === '$$ISO_TO') {
            newObj[key] = isoTo;
          } else {
            newObj[key] = processPipeline(val);
          }
        }
        return newObj;
      }
      return obj;
    };

    // Pre-procesar pipeline
    let procPipe = processPipeline(pipeline);

    // Si se recibe domain_category, insertar etapa $match con $in
    if (domain_category) {
      procPipe.unshift({
        $match: {
          domain_category: Array.isArray(domain_category)
            ? { $in: domain_category }
            : { $in: [domain_category] }
        }
      });
    }

    // Corregir agrupaciones con categoría recibida como string literal
    procPipe = procPipe.map(stage => {
      if (stage.$group && stage.$group._id) {
        // Si _id.category es string conteniendo llaves, usar campo real
        const cat = stage.$group._id.category;
        if (typeof cat === 'string' && cat.startsWith('{') && cat.endsWith('}')) {
          stage.$group._id.category = '$domain_category';
        }
      }
      return stage;
    });

    // 🌍 MANEJO ESPECIAL PARA GRAFANA VARIABLES
    // Detectar y corregir casos donde Grafana envía valores de variables incorrectamente
    procPipe = procPipe.map(stage => {
      if (stage.$group && stage.$group._id) {
        // Caso 1: _id es directamente ".*" (selección All en variable)
        if (stage.$group._id === '.*') {
          stage.$group._id = '$country_code';
          console.log('🔧 Corregido: _id ".*" → "$country_code" para mostrar todos los países');
        }
        // Caso 2: _id contiene valores múltiples de Grafana como "{EC,ES,US}"
        else if (typeof stage.$group._id === 'string' && stage.$group._id.match(/^\{.+\}$/)) {
          // Extraer los valores de la cadena {EC,ES,US}
          const originalValue = stage.$group._id;
          const countryValues = stage.$group._id.slice(1, -1).split(',');
          stage.$group._id = '$country_code';
          console.log(`🔧 Corregido: _id "${originalValue}" → "$country_code" para países: ${countryValues.join(', ')}`);
          
          // Agregar filtro $match para los países específicos seleccionados
          procPipe.unshift({
            $match: {
              country_code: { $in: countryValues }
            }
          });
          console.log(`🔧 Agregado filtro: {country_code: {$in: [${countryValues.join(', ')}]}}`);
        }
        // Caso 3: _id es un objeto que contiene campos con ".*" o patrones de Grafana
        else if (typeof stage.$group._id === 'object' && stage.$group._id !== null) {
          for (const [key, value] of Object.entries(stage.$group._id)) {
            if (value === '.*') {
              // Mapear campos comunes de Grafana a campos reales de MongoDB
              const fieldMapping = {
                'country': '$country_code',
                'country_code': '$country_code',
                'keyword': '$search_metadata.search_keyword',
                'domain_category': '$domain_category',
                'category': '$domain_category'  // Para consultas que usan "category": ".*"
              };
              stage.$group._id[key] = fieldMapping[key] || `$${key}`;
              console.log(`🔧 Corregido: _id.${key} ".*" → "${fieldMapping[key] || `$${key}`}"`);
            }
                         // Detectar patrones como "{EC,ES,US}" en objetos
             else if (typeof value === 'string' && value.match(/^\{.+\}$/)) {
               const fieldMapping = {
                 'country': '$country_code',
                 'country_code': '$country_code',
                 'keyword': '$search_metadata.search_keyword',
                 'domain_category': '$domain_category',
                 'category': '$domain_category'  // Para consultas que usan "category": ".*"
               };
              stage.$group._id[key] = fieldMapping[key] || `$${key}`;
              console.log(`🔧 Corregido: _id.${key} "${value}" → "${fieldMapping[key] || `$${key}`}"`);
            }
          }
        }
      }
      return stage;
    });

    // 🔍 DEBUG: Log del pipeline procesado
    console.log('🔧 Pipeline procesado:', JSON.stringify(procPipe, null, 2));

    const results = await db.collection(collection).aggregate(procPipe).toArray();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
  

// Consulta simple por filtro (POST /find)
app.post('/find', async (req, res) => {
  try {
    const { collection, filter = {}, projection = {}, limit = 100 } = req.body;
    if (!collection) {
      return res.status(400).json({ error: 'Parámetro requerido: collection (string)' });
    }
    const results = await db.collection(collection)
      .find(filter, { projection })
      .limit(Math.min(limit, 1000))
      .toArray();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Endpoint compatible con Infinity para consulta personalizada (POST /query_custom)
app.post('/query_custom', async (req, res) => {
  try {
    const { collection, pipeline, filter, projection, limit } = req.body;
    if (!collection) {
      return res.status(400).json({ error: 'Parámetro requerido: collection (string)' });
    }
    if (pipeline && Array.isArray(pipeline)) {
      // Si se envía pipeline, usar agregación
      const results = await db.collection(collection).aggregate(pipeline).toArray();
      return res.json(results);
    } else {
      // Si no, usar find simple
      const results = await db.collection(collection)
        .find(filter || {}, { projection: projection || {} })
        .limit(Math.min(limit || 100, 1000))
        .toArray();
      return res.json(results);
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Listar campos únicos de una colección (útil para variables en dashboards)
app.get('/distinct/:collection/:field', async (req, res) => {
  try {
    const { collection, field } = req.params;
    if (!collection || !field) {
      return res.status(400).json({ error: 'Parámetros requeridos: collection, field' });
    }
    const values = await db.collection(collection).distinct(field);
    res.json(values);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = 8080;
app.listen(PORT, () => {
  console.log(`🚀 API de MongoDB para Grafana escuchando en puerto ${PORT}`);
});