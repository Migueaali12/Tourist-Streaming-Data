# 🌍 Multi-Country Tourism Analytics - Real-time Data Pipeline

Sistema completo de análisis en tiempo real para datos de turismo **multi-país** utilizando **SerpAPI**, **Kafka**, **MongoDB** y **Grafana** con soporte para **5 países simultáneos**.

## 🎯 Overview

Este proyecto implementa un pipeline de datos completo que:

1. **Extrae** datos de Google vía SerpAPI sobre turismo en múltiples países (Ecuador, España, EE.UU., México, Alemania)
2. **Procesa** datos en tiempo real usando Kafka con metadatos geográficos
3. **Almacena** datos estructurados en MongoDB con índices optimizados por país
4. **Visualiza** insights en dashboards interactivos con Grafana y filtros geográficos

## 🏗️ Arquitectura Multi-País

```
🔍 SerpAPI → 📡 Kafka → 🗄️ MongoDB → 🔌 Express API → 📊 Grafana Dashboard
     ↓           ↓           ↓            ↓             ↓
 Multi-Country  Streaming   Country      REST API      Geographic
 Google Data   + Metadata  Structured   Interface     Filtered
                Processing   Storage     (Port 8080)   Visualization
```

### 🔗 Flujo de Datos Completo
1. **SerpAPI** extrae datos de Google por país
2. **Kafka** procesa streams con metadatos geográficos  
3. **MongoDB** almacena datos estructurados con índices por país
4. **Express API** expone endpoints REST para consultas optimizadas
5. **Grafana** visualiza datos usando el API como fuente

## 🌍 Nuevas Características Multi-País

### 🏭 **Producer Multi-País**
- ✅ **5 países pre-configurados**: Ecuador, España, EE.UU., México, Alemania
- ✅ **Configuración dinámica**: Cambio entre países con un comando
- ✅ **Metadatos geográficos**: Información completa del país en cada mensaje
- ✅ **Soporte multi-idioma**: Español e inglés automático por país

### 📡 **Consumer Mejorado** 
- ✅ **Extracción automática**: Información del país desde metadatos
- ✅ **Campos a nivel raíz**: `country_code`, `country_name`, `currency`, etc.
- ✅ **Índices optimizados**: Consultas rápidas por país
- ✅ **Retrocompatibilidad**: Funciona con datos sin información de país

### 📊 **Grafana con Filtros Geográficos**
- ✅ **Variable país**: Filtro dropdown en dashboards
- ✅ **Consultas optimizadas**: Agregaciones por país pre-configuradas
- ✅ **Comparativas**: Análisis cross-country automatizado

## 🗺️ Configuración de Países

### Países Disponibles

| País | Código | Idioma | Moneda | Configuración |
|------|--------|--------|--------|---------------|
| 🇪🇨 Ecuador | `EC` | Español | USD | `ecuador` |
| 🇪🇸 España | `ES` | Español | EUR | `spain` |
| 🇺🇸 Estados Unidos | `US` | Inglés | USD | `usa` |
| 🇲🇽 México | `MX` | Español | MXN | `mexico` |
| 🇩🇪 Alemania | `DE` | Español | EUR | `germany` |

### 📝 Ejemplos de Uso Multi-País

#### Producer - Cambiar País
```python
# Configurar API key
serp_producer = SerpAPIKafkaProducer(api_key="tu_api_key")

# Ecuador - mercado local en español
serp_producer.set_country('ecuador')
serp_producer.stream_search_data(['hoteles quito', 'vuelos ecuador'])

# Estados Unidos - mercado internacional en inglés  
serp_producer.set_country('usa')
serp_producer.stream_search_data(['cheap hotels', 'flights usa'])

# España - mercado europeo en español
serp_producer.set_country('spain')
serp_producer.stream_search_data(['hoteles madrid', 'vuelos españa'])
```

#### Consumer - Procesamiento Automático
```python
# El consumer detecta automáticamente la información del país
# y la propaga a nivel raíz para facilitar consultas

serp_consumer = MultiCountrySerpAPIConsumer()
serp_consumer.consume_from_kafka(['tourism_analytics'])
# ✅ Los datos se almacenan con campos: country_code, country_name, currency, etc.
```

## 🔌 API REST con Express

El sistema incluye un **API REST optimizado** que actúa como interfaz entre MongoDB y Grafana, ofreciendo endpoints especializados para consultas multi-país.

### 🛠️ Endpoints Disponibles

#### 📊 **Agregaciones Avanzadas**
```bash
POST http://localhost:8080/aggregate
Content-Type: application/json

{
  "collection": "search_results",
  "pipeline": [
    {"$match": {"country_code": "EC"}},
    {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
  ]
}
```

#### 🔍 **Consultas Simples**
```bash
POST http://localhost:8080/find
Content-Type: application/json

{
  "collection": "search_results",
  "filter": {"country_code": "ES"},
  "limit": 50
}
```

#### 📋 **Valores Únicos (para Variables de Grafana)**
```bash
GET http://localhost:8080/distinct/search_results/country_code
# Respuesta: ["EC", "ES", "US", "MX", "DE"]

GET http://localhost:8080/distinct/search_results/domain_category  
# Respuesta: ["hotel", "travel_agency", "airline", "tourism_board"]
```

#### ⚡ **Salud del Sistema**
```bash
GET http://localhost:8080/health
# Respuesta: {"status": "ok"}
```

### 🌍 **Características Especiales Multi-País**

- ✅ **Procesamiento automático de variables de Grafana** (manejo de `.*` y patrones `{EC,ES,US}`)
- ✅ **Optimización de consultas por país** con filtros automáticos
- ✅ **Soporte para agregaciones complejas** multi-país
- ✅ **Conversión automática de timestamps** para filtros temporales
- ✅ **CORS habilitado** para integración con Grafana
- ✅ **Logs detallados** para debugging de consultas

### 📝 **Ejemplos de Uso Programático**

#### Análisis Comparativo España vs Ecuador
```javascript
const response = await fetch('http://localhost:8080/aggregate', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    collection: 'search_results',
    pipeline: [
      {$match: {country_code: {$in: ['ES', 'EC']}}},
      {$group: {
        _id: {country: '$country_code', keyword: '$search_metadata.search_keyword'},
        count: {$sum: 1}
      }},
      {$sort: {count: -1}}
    ]
  })
});
const data = await response.json();
```

#### Top Keywords por País
```javascript
const response = await fetch('http://localhost:8080/aggregate', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({
    collection: 'search_results',
    pipeline: [
      {$group: {
        _id: {
          country: '$country_code',
          keyword: '$search_metadata.search_keyword'
        },
        count: {$sum: 1},
        avg_results: {$avg: '$search_metadata.total_results'}
      }},
      {$sort: {'_id.country': 1, count: -1}}
    ]
  })
});
```

## 🚀 Quick Start

### 1. Iniciar el Stack Completo

```bash
# Ejecutar todo el sistema con un comando
./start-analytics-stack.sh
```

### 2. Acceder a los Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| 📊 **Grafana** | http://localhost:3000 | admin / admin123 |
| 🔌 **Express API** | http://localhost:8080 | N/A |
| 📡 **Kafka** | localhost:9092 | N/A |
| 🗃️ **MongoDB** | localhost:27017 | admin / password123 |

### 3. Ejecutar Producer y Consumer Multi-País

```bash
# 1. Producer de SerpAPI (generar datos multi-país)
jupyter notebook # → Ejecutar Kafka-producer-SerpAPI.ipynb

# 2. Consumer MongoDB (procesar datos con metadatos geográficos)  
jupyter notebook # → Ejecutar Kafka-consumer-SerpAPI.ipynb
```

### 4. Ver Dashboard Multi-País

Ir a **Grafana** → Dashboard → **Multi-Country Tourism Analytics Dashboard**
- Usar variable "🌍 País" para filtrar por país
- Comparar datos entre diferentes mercados

## 📁 Estructura del Proyecto

```
streaming_env/
├── 🐳 docker-compose.yml              # Servicios Docker
├── 🚀 start-analytics-stack.sh        # Script de inicio
├── 🗄️ mongo-init/
│   └── init-db.js                     # Inicialización MongoDB
├── 🔌 mongo-api/                      # API REST Express
│   ├── index.js                       # Servidor API con endpoints
│   ├── Dockerfile                     # Configuración Docker
│   └── package.json                   # Dependencias Node.js
├── 📊 grafana/
│   ├── provisioning/                  # Configuración automática
│   └── dashboards/                    # Dashboards multi-país
├── 📓 Kafka-producer-SerpAPI.ipynb    # Producer multi-país
├── 📓 Kafka-consumer-SerpAPI.ipynb    # Consumer con metadatos geográficos
├── 📓 Get_data_SerpAPI.ipynb          # Producer básico
└── 📓 Get_data_SerpAPI_1.ipynb        # Variante producer
```

## 🔧 Requisitos

- 🐳 **Docker** y **Docker Compose**
- 💾 **4GB RAM** mínimo
- 🌐 **Puertos libres**: 3000 (Grafana), 8080 (API REST), 9092 (Kafka), 27017 (MongoDB)
- 🔑 **SerpAPI Key** para extracción de datos

## 📊 Dashboards Multi-País Incluidos

### 🏆 Multi-Country Tourism Analytics Dashboard

#### 📈 Métricas Globales
- **Throughput por País**: Procesamiento en tiempo real por mercado
- **Distribución Geográfica**: Volumen de datos por país
- **Performance Cross-Country**: Comparativa de latencia y errores

#### 🎯 Análisis por País
- **Top Keywords por País**: Términos más buscados con filtro geográfico
- **Categorías por Mercado**: Distribución por tipo de fuente y país
- **Timeline Comparativo**: Volumen de búsquedas por país y hora
- **Mapa de Calor Geográfico**: Posiciones por categoría y país

#### 🌐 Filtros Geográficos
- **🌍 Variable País**: Filtrar datos por país origen (EC, ES, US, MX, DE)
- **💰 Filtro por Moneda**: Análisis por mercado monetario (USD, EUR, MXN)
- **🗣️ Filtro por Idioma**: Español vs Inglés
- **📊 Comparativas Automáticas**: Datos side-by-side por país

### 🎛️ Controles Interactivos

- 🔄 **Auto-refresh**: Cada 30 segundos
- 📅 **Filtros de Tiempo**: 5m, 1h, 24h, 7d
- 🏷️ **Variables Dinámicas**: Keywords, categorías, países
- 📱 **Responsive**: Optimizado para móvil y desktop

## 🔍 Consultas MongoDB Multi-País

### 📊 Análisis por País Individual
```javascript
// Top keywords en Ecuador
db.search_results.aggregate([
  { $match: { country_code: "EC" } },
  { $group: { 
      _id: "$search_metadata.search_keyword", 
      count: { $sum: 1 } 
  }},
  { $sort: { count: -1 } },
  { $limit: 10 }
])
```

### 🏆 Comparación entre Países
```javascript
// Comparar dominios populares: España vs Estados Unidos
db.search_results.aggregate([
  { $match: { country_code: { $in: ["ES", "US"] } } },
  { $group: { 
      _id: { 
          country: "$country_code", 
          domain: "$domain" 
      }, 
      count: { $sum: 1 } 
  }},
  { $sort: { "_id.country": 1, count: -1 } }
])
```

### 💰 Análisis por Moneda/Mercado
```javascript
// Agrupar por moneda para análisis de mercado
db.search_results.aggregate([
  { $group: { 
      _id: "$currency", 
      paises: { $addToSet: "$country_name" },
      total_resultados: { $sum: 1 },
      keywords_unicos: { $addToSet: "$search_metadata.search_keyword" }
  }}
])
```

### 🔍 Consultas Avanzadas
```javascript
// Análisis de competencia por país
db.search_results.aggregate([
  {$match: {position: {$lte: 5}}},
  {$group: {
    _id: {domain: '$domain', country: '$country_code'},
    appearances: {$sum: 1},
    avg_position: {$avg: '$position'}
  }},
  {$sort: {appearances: -1}}
])
```

## 🎯 Casos de Uso Multi-País

### 🏢 Business Intelligence Global
1. **Análisis de Mercado Regional**
   - Comparar tendencias Ecuador vs España  
   - Identificar oportunidades de expansión
   - Benchmark contra mercados desarrollados (USA, Alemania)

2. **Estrategia de Contenido Global**
   - Keywords efectivos por país y idioma
   - Adaptación cultural de mensajes
   - SEO internacional optimizado

3. **Monitoreo de Competencia Cross-Border**
   - Competidores locales vs internacionales
   - Estrategias por mercado geográfico
   - Oportunidades de nicho por país

### 📊 Data Analytics Internacional
1. **Investigación de Mercado Comparativa**
   - Demanda turística por región
   - Patrones de búsqueda culturales
   - Seasonality por país y moneda

2. **Pricing Intelligence Multi-Moneda**
   - Análisis de precios en USD, EUR, MXN
   - Competitividad por mercado local
   - Oportunidades de arbitraje geográfico

### 🔍 Research & Intelligence
- **Sentiment Analysis Multi-Cultural**: Análisis de contenido por país
- **Keyword Research Geográfico**: Términos emergentes por mercado
- **Content Strategy Internacional**: Gaps y oportunidades por país
- **Market Research Comparativo**: Insights de demanda por región

### 🔌 Integración y Desarrollo
1. **APIs Terceros**
   - Integrar datos del API REST con aplicaciones externas
   - Desarrollar dashboards personalizados usando endpoints
   - Automatizar reportes multi-país via API

2. **Análisis Programático**
   - Scripts automáticos para análisis cross-country
   - Alertas customizadas basadas en endpoints del API
   - Exportación de datos para Machine Learning

## 🚨 Alertas Multi-País Configuradas

- ⚡ **Performance por País**: < 10 msg/min por mercado
- 📊 **Rate de Errores Geográfico**: > 5% por país
- 🎯 **Keywords Trending por País**: Actividad > 50 búsquedas/hora
- 🔄 **Sistema Offline**: Sin datos > 5 minutos por mercado
- 🌍 **Desbalance Geográfico**: Un país > 80% del tráfico total

## 📈 Métricas Clave Multi-País

| Métrica | Descripción | Umbral Global | Por País |
|---------|-------------|---------------|----------|
| **Throughput** | Mensajes procesados/min | > 250 | > 50 |
| **Distribución** | Balance entre países | 10-40% | Cada país |
| **Latencia** | Tiempo de procesamiento | < 100ms | < 150ms |
| **Error Rate** | % de errores | < 2% | < 5% |
| **Cobertura** | Países activos | = 5 | 100% |

## 🔧 Troubleshooting Multi-País

### Verificar Datos por País
```bash
# Conectar a MongoDB
docker exec my-mongo mongosh tourism_analytics

# Verificar países en los datos
db.search_results.distinct("country_code")
# Resultado esperado: ["EC", "ES", "US", "MX", "DE"]

# Verificar distribución por país
db.search_results.aggregate([
  {$group: {_id: "$country_code", count: {$sum: 1}}},
  {$sort: {count: -1}}
])
```

### Servicios no inician
```bash
# Verificar Docker
docker info

# Reiniciar stack
docker-compose down && docker-compose up -d

# Ver logs específicos
docker-compose logs -f grafana
docker-compose logs -f mongo
```

### API REST no responde
```bash
# Verificar estado del API
curl -s http://localhost:8080/health
# Respuesta esperada: {"status":"ok"}

# Verificar logs del API
docker-compose logs -f mongo-api

# Probar conexión desde Grafana al API
docker exec my-grafana curl -s http://mongo-api:8080/health
```

### Dashboard sin filtros geográficos
```bash
# Verificar variables en Grafana usando API
curl -s http://localhost:8080/distinct/search_results/country_code

# Verificar conexión Grafana → API → MongoDB
curl -X POST http://localhost:8080/aggregate \
  -H "Content-Type: application/json" \
  -d '{"collection":"search_results","pipeline":[{"$group":{"_id":"$country_code","count":{"$sum":1}}}]}'

# Verificar variables en Grafana
curl -s "http://admin:admin123@localhost:3000/api/dashboards/uid/tourism-analytics"
```

## ✅ Verificación de Funcionamiento Multi-País

### 🔍 Verificar Configuración Completa
```bash
# 1. Verificar países en MongoDB
docker exec my-mongo mongosh tourism_analytics --eval "db.search_results.distinct('country_code')"

# 2. Verificar información completa por país
docker exec my-mongo mongosh tourism_analytics --eval "db.search_results.findOne({country_code: 'EC'})"

# 3. Verificar dashboards en Grafana
curl -s "http://admin:admin123@localhost:3000/api/search?query=Multi-Country"
```

### 📊 Verificar Grafana Multi-País
1. Ir a http://localhost:3000
2. Dashboard: "Multi-Country Tourism Analytics Dashboard"
3. Variable "🌍 País" debe mostrar: Ecuador, España, Estados Unidos, México, Alemania
4. Filtrar por cada país y verificar que los datos cambien
5. Probar comparativas seleccionando múltiples países

## 🎉 ¡Implementación Multi-País Completa!

El sistema ahora soporta **análisis completo multi-país** con:
- ✅ **5 países pre-configurados** (EC, ES, US, MX, DE)
- ✅ **API REST optimizado** (Express) para consultas avanzadas
- ✅ **Cambio dinámico de configuración** por país
- ✅ **Filtros geográficos en Grafana** con variables
- ✅ **Consultas optimizadas por país** con índices
- ✅ **Comparativas automáticas cross-country**
- ✅ **Soporte para múltiples idiomas y monedas**
- ✅ **Pipeline de datos en tiempo real** por mercado
- ✅ **Dashboard profesional** con métricas multi-país
- ✅ **Sistema de alertas geográficas** para monitoreo
- ✅ **Endpoints programáticos** para integración externa
- ✅ **Arquitectura escalable** lista para más países

## 🚀 Próximos Pasos

### 🔄 Ejecución de Pruebas Multi-País
1. **Iniciar servicios**: `./start-analytics-stack.sh`
2. **Configurar API key** en `Kafka-producer-SerpAPI.ipynb`
3. **Ejecutar producer multi-país**: Descomenta `run_multi_country_analysis()`
4. **Ejecutar consumer**: `Kafka-consumer-SerpAPI.ipynb`
5. **Ver resultados en Grafana**: http://localhost:3000

### 📊 Personalización Avanzada
- **Agregar nuevos países**: Modifica `country_configs` en el Producer
- **Crear paneles específicos**: Usa las consultas multi-país de ejemplo
- **Configurar alertas geográficas**: Por país o cross-country
- **Extender API REST**: Agregar endpoints específicos en `mongo-api/index.js`
- **Integrar APIs adicionales**: Por mercado específico usando endpoints REST
- **Implementar ML geográfico**: Usar API para alimentar modelos de predicción
- **Desarrollar aplicaciones custom**: Consumir datos via endpoints REST

---

**¡Tu sistema de analytics de turismo multi-país está listo para generar insights globales!** 🌍🏝️✨

Para soporte técnico o consultas específicas por país, revisa los logs del sistema o las consultas de ejemplo por mercado geográfico. 