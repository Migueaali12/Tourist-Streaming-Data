#!/bin/bash

# 🚀 Tourism Analytics Stack Starter
# Script para inicializar todo el ecosistema de analytics con Grafana + MongoDB + Kafka

set -e

echo "🏝️ ========================================"
echo "🚀 INICIANDO TOURISM ANALYTICS STACK"
echo "🏝️ ========================================"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Función para logging con colores
log_info() {
    echo -e "${BLUE}ℹ️  INFO: $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ SUCCESS: $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  WARNING: $1${NC}"
}

log_error() {
    echo -e "${RED}❌ ERROR: $1${NC}"
}

# Verificar que Docker está corriendo
if ! docker info > /dev/null 2>&1; then
    log_error "Docker no está corriendo. Por favor inicia Docker primero."
    exit 1
fi

log_info "Docker está corriendo correctamente"

# Crear directorios necesarios
log_info "Creando estructura de directorios..."
mkdir -p mongo-init
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards
mkdir -p grafana/dashboards
mkdir -p mongo-api
log_success "Directorios creados"

# Verificar que los archivos de configuración existen
if [ ! -f "docker-compose.yml" ]; then
    log_error "docker-compose.yml no encontrado en el directorio actual"
    exit 1
fi

if [ ! -f "mongo-init/init-db.js" ]; then
    log_warning "Script de inicialización de MongoDB no encontrado"
    log_info "Creando script básico de inicialización..."
fi

if [ ! -f "mongo-api/package.json" ]; then
    log_warning "No se encontró package.json en mongo-api. Recuerda crear la API Express para MongoDB."
fi

# Detener servicios existentes si están corriendo
log_info "Deteniendo servicios existentes..."
docker-compose down --remove-orphans

# Limpiar volúmenes si es necesario (opcional)
read -p "¿Deseas limpiar los volúmenes de datos existentes? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_warning "Eliminando volúmenes existentes..."
    docker-compose down -v
    docker volume prune -f
    log_success "Volúmenes limpiados"
fi

# Iniciar servicios
log_info "Iniciando servicios del stack de analytics..."
docker-compose up --build -d

# Verificar que los servicios están corriendo
log_info "Verificando estado de los servicios..."

# Función para verificar si un puerto está disponible
check_service() {
    local service_name=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:$port > /dev/null 2>&1; then
            log_success "$service_name está corriendo en puerto $port"
            return 0
        fi
        
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    log_error "$service_name no responde en puerto $port después de $max_attempts intentos"
    return 1
}

echo
log_info "Esperando que los servicios se inicien..."

# Verificar MongoDB
log_info "Verificando MongoDB..."
sleep 5
if docker exec my-mongo mongosh --eval "db.runCommand('ping')" > /dev/null 2>&1; then
    log_success "MongoDB está corriendo correctamente"
else
    log_error "MongoDB no está respondiendo"
fi

# Verificar Kafka
log_info "Verificando Kafka..."
if docker exec my-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    log_success "Kafka está corriendo correctamente"
else
    log_warning "Kafka está iniciándose... (esto puede tomar unos minutos)"
fi

# Verificar Grafana
log_info "Verificando Grafana..."
check_service "Grafana" 3000

# Verificar API intermedia (mongo-api)
log_info "Verificando API intermedia (mongo-api)..."
check_service "Mongo-API" 8080


echo
log_success "🎉 ¡STACK DE ANALYTICS INICIADO CORRECTAMENTE!"
echo
echo "🌐 ========================================"
echo "📊 SERVICIOS DISPONIBLES:"
echo "🌐 ========================================"
echo
echo -e "${PURPLE}📊 Grafana Dashboard:${NC}"
echo "   🔗 URL: http://localhost:3000"
echo "   👤 Usuario: admin"
echo "   🔐 Contraseña: admin123"
echo

echo -e "${PURPLE}📡 Kafka:${NC}"
echo "   🔗 Bootstrap Server: localhost:9092"
echo "   📂 Topics disponibles: tourism_analytics, trending_analysis, competition_analysis"
echo
echo -e "${PURPLE}🗃️  MongoDB:${NC}"
echo "   🔗 Conexión: mongodb://localhost:27017"
echo "   🏪 Base de datos: tourism_analytics"
echo "   👤 Usuario Grafana: grafana_user / grafana_password"
echo
echo -e "${PURPLE}🧩 API intermedia para MongoDB (Express + Infinity):${NC}"
echo "   🔗 URL: http://localhost:8080"
echo "   📖 Endpoints útiles: /health, /search, /query_custom"
echo "   💡 Úsalo como fuente de datos en Grafana con el plugin Infinity (JSON/REST API)"
echo
echo "🌐 ========================================"
echo "🚀 PRÓXIMOS PASOS:"
echo "🌐 ========================================"
echo
echo "1️⃣  Ejecuta tu Producer de SerpAPI para generar datos"
echo "2️⃣  Ejecuta tu Consumer para procesar datos a MongoDB"
echo "3️⃣  Ve a Grafana y explora el dashboard 'Tourism Analytics'"
echo "4️⃣  Personaliza las visualizaciones según tus necesidades"
echo
echo -e "${GREEN}🎯 ¡Tu pipeline de analytics está listo para usar!${NC}"
echo

# Mostrar logs en tiempo real si se solicita
read -p "¿Deseas ver los logs en tiempo real? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Mostrando logs en tiempo real (Ctrl+C para salir)..."
    docker-compose logs -f
fi 