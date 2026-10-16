#!/usr/bin/env bash
# Starts the full stack: Kafka + ZooKeeper + MongoDB + Express API + Grafana.
# Usage: ./start-analytics-stack.sh [--reset] [--logs]
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${BLUE}[info]${NC} $1"; }
ok()    { echo -e "${GREEN}[ok]${NC} $1"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $1"; }
fail()  { echo -e "${RED}[fail]${NC} $1"; }

RESET=0
SHOW_LOGS=0
for arg in "$@"; do
  case "$arg" in
    --reset) RESET=1 ;;
    --logs)  SHOW_LOGS=1 ;;
    *) fail "Unknown option: $arg"; exit 1 ;;
  esac
done

# docker compose v2, falling back to the legacy binary.
if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  fail "Docker Compose not found. Install Docker Desktop or the compose plugin."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  fail "Docker daemon is not running."
  exit 1
fi

if [ ! -f .env ]; then
  warn "No .env found. Copying .env.example (dev defaults, change the passwords)."
  cp .env.example .env
fi

info "Stopping any previous containers..."
"${DC[@]}" down --remove-orphans

if [ "$RESET" -eq 1 ]; then
  warn "Removing volumes (all stored data will be lost)..."
  "${DC[@]}" down -v
fi

info "Building and starting services..."
"${DC[@]}" up --build -d

wait_for() {
  local name=$1 url=$2 attempts=30
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      ok "$name is up ($url)"
      return 0
    fi
    sleep 2
  done
  fail "$name did not respond at $url"
  return 1
}

info "Waiting for services..."
wait_for "MongoDB API" "http://localhost:8080/health" || true
wait_for "Grafana"     "http://localhost:3000/api/health" || true

if "${DC[@]}" exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  ok "Kafka is up"
else
  warn "Kafka is still starting (it can take a minute)"
fi

cat <<'EOF'

Services
  Grafana      http://localhost:3000   (admin / $GRAFANA_ADMIN_PASSWORD)
  Express API  http://localhost:8080   (/health, /aggregate, /find, /distinct/:collection/:field)
  Kafka        localhost:9092
  MongoDB      localhost:27017

Next steps
  1. Put your SerpAPI key in .env (SERPAPI_KEY=...)
  2. Run Kafka-producer-SerpAPI.ipynb to stream results into the tourism_analytics topic
  3. Run Kafka-consumer-SerpAPI.ipynb to land them in MongoDB
  4. Open the "Tourism Analytics" dashboard in Grafana
EOF

if [ "$SHOW_LOGS" -eq 1 ]; then
  info "Following logs (Ctrl+C to exit)..."
  "${DC[@]}" logs -f
fi