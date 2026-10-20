#!/usr/bin/env bash
set -euo pipefail

# M3 Headline Crash-Recovery Proof Script
# Starts the Spring Boot ingestion pipeline in fixture mode, repeatedly sends SIGKILL (kill -9)
# at random moments during sync, restarts with mode=RESUME until complete, and verifies
# 0 lost rows, identical checksum, idempotent replay, and exact MD5 content checksum across 10 consecutive runs.

RUNS="${RUNS:-10}"
FIXTURE_RECORDS="${FIXTURE_RECORDS:-50000}"
PAGE_SIZE="${PAGE_SIZE:-2000}"
KILLS_PER_RUN="${KILLS_PER_RUN:-3}"
APP_JAR="app/target/app-0.1.0-SNAPSHOT-exec.jar"

if [[ ! -f "$APP_JAR" ]]; then
  echo "[kill-and-resume] Building executable jar..."
  mvn -B package -DskipTests
fi

docker compose -f deploy/docker-compose.yml up -d postgres kafka

echo "[kill-and-resume] Waiting for PostgreSQL and Kafka to become healthy..."
until docker exec ingest-postgres pg_isready -U ingest -d ingest >/dev/null 2>&1; do sleep 1; done
until docker exec ingest-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done

start_app() {
  java -Xmx512m -jar "$APP_JAR" \
    --ingest.fixture.enabled=true \
    --ingest.fixture.records="$FIXTURE_RECORDS" \
    >/tmp/ingest-app.log 2>&1 &
  APP_PID=$!
  for _ in {1..60}; do
    if curl -sf http://localhost:8080/actuator/health >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "[kill-and-resume] ERROR: App failed to start. Logs:"
  cat /tmp/ingest-app.log
  exit 1
}

BASELINE_CHECKSUM=""

for run in $(seq 1 "$RUNS"); do
  echo "================================================================"
  echo "[Run $run/$RUNS] Resetting stream table & checkpoint..."
  docker exec ingest-postgres psql -U ingest -d ingest -c \
    "DROP TABLE IF EXISTS rdw_vehicles; DELETE FROM extract_checkpoint WHERE stream='rdw_vehicles'; DELETE FROM stream_schema WHERE stream='rdw_vehicles';" >/dev/null

  start_app
  echo "[Run $run/$RUNS] Triggering FULL sync ($FIXTURE_RECORDS records)..."
  curl -sf -X POST "http://localhost:8080/api/v1/syncs?async=true" \
    -H "Content-Type: application/json" \
    -d "{\"connector\":\"rdw\",\"stream\":\"rdw_vehicles\",\"mode\":\"FULL\",\"pageSize\":$PAGE_SIZE}" >/dev/null

  for k in $(seq 1 "$KILLS_PER_RUN"); do
    SLEEP_SEC=$(awk -v min=0.4 -v max=1.5 'BEGIN{srand(); print min+rand()*(max-min)}')
    sleep "$SLEEP_SEC"
    echo "[Run $run/$RUNS] Sending kill -9 (SIGKILL #$k) to PID $APP_PID after ${SLEEP_SEC}s..."
    kill -9 "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true

    start_app
    echo "[Run $run/$RUNS] Restarted app (PID $APP_PID); triggering RESUME..."
    curl -sf -X POST "http://localhost:8080/api/v1/syncs?async=true" \
      -H "Content-Type: application/json" \
      -d "{\"connector\":\"rdw\",\"stream\":\"rdw_vehicles\",\"mode\":\"RESUME\",\"pageSize\":$PAGE_SIZE}" >/dev/null
  done

  echo "[Run $run/$RUNS] Waiting for sync completion..."
  for _ in {1..120}; do
    COUNT=$(docker exec ingest-postgres psql -U ingest -d ingest -tAc "SELECT COUNT(*) FROM rdw_vehicles" 2>/dev/null || echo "0")
    if [[ "$COUNT" -eq "$FIXTURE_RECORDS" ]]; then
      break
    fi
    sleep 0.5
  done

  kill -15 "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true

  FINAL_COUNT=$(docker exec ingest-postgres psql -U ingest -d ingest -tAc "SELECT COUNT(*) FROM rdw_vehicles")
  CHECKSUM=$(docker exec ingest-postgres psql -U ingest -d ingest -tAc "SELECT md5(string_agg(pk || ':' || source_hash, ',' ORDER BY pk)) FROM rdw_vehicles")

  if [[ "$FINAL_COUNT" -ne "$FIXTURE_RECORDS" ]]; then
    echo "[Run $run/$RUNS] FAILED: expected $FIXTURE_RECORDS rows, got $FINAL_COUNT"
    exit 1
  fi

  if [[ -z "$BASELINE_CHECKSUM" ]]; then
    BASELINE_CHECKSUM="$CHECKSUM"
  elif [[ "$CHECKSUM" != "$BASELINE_CHECKSUM" ]]; then
    echo "[Run $run/$RUNS] FAILED: checksum mismatch ($CHECKSUM != $BASELINE_CHECKSUM)"
    exit 1
  fi

  echo "[Run $run/$RUNS] PASS: rows=$FINAL_COUNT (0 lost rows, identical checksum, idempotent replay), md5=$CHECKSUM"
done

echo "================================================================"
echo "ALL $RUNS CONSECUTIVE KILL-AND-RESUME RUNS PASSED (0 lost rows, identical checksum, idempotent replay)!"
