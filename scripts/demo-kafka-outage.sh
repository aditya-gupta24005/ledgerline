#!/usr/bin/env bash
# Orders keep working while Kafka is down; the outbox delivers the trades once Kafka is back.
# Needs: docker compose stack (incl. keycloak), order-service on 8081, settlement-service on 8082.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/token.sh

DOCKER="${DOCKER:-docker}"
ORDER_URL="${ORDER_URL:-http://localhost:8081}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
# Longer than the relay can block on one send (max.block.ms 5 s + send timeout 5 s), so failures show up.
OUTAGE_SECONDS="${OUTAGE_SECONDS:-15}"
# Not `tr </dev/urandom | head`: under pipefail, tr's SIGPIPE would abort the script.
SYMBOL="$(python3 -c 'import random, string; print("".join(random.choices(string.ascii_uppercase, k=6)))')"
JSON='Content-Type: application/json'
ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita); OSCAR=$(auth oscar)

pending() {
  curl -s "$ORDER_URL/actuator/metrics/ledgerline.outbox.pending" -H "$OSCAR" | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["measurements"][0]["value"]))'
}

outbox_rows() {
  "$DOCKER" compose exec -T postgres psql -U ledgerline -d ledgerline -c \
    "SELECT id, attempts, left(last_error, 60) AS last_error, created_at::time(3) AS created, published_at::time(3) AS published
     FROM order_service.outbox_events WHERE message_key = '$SYMBOL' ORDER BY id"
}

echo "== Stopping Kafka"
"$DOCKER" compose stop kafka

echo "== Crossing orders for $SYMBOL while Kafka is down (alice sells, bob buys)"
curl -s -o /dev/null -w "sell: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"
curl -s -o /dev/null -w "buy:  HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$BOB" -H "$JSON" \
  -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":50,\"quantity\":5}"

echo "== Keeping Kafka down for ${OUTAGE_SECONDS}s"
sleep "$OUTAGE_SECONDS"
echo "outbox pending while Kafka is down: $(pending)"
outbox_rows

echo "== Starting Kafka"
"$DOCKER" compose start kafka
for _ in $(seq 1 60); do
  [ "$(pending)" = "0" ] && break
  sleep 1
done
echo "outbox pending after Kafka is back: $(pending)"
outbox_rows

for _ in $(seq 1 30); do
  [[ "$(curl -s "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$RITA")" == *"$SYMBOL"* ]] && break
  sleep 1
done
echo "== bob's balances as seen by rita (look for $SYMBOL)"
curl -s "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$RITA"; echo
