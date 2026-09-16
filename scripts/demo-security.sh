#!/usr/bin/env bash
# Who can do what: traders act on their own account, risk reads everything, ops runs the dead-letter tools.
# Needs: docker compose stack (incl. keycloak), order-service 8081, settlement-service 8082, risk-service 8083.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/token.sh

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
RISK_URL="${RISK_URL:-http://localhost:8083}"
SYMBOL="$(python3 -c 'import random, string; print("".join(random.choices(string.ascii_uppercase, k=5)))')"
JSON='Content-Type: application/json'

show() { # expected, label, then curl args
  local expected=$1 label=$2; shift 2
  local code; code=$(curl -s -o /tmp/ll-demo-body -w '%{http_code}' "$@")
  local mark="ok  "; [ "$code" = "$expected" ] || mark="FAIL"
  printf '%s %-52s HTTP %s (expected %s)\n' "$mark" "$label" "$code" "$expected"
}

order_id() { python3 -c 'import json,sys; print(json.load(sys.stdin)["orderId"])' </tmp/ll-demo-body; }

echo "== Tokens come from Keycloak (password grant, dev only)"
ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita); OSCAR=$(auth oscar)

echo "== Trading on $SYMBOL: the account is the token's username, not a request field"
show 401 "no token: place order"          -X POST "$ORDER_URL/api/v1/orders" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show 403 "oscar (OPS): place order"        -X POST "$ORDER_URL/api/v1/orders" -H "$OSCAR" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show 201 "alice (TRADER): sell 5 @ 10"     -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show 201 "alice: rest a second sell @ 11"  -X POST "$ORDER_URL/api/v1/orders" -H "$ALICE" -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":11,\"quantity\":1}"
RESTING=$(order_id)
show 201 "bob (TRADER): buy 5 @ 10"        -X POST "$ORDER_URL/api/v1/orders" -H "$BOB"   -H "$JSON" -d "{\"symbol\":\"$SYMBOL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":10,\"quantity\":5}"
show 404 "bob: cancel alice's resting order"     -X DELETE "$ORDER_URL/api/v1/orders/$SYMBOL/$RESTING" -H "$BOB"
show 204 "alice: cancel her own resting order"   -X DELETE "$ORDER_URL/api/v1/orders/$SYMBOL/$RESTING" -H "$ALICE"
sleep 3

echo "== Balances"
show 200 "bob reads bob"                   "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$BOB"
show 403 "alice reads bob"                 "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$ALICE"
show 200 "rita (RISK) reads bob"           "$SETTLEMENT_URL/api/v1/accounts/bob/balances" -H "$RITA"
show 401 "no token reads bob"              "$SETTLEMENT_URL/api/v1/accounts/bob/balances"

echo "== Positions"
show 200 "bob reads bob"                   "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$BOB"
show 403 "alice reads bob"                 "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$ALICE"
show 200 "rita (RISK) reads bob"           "$RISK_URL/api/v1/risk/accounts/bob/positions" -H "$RITA"

echo "== Dead-letter ops API"
show 403 "alice lists dead letters"        "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$ALICE"
show 403 "rita lists dead letters"         "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$RITA"
show 200 "oscar (OPS) lists dead letters"  "$SETTLEMENT_URL/api/v1/ops/dead-letters" -H "$OSCAR"

echo "== Health stays public"
show 200 "no token: order-service health"  "$ORDER_URL/actuator/health"
