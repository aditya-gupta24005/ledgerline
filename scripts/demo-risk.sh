#!/usr/bin/env bash
# Trades flow into live positions and P&L; a large position raises one limit alert.
# Needs: docker compose stack (incl. keycloak), order-service on 8081, risk-service on 8083.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/token.sh

DOCKER="${DOCKER:-docker}"
ORDER_URL="${ORDER_URL:-http://localhost:8081}"
RISK_URL="${RISK_URL:-http://localhost:8083}"
# Not `tr </dev/urandom | head`: under pipefail, tr's SIGPIPE would abort the script.
SYMBOL="$(python3 -c 'import random, string; print("".join(random.choices(string.ascii_uppercase, k=5)))')"
# Accounts are Keycloak users now; a fresh symbol keeps this run's positions separate from earlier ones.
TRADER=alice
MARKET=bob
JSON='Content-Type: application/json'
ALICE=$(auth alice); BOB=$(auth bob); RITA=$(auth rita)

order() { # account, side, price, quantity
  local header; [ "$1" = alice ] && header=$ALICE || header=$BOB
  curl -s -o /dev/null -w "$1 $2 $4 @ $3: HTTP %{http_code}\n" -X POST "$ORDER_URL/api/v1/orders" -H "$header" -H "$JSON" \
    -d "{\"symbol\":\"$SYMBOL\",\"side\":\"$2\",\"type\":\"LIMIT\",\"price\":$3,\"quantity\":$4}"
}

positions() { # account, read as rita (RISK)
  curl -s "$RISK_URL/api/v1/risk/accounts/$1/positions" -H "$RITA"; echo
}

echo "== $TRADER buys 100 $SYMBOL at 50, then 100 more at 60 (average cost 55)"
order "$MARKET" SELL 50 100;  order "$TRADER" BUY 50 100
order "$MARKET" SELL 60 100;  order "$TRADER" BUY 60 100
echo "== $TRADER sells 50 at 70 (realised +750)"
order "$MARKET" BUY 70 50;    order "$TRADER" SELL 70 50
sleep 3
echo "== $TRADER positions (net 150, avg 55, realised 750, marked at 70 -> unrealised 2250)"
positions "$TRADER"
echo "== $MARKET positions (short 150)"
positions "$MARKET"

echo "== $TRADER buys 20000 more at 70 (notional 20,150 x 70 = 1,410,500 > 1,000,000 limit)"
order "$MARKET" SELL 70 20000; order "$TRADER" BUY 70 20000
sleep 3
echo "== Risk alerts for $SYMBOL"
"$DOCKER" compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ledgerline.risk.alerts --from-beginning --timeout-ms 5000 --property print.key=true 2>/dev/null \
  | grep "|$SYMBOL" || echo "(no alert found)"
