#!/usr/bin/env bash
# A malformed trade message is dead-lettered instead of blocking settlement; a valid dead letter can be replayed.
# Needs: docker compose stack running, settlement-service on 8082.
set -euo pipefail

DOCKER="${DOCKER:-docker}"
SETTLEMENT_URL="${SETTLEMENT_URL:-http://localhost:8082}"
TRADE_ID="T-demo-$(date +%s)"

produce() { # topic, key, value
  printf '%s|%s\n' "$2" "$3" | "$DOCKER" compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --property parse.key=true --property 'key.separator=|'
}

echo "== Publishing a malformed trade"
produce ledgerline.trades.executed POISON '{"bad":'
sleep 3
echo "== Latest dead letters"
curl -s "$SETTLEMENT_URL/api/v1/ops/dead-letters?limit=3"; echo

echo "== Putting a valid trade ($TRADE_ID) on the dead-letter topic, then replaying it"
produce ledgerline.trades.executed.DLT ACME "{\"tradeId\":\"$TRADE_ID\",\"symbol\":\"ACME\",\"currency\":\"USD\",\"price\":101.5,\"quantity\":10,\"buyOrderId\":1,\"buyAccountId\":\"replay-buyer\",\"sellOrderId\":2,\"sellAccountId\":\"replay-seller\",\"aggressorSide\":\"BUY\",\"executedAt\":\"2026-09-15T10:00:00Z\"}"
sleep 2
COORDINATES="$(curl -s "$SETTLEMENT_URL/api/v1/ops/dead-letters?limit=50" | python3 -c "
import json, sys
for record in json.load(sys.stdin):
    if '$TRADE_ID' in (record['payload'] or ''):
        print(record['partition'], record['offset']); break
")"
read -r PARTITION OFFSET <<< "$COORDINATES"
echo "found at partition $PARTITION offset $OFFSET"
curl -s -X POST "$SETTLEMENT_URL/api/v1/ops/dead-letters/$PARTITION/$OFFSET/replay"; echo
sleep 3
echo "== replay-buyer balances (ACME +10, USD -1015)"
curl -s "$SETTLEMENT_URL/api/v1/accounts/replay-buyer/balances"; echo
