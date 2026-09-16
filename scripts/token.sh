#!/usr/bin/env bash
# Dev-only: password-grant tokens from the ledgerline-cli client. Usage: source scripts/token.sh; token alice
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8180}"

token() { # username (password = username)
  curl -s -X POST "$KEYCLOAK_URL/realms/ledgerline/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=ledgerline-cli -d "username=$1" -d "password=$1" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}

auth() { # username -> header value
  echo "Authorization: Bearer $(token "$1")"
}
