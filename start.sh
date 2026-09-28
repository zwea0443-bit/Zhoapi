#!/bin/bash
set -e

echo "═══════════════════════════════════════════════════════════"
echo "  Jinx API — Tor + Shopify Checker"
echo "═══════════════════════════════════════════════════════════"

# ═══ Start Tor in background ═══
echo "⏳ Starting Tor pool..."

# Create Tor configs
for i in $(seq 0 4); do
    SOCKS_PORT=$((9050 + i * 2))
    CTRL_PORT=$((9051 + i * 2))
    DATA_DIR="/tmp/tor_api_$i"
    mkdir -p $DATA_DIR
    
    cat > /tmp/torrc_api_$i <<EOF
SocksPort $SOCKS_PORT
ControlPort $CTRL_PORT
DataDirectory $DATA_DIR
CookieAuthentication 0
Log notice file /tmp/tor_$i.log
MaxCircuitDirtiness 10
EOF
    
    tor -f /tmp/torrc_api_$i &
    echo "  ✅ Tor instance $i started (socks=$SOCKS_PORT)"
done

# ═══ Wait for Tor bootstrap ═══
echo "⏳ Waiting for Tor to bootstrap (60s)..."
sleep 60

# ═══ Check Tor status ═══
for i in $(seq 0 4); do
    PORT=$((9050 + i * 2))
    if curl -s --socks5 127.0.0.1:$PORT https://api.ipify.org --max-time 10; then
        echo ""
        echo "  ✅ Tor $i (port $PORT) IP fetched"
    else
        echo "  ⚠️ Tor $i (port $PORT) not ready"
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Starting API server..."
echo "═══════════════════════════════════════════════════════════"

# ═══ Start API ═══
exec python3 api.py
