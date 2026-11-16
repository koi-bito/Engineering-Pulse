#!/usr/bin/env bash
set -euo pipefail

# Gracefully drain WebSocket connections before container shutdown
echo "Sending WebSocket drain signal..."
curl -fs -X POST http://localhost:8000/admin/drain-ws || echo "Drain endpoint unreachable or skipped."
sleep 2
echo "Drain complete."

