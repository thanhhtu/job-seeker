#!/usr/bin/env bash
# Public-share the local app via a SINGLE ngrok tunnel.
#
# How it works:
#   - Vite (port 5173) serves the frontend AND proxies /api -> local backend (8080).
#   - So one ngrok tunnel on :5173 exposes the whole app to anyone.
#
# Prerequisites (run once):
#   ngrok config add-authtoken <YOUR_TOKEN>   # from https://dashboard.ngrok.com
#
# Usage:
#   1. Start the backend:   uv run uvicorn src.api.app:app --reload   (port 8080)
#   2. Start the frontend:  cd frontend && npm run dev                 (port 5173)
#   3. Run this script:     ./frontend/share-ngrok.sh
#
# ngrok prints a public https URL — share that with anyone.

set -euo pipefail

PORT="${1:-5173}"

if ! command -v ngrok >/dev/null 2>&1; then
  echo "ngrok not installed. Install with: brew install ngrok" >&2
  exit 1
fi

if ! ngrok config check >/dev/null 2>&1; then
  echo "ngrok authtoken not configured." >&2
  echo "Sign up at https://dashboard.ngrok.com/signup, then run:" >&2
  echo "  ngrok config add-authtoken <YOUR_TOKEN>" >&2
  exit 1
fi

echo "Exposing http://localhost:${PORT} to the internet..."
echo "Make sure both the backend (8080) and frontend (${PORT}) are already running."
echo "Watch for the 'Forwarding  https://...ngrok-free.app' line below — that is your public link."
echo "Local inspector / link list also available at: http://localhost:4040"
echo

# Use plain log output so the public URL is always printed (and not hidden by the
# full-screen TUI, which can exit early when launched from a script).
exec ngrok http "${PORT}" --log=stdout --log-format=logfmt
