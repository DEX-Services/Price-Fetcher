#!/usr/bin/env bash
#
# render-cli.sh — helpers for deploying Price-Fetcher to Render (Option A).
#
# Requires the Render CLI (https://render.com/docs/cli):
#     npm install -g @renderinc/cli
#     render login          # opens browser / device-code flow
#
# Usage:
#   ./scripts/render-cli.sh validate     # sanity-check config matches the service
#   ./scripts/render-cli.sh env-sync     # push REDIS_SERVICE_URI + ASSETS from .env
#   ./scripts/render-cli.sh deploy       # trigger a manual deploy of a service id
#
# Set RENDER_SERVICE_ID to your worker's id (dashboard URL: .../srv-XXXX, or
# `render list services`).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

SERVICE_ID="${RENDER_SERVICE_ID:-}"

require_render() {
  if ! command -v render >/dev/null 2>&1; then
    echo "ERROR: Render CLI not found. Install it: npm install -g @renderinc/cli"
    exit 1
  fi
  render whoami >/dev/null 2>&1 || { echo "ERROR: not logged in. Run: render login"; exit 1; }
}

# Read a KEY=vALUE (ignore comments/blank) from our .env style file.
env_get() {
  local key="$1"
  awk -F= -v k="$key" \
    '$1 == k { gsub(/^[ \t"+]+|[ \t"+]+$/, "", $2); print $2; exit }' \
    "$ENV_FILE" 2>/dev/null || true
}

validate() {
  echo "== Price-Fetcher config (from .env) =="
  echo "  REDIS_SERVICE_URI = $(env_get REDIS_SERVICE_URI | sed -E 's#(rediss?://[^:]+:)[^@]+@#\1****@#' )"
  echo "  ASSETS            = $(env_get ASSETS)"
  echo "  PRICE_QUOTE       = $(env_get PRICE_QUOTE)"
  echo "  PRICE_KEY_PREFIX  = $(env_get PRICE_KEY_PREFIX)"
  echo "  PRICE_STALE_TTL   = $(env_get PRICE_STALE_TTL)"
  echo "  PRICE_HEALTH_ADDR = $(env_get PRICE_HEALTH_ADDR)"
  echo
  echo "These are the values Render will receive. If any are wrong, edit .env first."
}

env_sync() {
  require_render
  if [[ -z "$SERVICE_ID" ]]; then
    echo "ERROR: set RENDER_SERVICE_ID (your 'srv-...' id) first." >&2
    exit 1
  fi
  local uri
  uri="$(env_get REDIS_SERVICE_URI)"
  if [[ -z "$uri" ]]; then
    echo "ERROR: REDIS_SERVICE_URI missing in $ENV_FILE" >&2
    exit 1
  fi
  echo "Syncing env vars to Render service $SERVICE_ID ..."
  render env update "$SERVICE_ID" REDIS_SERVICE_URI "$uri"
  render env update "$SERVICE_ID" ASSETS "$(env_get ASSETS)"
  echo "Done. Update happened live; the service will pick it up on next deploy/restart."
}

deploy() {
  require_render
  if [[ -z "$SERVICE_ID" ]]; then
    echo "ERROR: set RENDER_SERVICE_ID first." >&2
    exit 1
  fi
  echo "Triggering deploy of Render service $SERVICE_ID ..."
  render deploy "$SERVICE_ID"
}

case "${1:-}" in
  validate) validate ;;
  env-sync) env_sync ;;
  deploy)   deploy ;;
  *)
    echo "usage: $0 {validate|env-sync|deploy}"
    exit 1
    ;;
esac