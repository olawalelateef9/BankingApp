#!/usr/bin/env bash
# ============================================================
# Techbleat Global Bank — oauth2-proxy bootstrap (Helm)
# Usage:
#   COGNITO_CLIENT_ID=... COGNITO_CLIENT_SECRET=... \
#     ./scripts/deploy-oauth2-proxy.sh [dev|staging|prod]
#
# Optional: OAUTH2_PROXY_COOKIE_SECRET (reuse it to keep existing
# sessions valid; generated if unset), COGNITO_USER_POOL_ID.
#
# Prerequisites: kubectl (pointed at the target cluster), helm, python3
# CI does the same thing in .github/workflows/deploy.yml.
# ============================================================
set -euo pipefail

ENVIRONMENT="${1:-dev}"
NAMESPACE="banking-${ENVIRONMENT}"
USER_POOL_ID="${COGNITO_USER_POOL_ID:-eu-west-2_wtq53RFPW}"
OIDC_ISSUER="https://cognito-idp.eu-west-2.amazonaws.com/${USER_POOL_ID}"
COGNITO_DOMAIN="${COGNITO_DOMAIN:-eu-west-2wtq53rfpw.auth.eu-west-2.amazoncognito.com}"
CHART_VERSION="10.7.1"
VALUES_FILE="k8s/helm/oauth2-proxy-values.yaml"

case "${ENVIRONMENT}" in
  prod) REPLICAS=2 ;;
  dev|staging) REPLICAS=1 ;;
  *) echo "[ERROR] Environment must be dev, staging or prod" >&2; exit 1 ;;
esac

: "${COGNITO_CLIENT_ID:?Set COGNITO_CLIENT_ID to the Cognito app client ID}"
: "${COGNITO_CLIENT_SECRET:?Set COGNITO_CLIENT_SECRET to the Cognito app client secret}"

if [ -z "${OAUTH2_PROXY_COOKIE_SECRET:-}" ]; then
  echo "[STEP] Generating cookie secret..."
  OAUTH2_PROXY_COOKIE_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(16))")
  echo "[OK] Cookie secret generated — save it as the OAUTH2_PROXY_COOKIE_SECRET GitHub secret:"
  echo "     ${OAUTH2_PROXY_COOKIE_SECRET}"
fi

echo "[STEP] Ensuring namespace '${NAMESPACE}' exists..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "[STEP] Creating/updating secret 'oauth2-proxy-secret'..."
kubectl create secret generic oauth2-proxy-secret \
  --namespace "${NAMESPACE}" \
  --from-literal=client-id="${COGNITO_CLIENT_ID}" \
  --from-literal=client-secret="${COGNITO_CLIENT_SECRET}" \
  --from-literal=cookie-secret="${OAUTH2_PROXY_COOKIE_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[STEP] Creating/updating configmap 'oidc-config' for the backend services..."
kubectl create configmap oidc-config \
  --namespace "${NAMESPACE}" \
  --from-literal=OIDC_ISSUER="${OIDC_ISSUER}" \
  --from-literal=OIDC_AUDIENCE="${COGNITO_CLIENT_ID}" \
  --from-literal=OIDC_ADMIN_GROUP=bank-admins \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[STEP] Installing oauth2-proxy chart ${CHART_VERSION}..."
helm repo add oauth2-proxy https://oauth2-proxy.github.io/manifests >/dev/null
helm repo update oauth2-proxy >/dev/null
helm upgrade --install oauth2-proxy oauth2-proxy/oauth2-proxy \
  --version "${CHART_VERSION}" \
  --namespace "${NAMESPACE}" \
  -f "${VALUES_FILE}" \
  --set extraArgs.oidc-issuer-url="${OIDC_ISSUER}" \
  --set extraArgs.whitelist-domain="${COGNITO_DOMAIN}" \
  --set sessionStorage.redis.standalone.connectionUrl="redis://redis-service:6379" \
  --set replicaCount="${REPLICAS}" \
  --wait --timeout 3m

echo "[OK] oauth2-proxy deployed to ${NAMESPACE}"
echo "     Restart the backend services so they pick up oidc-config:"
echo "     kubectl rollout restart deployment -n ${NAMESPACE}"
