#!/usr/bin/env bash
# ============================================================
# Techbleat Global Bank — oauth2-proxy bootstrap
# Usage: ./scripts/deploy-oauth2-proxy.sh
# Prerequisites: kubectl (pointed at the target cluster), python3
# ============================================================
set -euo pipefail

NAMESPACE="banking"
SECRET_NAME="oauth2-proxy-secret"
MANIFEST="k8s/base/oauth2-proxy.yaml"

echo "[STEP] Generating cookie secret..."
COOKIE_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(16))")
echo "[OK] Cookie secret generated"

echo "[STEP] Ensuring namespace '${NAMESPACE}' exists..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
echo "[OK] Namespace ready"

echo "[STEP] Creating/updating secret '${SECRET_NAME}' with placeholder Cognito credentials..."
kubectl create secret generic "${SECRET_NAME}" \
  --namespace "${NAMESPACE}" \
  --from-literal=client-id="REPLACE_WITH_COGNITO_APP_CLIENT_ID" \
  --from-literal=client-secret="REPLACE_WITH_COGNITO_APP_CLIENT_SECRET" \
  --from-literal=cookie-secret="${COOKIE_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "[OK] Secret '${SECRET_NAME}' created with placeholders"

echo "[STEP] Applying oauth2-proxy manifest..."
kubectl apply -f "${MANIFEST}"
echo "[OK] oauth2-proxy applied"

cat <<EOF

============================================================
 oauth2-proxy is deployed, but it will NOT work yet.
============================================================

 It is currently running with PLACEHOLDER Cognito credentials
 and a real (but generated-just-now) cookie secret:

   client-id     = REPLACE_WITH_COGNITO_APP_CLIENT_ID
   client-secret = REPLACE_WITH_COGNITO_APP_CLIENT_SECRET
   cookie-secret = ${COOKIE_SECRET}   (already applied, safe to keep)

 Once you have created your Cognito User Pool and App Client:

 1. Replace the client-id / client-secret in the secret
    (keep the same cookie-secret so existing sessions/cookies
    stay consistent):

    kubectl create secret generic ${SECRET_NAME} \\
      --namespace ${NAMESPACE} \\
      --from-literal=client-id=<COGNITO_APP_CLIENT_ID> \\
      --from-literal=client-secret=<COGNITO_APP_CLIENT_SECRET> \\
      --from-literal=cookie-secret=${COOKIE_SECRET} \\
      --dry-run=client -o yaml | kubectl apply -f -

 2. Replace COGNITO_USER_POOL_ID_PLACEHOLDER in
    ${MANIFEST} with your real User Pool ID, so the
    --oidc-issuer-url arg reads:

      https://cognito-idp.eu-west-2.amazonaws.com/<COGNITO_USER_POOL_ID>

    Then re-apply:

      kubectl apply -f ${MANIFEST}

 3. Restart the rollout so the new args/secret take effect:

      kubectl rollout restart deployment/oauth2-proxy -n ${NAMESPACE}
      kubectl rollout status deployment/oauth2-proxy -n ${NAMESPACE}
============================================================
EOF
