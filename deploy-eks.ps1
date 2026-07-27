# ============================================================
# Techbleat Global Bank — AWS EKS Deployment Script
# Usage: .\deploy-eks.ps1
# Prerequisites: aws cli, eksctl, helm, kubectl installed
# ============================================================

param(
    [string]$Action = "deploy",  # deploy or destroy
    [string]$ClusterName = "techbleat-banking",
    [string]$Region = "eu-west-2",
    [string]$Domain = "techbleatglobalbank.duckdns.org",
    [string]$Email = "olawalelateef9@gmail.com"
)

$ErrorActionPreference = "Stop"
$AppDir = "C:\Users\olawa\BankingApp"

# ============================================================
# COLOURS
# ============================================================
function Write-Step  { param($msg) Write-Host "`n[STEP] $msg" -ForegroundColor Cyan }
function Write-OK    { param($msg) Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Warn  { param($msg) Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Fail  { param($msg) Write-Host "[FAIL] $msg" -ForegroundColor Red }

# ============================================================
# DESTROY MODE
# ============================================================
if ($Action -eq "destroy") {
    Write-Step "Deleting EKS cluster $ClusterName..."
    eksctl delete cluster --name $ClusterName --region $Region
    Write-OK "Cluster deleted. No more AWS charges."
    exit 0
}

# ============================================================
# DEPLOY MODE
# ============================================================

# ------------------------------------------------------------
# 1. Create EKS cluster
# ------------------------------------------------------------
Write-Step "Creating EKS cluster $ClusterName in $Region..."
eksctl create cluster -f "$AppDir\eks-cluster.yaml"
Write-OK "EKS cluster created"

# ------------------------------------------------------------
# 2. Update kubeconfig
# ------------------------------------------------------------
Write-Step "Updating kubeconfig..."
aws eks update-kubeconfig --name $ClusterName --region $Region
Write-OK "kubeconfig updated"

# ------------------------------------------------------------
# 3. Get node names and label them
# ------------------------------------------------------------
Write-Step "Labelling nodes..."
Start-Sleep -Seconds 10

$nodeOutput = kubectl get nodes -o jsonpath='{.items[*].metadata.name}'
$nodes = $nodeOutput -split ' '
$appNodes   = $nodes | Where-Object { $_ -match "192-168-(0|52)-" }
$ingressNode = $nodes | Where-Object { $_ -match "192-168-29-" }

# Fallback — label by node group
if (-not $appNodes) {
    Write-Warn "Using nodegroup labels instead"
    $allNodes = kubectl get nodes -o custom-columns="NAME:.metadata.name,LABEL:.metadata.labels.eks\.amazonaws\.com/nodegroup" --no-headers
    Write-Host $allNodes
} else {
    foreach ($node in $appNodes) {
        kubectl label node $node node-role=application --overwrite
        Write-OK "Labelled $node as application"
    }
    if ($ingressNode) {
        kubectl label node $ingressNode node-role=ingress --overwrite
        Write-OK "Labelled $ingressNode as ingress"
    }
}

# ------------------------------------------------------------
# 4. Install NGINX Ingress
# ------------------------------------------------------------
Write-Step "Installing NGINX Ingress Controller..."
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx 2>$null
helm repo update

helm install ingress-nginx ingress-nginx/ingress-nginx `
    --namespace ingress-nginx `
    --create-namespace `
    --set controller.nodeSelector."node-role"=ingress `
    --wait --timeout 5m

Write-OK "NGINX Ingress installed"

# ------------------------------------------------------------
# 5. Get Load Balancer IP and show it
# ------------------------------------------------------------
Write-Step "Getting Load Balancer details..."
Start-Sleep -Seconds 15

$lbHostname = kubectl get svc ingress-nginx-controller -n ingress-nginx `
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'

Write-OK "Load Balancer: $lbHostname"

$lbIp = (Resolve-DnsName $lbHostname -ErrorAction SilentlyContinue | `
    Where-Object { $_.Type -eq "A" } | Select-Object -First 1).IPAddress

if ($lbIp) {
    Write-OK "Load Balancer IP: $lbIp"
    Write-Warn "ACTION REQUIRED: Update DuckDNS techbleatglobalbank to IP: $lbIp"
    Write-Host "Press ENTER after updating DuckDNS..." -ForegroundColor Yellow
    Read-Host
} else {
    Write-Warn "Could not resolve LB IP automatically. Update DuckDNS manually with: $lbHostname"
    Write-Host "Press ENTER after updating DuckDNS..." -ForegroundColor Yellow
    Read-Host
}

# ------------------------------------------------------------
# 6. Install EBS CSI Driver
# ------------------------------------------------------------
Write-Step "Installing EBS CSI Driver..."
eksctl create addon --name aws-ebs-csi-driver `
    --cluster $ClusterName --region $Region --force
Start-Sleep -Seconds 30
Write-OK "EBS CSI Driver installed"

# ------------------------------------------------------------
# 7. Install cert-manager
# ------------------------------------------------------------
Write-Step "Installing cert-manager..."
helm repo add jetstack https://charts.jetstack.io 2>$null
helm repo update

helm install cert-manager jetstack/cert-manager `
    --namespace cert-manager `
    --create-namespace `
    --set crds.enabled=true `
    --set nodeSelector."node-role"=ingress `
    --set cainjector.nodeSelector."node-role"=ingress `
    --set webhook.nodeSelector."node-role"=ingress `
    --timeout 10m `
    --wait

Write-OK "cert-manager installed"

# ------------------------------------------------------------
# 8. Create ClusterIssuer
# ------------------------------------------------------------
Write-Step "Creating ClusterIssuer..."
@"
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: $Email
    privateKeySecretRef:
      name: letsencrypt-prod-key
    solvers:
      - http01:
          ingress:
            class: nginx
"@ | kubectl apply -f -

Start-Sleep -Seconds 10
$issuerReady = kubectl get clusterissuer letsencrypt-prod -o jsonpath='{.status.conditions[0].status}'
if ($issuerReady -eq "True") {
    Write-OK "ClusterIssuer is ready"
} else {
    Write-Warn "ClusterIssuer not ready yet - continuing anyway"
}

# ------------------------------------------------------------
# 9. Deploy Banking App
# ------------------------------------------------------------
Write-Step "Deploying banking application..."

kubectl create namespace banking 2>$null

$manifests = @(
    "namespace.yaml",
    "secret.yaml",
    "db-init-configmap.yaml",
    "postgres-statefulset.yaml",
    "postgres-service.yaml",
    "redis.yaml",
    "kafka.yaml",
    "user-service-deployment.yaml",
    "user-service-svc.yaml",
    "transaction-service.yaml",
    "activity-service-deployment.yaml",
    "frontend-deployment.yaml",
    "gateway-services.yaml"
)

foreach ($manifest in $manifests) {
    $path = "$AppDir\k8s\$manifest"
    if (Test-Path $path) {
        kubectl apply -f $path
        Write-OK "Applied $manifest"
    } else {
        Write-Warn "Skipping $manifest - file not found"
    }
}

# ------------------------------------------------------------
# 10. Set CORS environment variables
# ------------------------------------------------------------
Write-Step "Setting CORS environment variables..."
kubectl set env deployment/user-service `
    FRONTEND_ORIGIN=https://$Domain -n banking
kubectl set env deployment/activity-service `
    FRONTEND_ORIGIN=https://$Domain -n banking
Write-OK "CORS configured"

# ------------------------------------------------------------
# 11. Apply Ingress
# ------------------------------------------------------------
Write-Step "Applying Ingress rules..."
@"
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: banking-ingress-api
  namespace: banking
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/rewrite-target: /`$2
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - $Domain
      secretName: techbleatglobalbank-tls
  rules:
    - host: $Domain
      http:
        paths:
          - path: /api/users(/|`$)(.*)
            pathType: ImplementationSpecific
            backend:
              service:
                name: user-service
                port:
                  number: 8000
          - path: /api/transactions(/|`$)(.*)
            pathType: ImplementationSpecific
            backend:
              service:
                name: transaction-service
                port:
                  number: 8080
          - path: /api/activities(/|`$)(.*)
            pathType: ImplementationSpecific
            backend:
              service:
                name: activity-service-external
                port:
                  number: 8001
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: banking-ingress-frontend
  namespace: banking
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - $Domain
      secretName: techbleatglobalbank-tls
  rules:
    - host: $Domain
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: frontend-service
                port:
                  number: 3000
"@ | kubectl apply -f -

Write-OK "Ingress rules applied"

# ------------------------------------------------------------
# 12. Wait for certificate
# ------------------------------------------------------------
Write-Step "Waiting for SSL certificate..."
$maxAttempts = 20
$attempt = 0
do {
    Start-Sleep -Seconds 15
    $attempt++
    $certReady = kubectl get certificate techbleatglobalbank-tls -n banking `
        -o jsonpath='{.status.conditions[0].status}' 2>$null
    Write-Host "  Certificate status: $certReady (attempt $attempt/$maxAttempts)"
} while ($certReady -ne "True" -and $attempt -lt $maxAttempts)

if ($certReady -eq "True") {
    Write-OK "SSL Certificate issued successfully"
} else {
    Write-Warn "Certificate not ready yet - check: kubectl get certificate -n banking"
}

# ------------------------------------------------------------
# 13. Wait for pods
# ------------------------------------------------------------
Write-Step "Waiting for banking pods to be ready..."
Start-Sleep -Seconds 30
kubectl get pods -n banking -o wide

# ------------------------------------------------------------
# 14. Final verification
# ------------------------------------------------------------
Write-Step "Final verification..."
Start-Sleep -Seconds 10

try {
    $response = Invoke-WebRequest -Uri "https://$Domain" -Method Head -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-OK "Site is live at https://$Domain"
    }
} catch {
    Write-Warn "Site not responding yet - may need a few more minutes"
}

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host " DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " URL:      https://$Domain" -ForegroundColor White
Write-Host " Cluster:  $ClusterName ($Region)" -ForegroundColor White
Write-Host " Nodes:    kubectl get nodes" -ForegroundColor White
Write-Host " Pods:     kubectl get pods -n banking" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " COST WARNING: ~£0.24/hour while running" -ForegroundColor Yellow
Write-Host " To destroy:  .\deploy-eks.ps1 -Action destroy" -ForegroundColor Yellow
Write-Host "============================================`n" -ForegroundColor Cyan
