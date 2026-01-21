#!/bin/bash
# Setup GHCR (GitHub Container Registry) secret for pulling private images
#
# Prerequisites:
# - kubectl configured to access the K3s cluster
# - GitHub Personal Access Token (PAT) with read:packages scope
#
# Usage: ./setup-ghcr-secret.sh

set -e

echo "=== GHCR Secret Setup ==="
echo ""
echo "This script creates a Kubernetes secret to pull images from ghcr.io/elskow/saga-pattern"
echo ""

# Check if kubectl is configured
if ! kubectl cluster-info &>/dev/null; then
    echo "Error: kubectl is not configured or cluster is not reachable"
    exit 1
fi

# Get credentials
read -p "GitHub Username: " GITHUB_USER
if [ -z "$GITHUB_USER" ]; then
    echo "Error: GitHub username is required"
    exit 1
fi

read -sp "GitHub PAT (read:packages scope): " GITHUB_PAT
echo ""
if [ -z "$GITHUB_PAT" ]; then
    echo "Error: GitHub PAT is required"
    exit 1
fi

# Create namespace if it doesn't exist
kubectl create namespace saga-test --dry-run=client -o yaml | kubectl apply -f -

# Create/update the secret
echo ""
echo "Creating GHCR secret in saga-test namespace..."

kubectl create secret docker-registry ghcr-secret \
    --docker-server=ghcr.io \
    --docker-username="$GITHUB_USER" \
    --docker-password="$GITHUB_PAT" \
    --docker-email="${GITHUB_USER}@users.noreply.github.com" \
    --namespace=saga-test \
    --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "=== Setup Complete ==="
echo ""
echo "GHCR secret 'ghcr-secret' created in namespace 'saga-test'"
echo ""
echo "You can now deploy the services:"
echo "  kubectl apply -k overlays/choreography    # For choreography pattern"
echo "  kubectl apply -k overlays/orchestration   # For orchestration pattern"
