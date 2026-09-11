#!/usr/bin/env bash
#
# Idempotent bootstrap for the local eventpulse k3d cluster.
# - Creates the k3d cluster if it does not exist.
# - Tags and imports the locally-built container images into the cluster.
#
# Invoked by the null_resource in main.tf; safe to run repeatedly.
set -euo pipefail

CLUSTER="${K3D_CLUSTER:-eventpulse}"
REGISTRY="${IMAGE_REGISTRY:-ghcr.io/eventpulse}"
TAG="${IMAGE_TAG:-0.1.0}"
PORTS="${INGRESS_PORTS:-18080,18090}"

CONTEXT="k3d-${CLUSTER}"

command -v k3d >/dev/null 2>&1 || { echo "k3d is required: brew install k3d" >&2; exit 1; }

# 1. Create the cluster when missing.
if ! kubectl config get-contexts "${CONTEXT}" >/dev/null 2>&1; then
  echo "Creating k3d cluster '${CLUSTER}'..."
  args=(cluster create "${CLUSTER}" --agents 1 --wait)
  IFS=',' read -r -a ports <<<"${PORTS}"
  for p in "${ports[@]}"; do
    args+=("--port" "${p}:80@loadbalancer")
  done
  k3d "${args[@]}"
  kubectl config use-context "${CONTEXT}" >/dev/null
fi

# 2. Tag and import the four images built by docker-compose.
for svc in ingestion-gateway document-processor metrics-aggregator mcp-server; do
  target="${REGISTRY}/${svc}:${TAG}"
  if ! docker image inspect "${target}" >/dev/null 2>&1; then
    source_img="eventpulse-${svc}:latest"
    if ! docker image inspect "${source_img}" >/dev/null 2>&1; then
      echo "Building image '${source_img}'..."
      docker compose build "${svc}"
    fi
    docker tag "${source_img}" "${target}"
  fi
  if ! docker exec "k3d-${CLUSTER}-server-0" ctr -n k8s.io images ls 2>/dev/null | grep -q "${target}"; then
    echo "Importing '${target}' into '${CLUSTER}'..."
    k3d image import "${target}" -c "${CLUSTER}"
  fi
done

echo "Bootstrap complete."