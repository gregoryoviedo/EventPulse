#!/usr/bin/env bash
#
# Idempotent bootstrap for the eventpulse Kubernetes cluster.
#
# Two modes:
#   k3d  (default, dev on macOS): creates a k3d cluster inside Docker/OrbStack
#        and imports the locally-built images into it.
#   k3s  (Linux homelab / production): installs k3s as a system service when
#        missing and waits for the node to be ready. Images are pulled from
#        the container registry, so nothing is imported locally.
#
# Invoked by the null_resource in main.tf; safe to run repeatedly.
set -euo pipefail

CLUSTER="${K3D_CLUSTER:-eventpulse}"
MODE="${BOOTSTRAP_MODE:-k3d}"
REGISTRY="${IMAGE_REGISTRY:-ghcr.io/eventpulse}"
TAG="${IMAGE_TAG:-0.1.0}"
PORTS="${INGRESS_PORTS:-18080,18090}"

case "${MODE}" in
  k3d|k3s) ;;
  *) echo "BOOTSTRAP_MODE must be 'k3d' or 'k3s', got '${MODE}'" >&2; exit 1 ;;
esac

# --- k3d: local dev cluster on macOS ---------------------------------------
if [ "${MODE}" = "k3d" ]; then
  command -v k3d >/dev/null 2>&1 || { echo "k3d is required: brew install k3d" >&2; exit 1; }
  CONTEXT="k3d-${CLUSTER}"

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

  # Tag and import the four images built by docker-compose.
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
fi

# --- k3s: Linux homelab / production ----------------------------------------
if [ "${MODE}" = "k3s" ]; then
  if ! command -v k3s >/dev/null 2>&1; then
    echo "Installing k3s..."
    curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
  fi

  # k3s bundles kubectl; expose it under the standard name when missing.
  if ! command -v kubectl >/dev/null 2>&1; then
    ln -sf /usr/local/bin/k3s /usr/local/bin/kubectl
  fi

  export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
  echo "Waiting for k3s node to be ready..."
  until kubectl get nodes >/dev/null 2>&1 && [ "$(kubectl get nodes --no-headers 2>/dev/null | wc -l)" -gt 0 ]; do
    echo "waiting for k3s API..."
    sleep 3
  done
  kubectl wait --for=condition=Ready node --all --timeout=120s
fi

echo "Bootstrap complete (mode: ${MODE})."