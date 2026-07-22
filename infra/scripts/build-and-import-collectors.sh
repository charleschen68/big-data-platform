#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

VM_NAME="${VM_NAME:-k3s-node}"
PLATFORM="linux/arm64"

images=(rss market settlement retrain)
for name in "${images[@]}"; do
  case "${name}" in
    rss) image="big-data/rss-collector:phase2" ;;
    market) image="big-data/market-collector:phase2" ;;
    settlement) image="big-data/settlement-worker:phase2" ;;
    retrain) image="big-data/model-retrain:phase2" ;;
  esac
  docker build --platform "${PLATFORM}" -f "dataflow/docker/${name}.Dockerfile" -t "${image}" .
  docker save "${image}" | orb -m "${VM_NAME}" -u root k3s ctr images import -
done

orb -m "${VM_NAME}" -u root k3s ctr images list | grep 'big-data/'
