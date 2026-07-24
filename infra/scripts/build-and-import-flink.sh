#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

VM_NAME="${VM_NAME:-k3s-node}"
PLATFORM="linux/arm64"

JOBS=(
  eth-sentiment-trading-job
  eth-sentiment-analysis-job
  kafka2milvus
  employee-message-processor
  realtime-riskcontrol-embedding-job
)

for JOB in "${JOBS[@]}"; do
  image="big-data/${JOB}:phase4"
  echo "Building ${image}..."
  docker build --platform "${PLATFORM}" \
    -f "datastream/${JOB}/Dockerfile" \
    -t "${image}" .
  echo "Importing ${image} to ${VM_NAME}..."
  docker save "${image}" | orb -m "${VM_NAME}" -u root k3s ctr images import -
  echo "Verifying ${image} in k3s..."
  orb -m "${VM_NAME}" -u root k3s ctr images list | grep 'big-data/' | grep phase4
  echo ""
done

echo "All Flink images built and imported."
