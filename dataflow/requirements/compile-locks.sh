#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
REQUIREMENTS_DIR="${REPO_ROOT}/dataflow/requirements"
PYTHON_IMAGE="python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1"

case "${1:-}" in
  "")
    MODE="default"
    ;;
  --refresh)
    MODE="refresh"
    ;;
  *)
    echo "usage: $0 [--refresh]" >&2
    exit 64
    ;;
esac

docker run --rm --platform linux/arm64 \
  --env HOME=/tmp \
  --env PIP_TOOLS_CACHE_DIR=/tmp/pip-tools-cache \
  --mount "type=bind,src=${REQUIREMENTS_DIR},dst=/work" \
  --workdir /work \
  --user "$(id -u):$(id -g)" \
  "${PYTHON_IMAGE}" \
  sh -ec '
    python -m venv /tmp/pip-tools
    /tmp/pip-tools/bin/pip install --no-cache-dir --require-hashes --requirement bootstrap.txt
    for name in rss market settlement retrain; do
      if [ "$1" = "refresh" ]; then
        /tmp/pip-tools/bin/pip-compile --upgrade --rebuild --allow-unsafe --generate-hashes --strip-extras \
          --output-file "${name}.txt" "${name}.in"
      else
        /tmp/pip-tools/bin/pip-compile --dry-run --allow-unsafe --generate-hashes --strip-extras \
          --constraint "${name}.txt" \
          --output-file "${name}.txt" "${name}.in"
      fi
    done
  ' sh "${MODE}"
