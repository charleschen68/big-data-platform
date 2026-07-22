#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

docker run --rm --platform linux/arm64 \
  --env HOME=/tmp \
  --env PIP_TOOLS_CACHE_DIR=/tmp/pip-tools-cache \
  --volume "${REPO_ROOT}:/work" \
  --workdir /work \
  --user "$(id -u):$(id -g)" \
  python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1 \
  sh -ec '
    python -m venv /tmp/pip-tools
    /tmp/pip-tools/bin/pip install --no-cache-dir pip-tools==7.5.3
    for name in rss market settlement retrain; do
      /tmp/pip-tools/bin/pip-compile --upgrade --rebuild --allow-unsafe --generate-hashes --strip-extras \
        --output-file "dataflow/requirements/${name}.txt" "dataflow/requirements/${name}.in"
    done
  '
