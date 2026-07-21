#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"

if orb list 2>/dev/null | grep -q "^${VM_NAME}\b"; then
  echo "VM ${VM_NAME} 已存在，跳过创建"
else
  orb create --memory 28G --cpus 8 ubuntu:24.04 "${VM_NAME}"
fi

orb -m "${VM_NAME}" -u root true
echo "VM ${VM_NAME} 就绪"
