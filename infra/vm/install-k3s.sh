#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"
K3S_VERSION="v1.36.2+k3s1"

orb -m "${VM_NAME}" -u root sh -c "curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=${K3S_VERSION} INSTALL_K3S_EXEC='server --tls-san ${VM_NAME}.orb.local' sh -"
# --tls-san 必须在装机时给：k3s 自动生成的 server 证书默认 SAN 不含 OrbStack 的 .orb.local 域名，
# 而 merge-kubeconfig.sh 会把 kubeconfig 的 server 地址改写成这个域名，装完不给会导致宿主机侧
# kubectl 报 x509 SAN 不匹配（已实测踩过一次坑）。

echo "等待节点 Ready..."
for i in $(seq 1 30); do
  if orb -m "${VM_NAME}" -u root k3s kubectl get nodes --no-headers 2>/dev/null | grep -q " Ready "; then
    echo "节点已 Ready"
    break
  fi
  sleep 2
done

orb -m "${VM_NAME}" -u root k3s kubectl get nodes
orb -m "${VM_NAME}" -u root k3s --version
