#!/usr/bin/env bash
set -euo pipefail

VM_NAME="k3s-node"
CONTEXT_NAME="k3s-node"
TMP_KUBECONFIG="$(mktemp)"

cp "${HOME}/.kube/config" "${HOME}/.kube/config.bak-$(date +%Y%m%d%H%M%S)"

orb -m "${VM_NAME}" -u root cat /etc/rancher/k3s/k3s.yaml > "${TMP_KUBECONFIG}"

sed -i '' \
  -e "s/name: default/name: ${CONTEXT_NAME}/g" \
  -e "s/cluster: default/cluster: ${CONTEXT_NAME}/g" \
  -e "s/user: default/user: ${CONTEXT_NAME}/g" \
  -e "s/current-context: default/current-context: ${CONTEXT_NAME}/g" \
  -e "s#server: https://127.0.0.1:6443#server: https://${VM_NAME}.orb.local:6443#" \
  "${TMP_KUBECONFIG}"

KUBECONFIG="${HOME}/.kube/config:${TMP_KUBECONFIG}" kubectl config view --flatten > "${TMP_KUBECONFIG}.merged"
mv "${TMP_KUBECONFIG}.merged" "${HOME}/.kube/config"
rm -f "${TMP_KUBECONFIG}"

kubectl config use-context "${CONTEXT_NAME}"
echo "已切换到 context: ${CONTEXT_NAME}"
kubectl get nodes
