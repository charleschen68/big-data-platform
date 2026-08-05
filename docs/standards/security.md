# Security Standard

## Secrets and sensitive data

- Store secrets outside Git and inject them through an approved secret-management path. Source files, manifests, examples, logs, test fixtures, designs, and wiki pages MUST NOT contain usable credentials.
- Treat connection strings, account identifiers, signing material, access tokens, and production endpoints as sensitive unless explicitly classified otherwise.
- Redact sensitive output before attaching it to issues, incidents, pull requests, or documentation. Rotate a secret immediately when exposure is suspected and record the response through the approved incident process.

## Access and platform controls

- Apply least-privilege RBAC to Kubernetes service accounts, CI identities, repositories, data stores, and deployment credentials. Review access after role, integration, or environment changes.
- External network exposure, privileged workloads, host mounts, and money-moving or order-placement paths require an approved threat-model review in a [design record](../../design/README.md).
- Production access MUST be attributable to an identity and limited to the minimum necessary scope and duration.

## Supply chain and dependencies

- Use trusted, version-pinned dependencies and base images. Review vulnerability findings, provenance, and update impact before promotion.
- Do not bypass signature, integrity, vulnerability, or policy checks merely to unblock a build. A documented, time-bound exception must define compensating controls.
- Build outputs and deployment artifacts MUST be traceable to a reviewed source revision and reproducible build inputs.
