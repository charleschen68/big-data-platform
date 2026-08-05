# Production Standards

These documents define mandatory engineering and operating practices for this repository. They are read with [`Instructions.md`](../../Instructions.md); neither document replaces a change-specific design record.

## Precedence and exceptions

Requirements apply in this order:

1. Applicable law, contractual commitments, and platform/provider requirements.
2. [`Instructions.md`](../../Instructions.md).
3. This standards directory.
4. An approved, change-specific record in [`design/`](../../design/README.md).
5. Local module conventions that do not conflict with a higher rule.

A lower-priority document cannot silently weaken a higher-priority requirement. An exception is valid only when its approved design record names the owner, expiry date, risk, compensating controls, and review approval. Expired exceptions are invalid until renewed.

## Standards index

- [Engineering](engineering.md): code, configuration, images, and Kubernetes manifests.
- [Data contracts](data-contracts.md): Kafka and cross-service data interfaces.
- [Reliability and operations](reliability-operations.md): observability, recovery, and incident learning.
- [Security](security.md): secrets, access, supply chain, and threat modeling.
- [Testing and release](testing-release.md): validation, rollout, rollback, and release evidence.
