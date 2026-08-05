# Testing and Release Standard

## Test strategy

- Test behavior at the lowest useful level first: deterministic unit tests for logic, contract tests for interfaces, and integration tests for real service boundaries and failure paths.
- Critical paths include data loss, duplicate processing, ordering, idempotency, authentication, secret handling, model or external-service failure, and any order-placement behavior. Changes affecting them require targeted tests.
- Tests MUST assert outcomes and failure behavior, not merely process exit codes. Do not delete or weaken tests to accommodate a regression.

## Build and review gates

- Builds, dependency resolution, static analysis, type checks, and tests applicable to the changed modules MUST pass before release, unless an approved, time-bound exception is documented.
- Reviewers MUST inspect the actual deployment and configuration diff. Generated manifests, dependency updates, and infrastructure changes require particular scrutiny for unintended scope.
- A release candidate MUST identify the source revision, artifact or image references, configuration changes, migration dependencies, responsible owner, and rollback point.

## Rollout and rollback

- Define rollout sequence, monitoring signals, stop conditions, and rollback actions before deployment. The rollback must be executable without relying on undocumented local state.
- Post-deploy verification MUST check the behavior and data outcome affected by the release, not only workload readiness or a successful deployment command.
- A release is incomplete when validation evidence is absent, even if every command returned success. Record the evidence in the change record or relevant wiki/runbook.

## Change records

- Use an approved [design record](../../design/README.md) for changes that meet its threshold. Keep the record current through implementation, validation, rollback, and supersession.
- Emergency changes require retrospective documentation and review after service stabilization; emergency status is not a permanent exemption from these standards.
