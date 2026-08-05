# Project Instructions

## Purpose and scope

This repository contains a data platform composed of Java Flink jobs, Python collectors, Kubernetes infrastructure, and stateful data services. These instructions apply to every change in this repository, including experiments, operational changes, documentation, automation, and changes made by humans or agents.

The standard is production readiness. A change is not low risk merely because it is local, experimental, or performed on a single-node cluster.

## Source of truth and evidence

- Treat version-controlled manifests, code, approved designs, and current command output as separate sources of truth. Resolve conflicts before acting.
- Record time-sensitive operational facts with their verification time, command or dashboard source, and environment.
- Do not describe a plan, successful build, or historical observation as a deployed or healthy production state.
- When evidence is unavailable, state the uncertainty and define the check required to resolve it.

## Architecture boundaries

- Keep Java Flink jobs in `datastream/`, Python collectors and their tests in `dataflow/`, and deployment definitions in `infra/`.
- Treat Kafka topics, schemas, consumer groups, idempotency keys, database schemas, Kubernetes service names, and external side effects as cross-service interfaces.
- Preserve module ownership and public contracts. Cross-boundary changes require an approved design record in [`design/`](design/README.md).
- Compose files are transition or local-development artifacts only; do not use them to bypass the declared Kubernetes and GitOps deployment path without an approved exception.

## Change control

- Create and approve a design record before changing infrastructure topology, stateful storage, service interfaces, Kafka contracts, security controls, reliability behavior, or a flow that can place orders or move money.
- Define the owner, rollout sequence, expected signals, failure threshold, rollback procedure, and validation evidence before the change starts.
- Review changes as scoped diffs. Do not combine unrelated refactors, generated-file updates, or formatting churn with a production change.
- Update the relevant design, wiki entry, runbook, or standard in the same change when operational behavior or a durable decision changes.

## Production safety rules

- Obtain explicit approval before actions that can interrupt, recreate, delete, or reconfigure shared services, Kafka, stateful workloads, persistent volumes, secrets, or live Flink deployments.
- Use bounded timeouts, explicit retry policies, idempotent writes, and dead-letter handling where a failure can be retried or replayed.
- Verify backup and restore paths with evidence before relying on them for recovery.
- Never weaken readiness, liveness, resource, security, or test controls merely to make a deployment appear healthy.
- Stop and escalate when validation fails or evidence contradicts the rollout assumptions.

## Documentation responsibilities

- Use [`design/`](design/README.md) for future decisions and trade-offs; use [`wiki/`](wiki/README.md) for verified operational knowledge.
- Follow the normative requirements in [`docs/standards/`](docs/standards/README.md).
- Mark stale information, name its owner, and set a re-verification date. Do not silently leave operational guidance to decay.
- Keep secrets, personal data, account identifiers, and production endpoints out of documentation.

## Prohibited practices

- Do not commit credentials, tokens, connection strings, private keys, customer data, or secret-bearing command output.
- Do not swallow exceptions, use unbounded retries, or convert failed validation into a success result.
- Do not mutate schemas, topics, consumer groups, persistent data, or externally visible behavior without a compatibility and rollback plan.
- Do not delete tests, disable type checks, suppress security warnings, or claim completion without current evidence.
- Do not add IDE metadata, dependency caches, build artifacts, runtime dumps, or generated files unless they are intentionally versioned and the reason is documented.
