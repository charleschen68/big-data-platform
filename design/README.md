# Design Records

This directory contains forward-looking design records for changes that alter production behavior, system boundaries, or durable technical decisions. It is not a runbook, an incident log, or a changelog.

Read the repository-wide contract in [`Instructions.md`](../Instructions.md) before creating a record.

## When a design record is required

Create a record before changing any of the following:

- service topology, stateful storage, Kubernetes deployment model, or recovery path;
- Kafka topics, schemas, consumer groups, delivery guarantees, or replay behavior;
- public API, data contract, idempotency mechanism, or external side effect;
- authentication, authorization, secrets management, network exposure, or supply-chain control;
- reliability objective, alerting policy, capacity assumption, or failure-handling behavior.

Small, isolated changes may use a pull-request description when they do not change an interface or operational risk. The author must be able to justify that classification during review.

## File naming and lifecycle

Name each record `YYYY-MM-DD-short-topic.md`, using lowercase kebab case. Set one status near the top:

| Status | Meaning |
| --- | --- |
| `draft` | Under discussion; not authorized for implementation. |
| `approved` | Decision accepted; implementation may begin. |
| `implemented` | Implementation and defined validation evidence are complete. |
| `superseded` | Replaced; link to the successor record. |
| `rejected` | Considered and declined; retain the rationale. |

Never mark a record `implemented` from code review or a successful command alone. Link the verification evidence.

## Required structure

Every record must include these headings:

```markdown
# Decision title

**Status:** draft
**Owner:** team or accountable individual
**Decision date:** YYYY-MM-DD

## Context
## Goals and non-goals
## Options considered
## Decision
## Interfaces and data impact
## Failure and rollback behavior
## Observability
## Validation plan
## Open decisions
```

Describe alternatives honestly, including the reason they were rejected. Define compatibility, ordering, failure modes, rollback triggers, and validation signals concretely enough for an independent reviewer to test.

## Review and maintenance

The record owner updates status when the decision changes. A superseded record must link to its replacement. Any approved exception to a project standard belongs in the relevant record and must name its owner, expiry date, risk, and compensating controls.
