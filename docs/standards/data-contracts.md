# Data Contract Standard

## Scope

Kafka topics, schemas, consumer groups, database records exchanged between services, vector-store payloads, and every externally visible side effect are production interfaces. Their owners must manage them as contracts, not implementation details.

## Ownership and evolution

- Every topic and schema MUST have an accountable producer owner and documented consumer set.
- A contract change MUST declare schema compatibility, required defaulting behavior, producer/consumer rollout order, data backfill or replay impact, monitoring signals, and rollback behavior in an approved [design record](../../design/README.md).
- Producers MUST preserve backward compatibility until all supported consumers have migrated, unless a coordinated migration window and recovery plan are approved.
- Consumer groups are stable operational identities. Renaming, resetting, or deleting one requires a documented offset, replay, duplication, and data-loss analysis.

## Delivery, failures, and replay

- A consumer that writes to a database, invokes a model, or emits an external action MUST define its idempotency key and duplicate-handling behavior.
- Invalid or non-retriable records MUST have a traceable failure path, normally a dead-letter topic or equivalent quarantined store, with reason, source reference, and operator action.
- Retention, compaction, and partition changes MUST be assessed for replay duration, ordering, consumer lag, storage pressure, and recovery objectives.
- Replays MUST be isolated from live side effects or protected by explicit idempotency and approval. A successful replay command is not proof of correct business outcomes.

## Observability and evidence

- Contract owners MUST expose enough metrics and logs to identify publish failures, consumer lag, deserialization failures, DLQ volume, duplicate suppression, and external-write outcomes.
- A contract release is incomplete without evidence that producers and consumers behave as designed under its rollout path.
