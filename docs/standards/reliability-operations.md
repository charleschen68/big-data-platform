# Reliability and Operations Standard

## Ownership and service readiness

- Every production service MUST have an owner, dependencies, expected inputs and outputs, failure modes, and an escalation path recorded in the wiki before it is considered operationally supported.
- Define measurable service-level indicators before adopting numeric SLOs. Numeric targets require an owner, measurement window, data source, and review cadence.
- Dashboards, alerts, and runbooks MUST be linked from the service entry when they exist. An alert without an actionable operator response is a defect, not background noise.

## Observability

- Emit structured logs, metrics, and correlation identifiers sufficient to trace an input through Kafka, Flink, collectors, and persisted outputs where technically feasible.
- Monitor availability, latency, throughput, error rate, backlog or lag, resource saturation, and data-quality signals appropriate to each service.
- Alert thresholds MUST reflect a user, business, data-integrity, or recovery risk. Suppressing a noisy alert without correcting its signal is prohibited.

## Recovery and incident response

- Backup, restore, checkpoint, and savepoint claims require periodic restoration evidence. A backup that has not been restored is an unverified recovery hypothesis.
- Runbooks MUST state prerequisites, safe scope, expected output, rollback action, and evidence capture. Store them under [`wiki/runbooks/`](../../wiki/README.md) when first created.
- Preserve incident timelines, command output references, impact, hypotheses, and corrective-action evidence under [`wiki/incidents/`](../../wiki/README.md).
- Schedule resilience exercises for the failure modes whose recovery depends on human action, state restoration, or cross-service ordering.

## Capacity and change operations

- Capacity settings MUST identify the workload assumption and observed evidence used to choose them. Reassess after material traffic, model, schema, or topology changes.
- A disruptive change requires an approved rollout window, monitoring owner, stop condition, and rollback decision authority.
