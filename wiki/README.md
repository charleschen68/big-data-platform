# Project Wiki

This directory holds verified, reusable project knowledge: how the platform operates, how to recover it, what a service owns, and what prior incidents taught us. It does not hold proposals or unverified assumptions; use [`design/`](../design/README.md) for those.

Read the repository-wide contract in [`Instructions.md`](../Instructions.md) before adding or changing a wiki entry.

## Intended organization

Create these subdirectories when their first verified entry is added:

```text
wiki/
├── runbooks/    # operator procedures, prerequisites, rollback, validation
├── services/    # ownership, dependencies, interfaces, SLO/SLI references
├── incidents/   # factual timelines, impact, causes, follow-up evidence
└── glossary/    # stable platform terminology and canonical definitions
```

Do not create empty documents merely to fill this structure.

## Entry requirements

Each entry starts with this metadata block:

```markdown
**Owner:** team or accountable individual
**Last verified:** YYYY-MM-DD
**Evidence:** command output, dashboard, incident record, or approved design link
**Review cadence:** interval or event that requires re-verification
**Applies to:** environment, service, component, or version boundary
```

State only what the cited evidence supports. Mark content `stale` when the review cadence has elapsed, the referenced environment changed, or a known assumption is no longer verified. Stale content may guide investigation but must not authorize a production action.

## Runbook requirements

A runbook must name prerequisites, access requirements, safety boundaries, step-by-step actions, expected signals, failure criteria, rollback actions, and evidence to capture. Never embed credentials, unredacted customer data, or a command whose scope is broader than the documented target.

## Incident requirements

An incident entry must distinguish observed facts from hypotheses, preserve timestamps and evidence locations, identify impact, and track corrective actions to verifiable completion. Do not rewrite history to make a decision appear more certain than it was.
