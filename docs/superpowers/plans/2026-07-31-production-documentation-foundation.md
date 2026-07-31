# Production Documentation Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** Establish version-controlled, production-grade collaboration instructions, design records, wiki conventions, and engineering standards for this data platform.

**Architecture:** Add three stable documentation entry points at the repository root: \`Instructions.md\` for durable constraints, \`design/\` for prospective change records, and \`wiki/\` for verified operational knowledge. Add \`docs/standards/\` as the governed set of executable norms. All links are repository-relative and the documents do not assert unverified environment state.

**Tech Stack:** CommonMark Markdown, repository-relative links, existing Git review workflow.

## Global Constraints

- Do not modify application code, Kubernetes manifests, existing design documents, or user-owned untracked files.
- State uncertain or time-sensitive operational claims as verification requirements, never as current fact.
- Never place credentials, tokens, connection strings, customer data, or production identifiers in repository documentation.
- Production changes require explicit verification signals, rollback criteria, and an evidence record.
- All internal links must resolve within this repository; all documents are UTF-8 Markdown.

---

### Task 1: Add the repository collaboration contract

**Files:**
- Create: \`Instructions.md\`
- Test: \`Instructions.md\`

**Interfaces:**
- Consumes: repository layout in \`README.md\` and the global constraints above.
- Produces: durable rules referenced by \`design/README.md\`, \`wiki/README.md\`, and \`docs/standards/README.md\`.

- [ ] **Step 1: Create \`Instructions.md\` with an operational contract**

Include these sections in this exact order: \`Purpose and scope\`, \`Source of truth and evidence\`, \`Architecture boundaries\`, \`Change control\`, \`Production safety rules\`, \`Documentation responsibilities\`, and \`Prohibited practices\`. Require a design record before cross-service, schema, infrastructure, or reliability changes; require explicit approval before disruptive shared-service actions; require pre-change signals, post-change evidence, and rollback conditions.

- [ ] **Step 2: Verify contract coverage**

Run:

\`\`\`bash
rg -n '^## (Purpose and scope|Source of truth and evidence|Architecture boundaries|Change control|Production safety rules|Documentation responsibilities|Prohibited practices)$' Instructions.md
\`\`\`

Expected: seven matching section headings.

- [ ] **Step 3: Commit the independently reviewable change**

\`\`\`bash
git add Instructions.md
git commit -m "docs: add production collaboration instructions"
\`\`\`

### Task 2: Add design and wiki entry points

**Files:**
- Create: \`design/README.md\`
- Create: \`wiki/README.md\`
- Test: \`design/README.md\`, \`wiki/README.md\`

**Interfaces:**
- Consumes: \`Instructions.md\` rules for evidence and change control.
- Produces: creation templates and lifecycle requirements for future design and knowledge documents.

- [ ] **Step 1: Create \`design/README.md\`**

Define design records as forward-looking documents. Require file names in the form \`YYYY-MM-DD-short-topic.md\`; require \`Status\`, \`Owner\`, \`Decision date\`, \`Context\`, \`Goals and non-goals\`, \`Options considered\`, \`Decision\`, \`Interfaces and data impact\`, \`Failure and rollback behavior\`, \`Observability\`, \`Validation plan\`, and \`Open decisions\` sections. Define statuses \`draft\`, \`approved\`, \`implemented\`, \`superseded\`, and \`rejected\`; require \`superseded\` records to link to their successor.

- [ ] **Step 2: Create \`wiki/README.md\`**

Define wiki entries as verified reference material rather than plans. Require each entry to show \`Owner\`, \`Last verified\`, \`Evidence\`, \`Review cadence\`, and \`Applies to\`. Define \`runbooks/\`, \`services/\`, \`incidents/\`, and \`glossary/\` as future subdirectories, and require stale or unverified content to be explicitly marked.

- [ ] **Step 3: Verify both entry points link to the collaboration contract**

Run:

\`\`\`bash
rg -n -F '](../Instructions.md)' design/README.md wiki/README.md
\`\`\`

Expected: one link in each file.

- [ ] **Step 4: Commit the independently reviewable change**

\`\`\`bash
git add design/README.md wiki/README.md
git commit -m "docs: add design and wiki conventions"
\`\`\`

### Task 3: Add the standards index and engineering/data-contract standards

**Files:**
- Create: \`docs/standards/README.md\`
- Create: \`docs/standards/engineering.md\`
- Create: \`docs/standards/data-contracts.md\`
- Test: all three files

**Interfaces:**
- Consumes: \`Instructions.md\` change control and \`design/README.md\` design-record schema.
- Produces: implementation and data-interface constraints for Java, Python, Kubernetes, Kafka, Flink, and stateful consumers.

- [ ] **Step 1: Create \`docs/standards/README.md\`**

Define precedence: applicable law and platform requirements, then \`Instructions.md\`, then standards in this directory, then a change-specific approved design, then local module conventions. Link every standards file and specify that deviations require a written owner, expiry date, risk, compensating controls, and approval in the applicable design record.

- [ ] **Step 2: Create \`docs/standards/engineering.md\`**

Set production rules for Java, Python, container images, and Kubernetes manifests: explicit error handling; bounded retries with timeouts; no swallowed exceptions; deterministic configuration; least privilege; non-root containers; probes that reflect actual readiness; immutable image tags; resource requests and limits; and tests for critical behavior. Ban committing generated files, local IDE metadata, dependency caches, and runtime artifacts unless intentionally versioned with a documented reason.

- [ ] **Step 3: Create \`docs/standards/data-contracts.md\`**

Define Kafka topic ownership, compatibility review, schema versioning, consumer-group stability, idempotency keys, DLQ behavior, replay safety, retention impact, and external-side-effect handling. Require every interface change to declare producer/consumer rollout order, backward compatibility, monitoring signals, and rollback behavior.

- [ ] **Step 4: Verify index coverage and link targets**

Run:

\`\`\`bash
rg -n '\\]\\((engineering|data-contracts|reliability-operations|security|testing-release)\\.md\\)' docs/standards/README.md
test -f docs/standards/engineering.md && test -f docs/standards/data-contracts.md
\`\`\`

Expected: five indexed standards links and zero exit status for existing files.

- [ ] **Step 5: Commit the independently reviewable change**

\`\`\`bash
git add docs/standards/README.md docs/standards/engineering.md docs/standards/data-contracts.md
git commit -m "docs: define engineering and data contract standards"
\`\`\`

### Task 4: Add reliability, security, and release standards

**Files:**
- Create: \`docs/standards/reliability-operations.md\`
- Create: \`docs/standards/security.md\`
- Create: \`docs/standards/testing-release.md\`
- Test: all three files

**Interfaces:**
- Consumes: engineering and data-contract requirements from Task 3.
- Produces: production readiness gates used by all future designs, deployments, and runbooks.

- [ ] **Step 1: Create \`docs/standards/reliability-operations.md\`**

Require service ownership, measurable SLIs before numeric SLO adoption, dashboard and alert links, runbooks for known failure modes, log/metric/trace correlation, capacity assumptions, backup/restore verification, incident evidence preservation, and scheduled resilience exercises. Require noisy or unactionable alerts to be corrected rather than ignored.

- [ ] **Step 2: Create \`docs/standards/security.md\`**

Require secrets to be stored outside Git and injected through approved secret-management paths; prohibit secret values in logs and documentation; require least-privilege RBAC, image provenance and vulnerability review, dependency updates, access review, and threat-model review for externally reachable or money-moving flows.

- [ ] **Step 3: Create \`docs/standards/testing-release.md\`**

Require tests at the lowest useful level, integration coverage for service boundaries, reproducible builds, reviewable deployment diffs, explicit rollout and rollback criteria, post-deploy verification, and change records. Define a release as incomplete when validation evidence is absent, regardless of a successful command exit code.

- [ ] **Step 4: Verify production coverage**

Run:

\`\`\`bash
rg -n -i 'runbook|rollback|secret|least privilege|integration|post-deploy' \\
  docs/standards/reliability-operations.md \\
  docs/standards/security.md \\
  docs/standards/testing-release.md
\`\`\`

Expected: at least one match for every listed control concept.

- [ ] **Step 5: Commit the independently reviewable change**

\`\`\`bash
git add docs/standards/reliability-operations.md docs/standards/security.md docs/standards/testing-release.md
git commit -m "docs: add production operations and release standards"
\`\`\`

### Task 5: Validate the documentation foundation as one unit

**Files:**
- Modify: \`docs/superpowers/specs/2026-07-31-production-documentation-foundation-design.md\` only if its status needs an evidence-backed update
- Test: all files created by Tasks 1–4

**Interfaces:**
- Consumes: all documentation entry points and standards.
- Produces: a reviewable validation record with no unverified completion claim.

- [ ] **Step 1: Run structural checks**

Run:

\`\`\`bash
test -f Instructions.md
test -f design/README.md
test -f wiki/README.md
test -f docs/standards/README.md
test -f docs/standards/engineering.md
test -f docs/standards/data-contracts.md
test -f docs/standards/reliability-operations.md
test -f docs/standards/security.md
test -f docs/standards/testing-release.md
git diff --check -- Instructions.md design wiki docs/standards
\`\`\`

Expected: zero exit status.

- [ ] **Step 2: Run placeholder and internal-link checks**

Run:

\`\`\`bash
rg -n -i 'T[O]DO|T[B]D|place[ ]holder' Instructions.md design wiki docs/standards && exit 1 || true
rg -n -F '](../Instructions.md)' design/README.md wiki/README.md
rg -n -F '](../../Instructions.md)' docs/standards/README.md
\`\`\`

Expected: no placeholders; every entry point links to \`Instructions.md\` using its correct relative path.

- [ ] **Step 3: Review the scoped diff**

Run:

\`\`\`bash
git diff --check
git status --short -- Instructions.md design wiki docs/standards docs/superpowers/specs/2026-07-31-production-documentation-foundation-design.md docs/superpowers/plans/2026-07-31-production-documentation-foundation.md
\`\`\`

Expected: only the intended documentation files are reported for this work.

- [ ] **Step 4: Commit the implementation plan and completed documentation set**

\`\`\`bash
git add Instructions.md design wiki docs/standards docs/superpowers/specs/2026-07-31-production-documentation-foundation-design.md docs/superpowers/plans/2026-07-31-production-documentation-foundation.md
git commit -m "docs: establish production documentation foundation"
\`\`\`
