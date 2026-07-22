# Task 5 report: independent workload images

## Implemented files

- `dataflow/docker/rss.Dockerfile`
- `dataflow/docker/market.Dockerfile`
- `dataflow/docker/settlement.Dockerfile`
- `dataflow/docker/retrain.Dockerfile`
- `dataflow/requirements/rss.txt`
- `dataflow/requirements/market.txt`
- `dataflow/requirements/settlement.txt`
- `dataflow/requirements/retrain.txt`
- `infra/scripts/build-and-import-collectors.sh` (executable)

Each Dockerfile uses `python:3.11-slim-bookworm`, creates `collector` with uid/gid `10001`, copies only its runtime code and entrypoint, and runs as that non-root user. The requirements files contain the exact versions from the Task 5 brief. The import script uses `set -euo pipefail`, builds `linux/arm64` images, imports each into `k3s-node`, and lists the imported `big-data/` images.

## Validation

Static checks passed:

```bash
bash -n infra/scripts/build-and-import-collectors.sh
git diff --check -- dataflow/docker dataflow/requirements infra/scripts/build-and-import-collectors.sh
# requirement pin format, Python 3.11 base, uid/gid 10001, USER collector, and executable-bit assertions
```

The first build attempt from the restricted environment was blocked by access to the Docker socket:

```text
ERROR: permission denied while trying to connect to the docker API at unix:///Users/ad/.orbstack/run/docker.sock
```

With approved host Docker/OrbStack access, the required command completed:

```bash
infra/scripts/build-and-import-collectors.sh
```

The k3s image listing contained all four arm64 images:

```text
docker.io/big-data/rss-collector:phase2
docker.io/big-data/market-collector:phase2
docker.io/big-data/settlement-worker:phase2
docker.io/big-data/model-retrain:phase2
```

The required non-root smoke check passed for every image:

```bash
for image in big-data/rss-collector:phase2 big-data/market-collector:phase2 big-data/settlement-worker:phase2 big-data/model-retrain:phase2; do
  docker run --rm --entrypoint id "$image"
done
```

Each invocation returned:

```text
uid=10001(collector) gid=10001(collector) groups=10001(collector)
```

## Concerns

- The retraining image is 725.8 MiB after import, largely because its pinned scientific and machine-learning dependencies include PyArrow, SciPy, XGBoost, and Milvus Lite. This task requires those exact runtime dependencies; reducing its size requires a separately approved dependency/image design change.
- Pip emits its expected build-time warning about installation as root. The runtime image uses the non-root `collector` user, confirmed by the smoke checks.

## Review remediation (2026-07-22)

The review findings were remediated as follows:

- Added a root allowlist `.dockerignore`. The only application inputs retained in a build context are the four Dockerfiles, the four locked `.txt` files, `collector_runtime`, and the four copied entrypoints. `.git`, `secrets`, `data`, lock-generation inputs, and Python bytecode remain excluded.
- `infra/scripts/build-and-import-collectors.sh` derives the repository root from `${BASH_SOURCE[0]}` and changes into it before invoking Docker.
- All four `FROM` lines are pinned to the current `linux/arm64/v8` manifest digest for `python:3.11-slim-bookworm`: `sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1`.
- Kept the brief's direct versions in four `.in` files and generated complete Python 3.11 `--generate-hashes` locks in the image-facing `.txt` files. The Dockerfiles now use `pip install --require-hashes`. `dataflow/requirements/compile-locks.sh` regenerates them in the pinned `linux/arm64` Python image with `pip-tools==7.5.3`.

### Remediation validation

The base-image digest was obtained from Docker Hub with:

```bash
docker buildx imagetools inspect python:3.11-slim-bookworm
```

It reported the arm64 manifest shown above (Python `3.11.15-slim-bookworm`).

The build-import script regression test was run from `/tmp` with Docker and Orb test doubles that verify the `-f` argument exists. The pre-fix script returned `missing Dockerfile: dataflow/docker/rss.Dockerfile` with exit 42; after the root-resolution change it completed with four build invocations and four import invocations:

```bash
(cd /tmp && PATH="$test_dir/bin:$PATH" TASK5_LOG="$test_dir/log" \
  /Users/ad/big-data-platform/infra/scripts/build-and-import-collectors.sh)
rg -c '^docker build --platform linux/arm64 -f dataflow/docker/' "$test_dir/log" # 4
rg -c '^orb -m k3s-node -u root k3s ctr images import -$' "$test_dir/log"     # 4
```

The target-platform locks were generated from outside the repository with:

```bash
(cd /tmp && /Users/ad/big-data-platform/dataflow/requirements/compile-locks.sh)
```

The Docker build context transfers were 66.12 kB for RSS and 117.42 kB for market, while the excluded working-tree `data/` directory alone is 930 MiB and `.git` is 17 MiB.

All four arm64 images were rebuilt and reimported by running from outside the repository:

```bash
(cd /tmp && /Users/ad/big-data-platform/infra/scripts/build-and-import-collectors.sh)
```

The k3s image listing confirmed `rss-collector`, `market-collector`, `settlement-worker`, and `model-retrain`, all at `linux/arm64`. The required smoke check passed for every image:

```text
big-data/rss-collector:phase2: uid=10001(collector) gid=10001(collector) groups=10001(collector)
big-data/market-collector:phase2: uid=10001(collector) gid=10001(collector) groups=10001(collector)
big-data/settlement-worker:phase2: uid=10001(collector) gid=10001(collector) groups=10001(collector)
big-data/model-retrain:phase2: uid=10001(collector) gid=10001(collector) groups=10001(collector)
```

Additional static validation and the existing collector test suite passed:

```bash
bash -n infra/scripts/build-and-import-collectors.sh dataflow/requirements/compile-locks.sh
pytest -q dataflow/tests
# 34 passed in 0.98s
```

The correct committed-range whitespace check for the original Task 5 commit also passed with no output:

```bash
git diff --check 8c947a2..a6050b5
```

## Lock-generator remediation (2026-07-22)

`dataflow/requirements/compile-locks.sh` now derives its repository root from
`${BASH_SOURCE[0]}`, then bind-mounts only
`${REPO_ROOT}/dataflow/requirements` at `/work`; the repository root is no
longer exposed to the networked generator container. The mount is writable
because `pip-compile` must replace the four image-facing lock files.

The committed `bootstrap.in` pins `pip-tools==7.5.3`; its generated
`bootstrap.txt` pins and hashes all generator dependencies (`build`, `click`,
`packaging`, `pip`, `pip-tools`, `pyproject-hooks`, `setuptools`, and `wheel`).
The generator creates an isolated venv and installs that file with
`pip install --require-hashes --requirement bootstrap.txt`. It uses the same
immutable arm64 Python digest as the collector images:

```text
python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1
```

Default `compile-locks.sh` intentionally omits both `--upgrade` and
`--rebuild`. It validates each lock with `--dry-run --constraint "${name}.txt"`,
so existing resolved transitive versions and lock-file bytes are retained.
Deliberate dependency refreshes require `compile-locks.sh --refresh`, which
adds both flags and writes regenerated locks.

### Lock-generator remediation validation

Focused static/script regression coverage passed from the repository and runs
the generator through a fake Docker executable while its working directory is
`/tmp`:

```bash
bash -n dataflow/requirements/compile-locks.sh
pytest -q dataflow/tests/test_compile_locks_script.py
# 5 passed in 0.90s
```

The five checks verify the single requirements-only bind mount and `/tmp`
root resolution, `--require-hashes` bootstrap installation and hashes for all
toolchain artifacts, the immutable arm64 base digest, default non-upgrade
behavior with existing-lock constraints and dry-run preservation, and explicit
`--refresh` upgrade/rebuild behavior.

The committed bootstrap lock was independently installed in a new virtual
environment with the exact enforcement used by the generator:

```bash
/tmp/task5-bootstrap-verify/bin/pip install --disable-pip-version-check --no-input \
  --require-hashes --requirement dataflow/requirements/bootstrap.txt
/tmp/task5-bootstrap-verify/bin/pip-compile --version
# pip-compile, version 7.5.3
```

The actual pinned arm64 generator was also run from `/tmp` in default mode;
its default dry-run validation completed successfully and did not change any
image-facing lock:

```bash
(cd /tmp && /Users/ad/big-data-platform/dataflow/requirements/compile-locks.sh \
  >/tmp/task5-compile-locks.log 2>&1)
git diff --quiet -- dataflow/requirements/rss.txt dataflow/requirements/market.txt \
  dataflow/requirements/settlement.txt dataflow/requirements/retrain.txt
# exit 0
```

The complete existing dataflow suite and the scoped whitespace check also
passed:

```bash
pytest -q dataflow/tests
# 39 passed in 1.66s
git diff --check -- .dockerignore dataflow/requirements/compile-locks.sh \
  dataflow/requirements/bootstrap.in dataflow/requirements/bootstrap.txt \
  dataflow/tests/test_compile_locks_script.py
```

No image-facing lock changed (`rss.txt`, `market.txt`, `settlement.txt`, and
`retrain.txt`), so collector images were not rebuilt or reimported. The
bootstrap lock is excluded from the collector build context because no image
uses it.

### Concerns

`--refresh` deliberately permits resolver upgrades; review and commit its lock
diffs as a dependency update. The default mode remains the reproducible,
transitive-version-preserving path.

## Final lock-generator remediation (2026-07-22)

Default `compile-locks.sh` now bind-mounts the requirements directory with
`readonly`. For each image-facing lock it first runs a hash-enforced
`pip install --dry-run --require-hashes`, then creates a no-header candidate in
container-local `/tmp` with the committed lock as a constraint. The comparison
retains every dependency and hash line verbatim while excluding only generated
headers and pip-tools `# via` annotations, which change when constraints are
present but do not alter the resolved graph or artifacts. A dependency/hash
body mismatch reports the affected lock and exits nonzero. `--refresh` keeps a
writable requirements mount and is the only mode that runs `pip-compile` with
`--upgrade --rebuild` against the committed output paths.

### Final lock-generator validation

The focused regression suite uses an executing Docker double and verifies the
default read-only mount, refresh writable mount, four hash dry-run installs,
container-local no-header candidate generation with committed constraints,
mismatch failure, and refresh rewrites:

```bash
bash -n dataflow/requirements/compile-locks.sh
pytest -q dataflow/tests/test_compile_locks_script.py
# 6 passed in 2.56s
git diff --check -- dataflow/requirements/compile-locks.sh \
  dataflow/tests/test_compile_locks_script.py
```

The full dataflow suite also passed:

```bash
pytest -q dataflow/tests
# 40 passed in 4.45s
```

The real pinned arm64 default mode was launched from `/tmp`, with its output
captured at `/tmp/task5-default-lock-validation-final.log`. Docker reported
the validation container (`54001cb1ff33`) exited successfully:

```bash
docker events --since 10m --until 2026-07-22T11:25:28Z \
  --filter container=54001cb1ff33 \
  --format '{{.Action}} {{.Actor.Attributes.exitCode}}'
# create <no value>
# attach <no value>
# start <no value>
# die 0
# destroy <no value>

git diff --quiet -- dataflow/requirements/rss.txt \
  dataflow/requirements/market.txt \
  dataflow/requirements/settlement.txt \
  dataflow/requirements/retrain.txt
# exit 0
```

No image-facing lock changed, so no collector image rebuild or reimport was
needed.
