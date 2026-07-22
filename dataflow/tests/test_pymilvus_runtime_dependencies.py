import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS_DIR = REPO_ROOT / "dataflow" / "requirements"
BUILD_SCRIPT = REPO_ROOT / "infra" / "scripts" / "build-and-import-collectors.sh"


def _pinned_versions(path: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([a-z0-9-]+)==([^ \\]+)", line)
        if match:
            versions[match.group(1)] = match.group(2)
    return versions


@pytest.mark.parametrize("workload", ("settlement", "retrain"))
def test_pymilvus_workloads_pin_setuptools_that_provides_pkg_resources(workload: str):
    declared = _pinned_versions(REQUIREMENTS_DIR / f"{workload}.in")
    locked = _pinned_versions(REQUIREMENTS_DIR / f"{workload}.txt")

    assert declared["pymilvus"] == "2.4.15"
    assert declared["setuptools"] == "81.0.0"
    assert locked["setuptools"] == "81.0.0"


def test_collector_build_checks_pymilvus_runtime_imports_before_importing_images():
    source = BUILD_SCRIPT.read_text(encoding="utf-8")

    assert 'if [[ "${name}" == "settlement" || "${name}" == "retrain" ]]; then' in source
    assert "docker run --rm --platform \"${PLATFORM}\" --entrypoint python \"${image}\" -c 'import pkg_resources; import pymilvus'" in source
