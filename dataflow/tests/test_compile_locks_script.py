import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "dataflow" / "requirements" / "compile-locks.sh"
REQUIREMENTS_DIR = REPO_ROOT / "dataflow" / "requirements"


def _run_with_fake_docker(tmp_path: Path, *script_args: str) -> list[str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "docker-args.txt"
    docker = fake_bin / "docker"
    docker.write_text(
        "#!/usr/bin/env sh\n"
        "printf '%s\\n' \"$@\" > \"$TASK5_DOCKER_LOG\"\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    environment = os.environ | {"PATH": f"{fake_bin}:{os.environ['PATH']}", "TASK5_DOCKER_LOG": str(log)}
    subprocess.run([str(SCRIPT), *script_args], cwd="/tmp", env=environment, check=True)
    return log.read_text(encoding="utf-8").splitlines()


def _argument_after(arguments: list[str], option: str) -> str:
    return arguments[arguments.index(option) + 1]


def test_compile_locks_mounts_only_requirements_and_resolves_from_tmp(tmp_path):
    arguments = _run_with_fake_docker(tmp_path)

    assert _argument_after(arguments, "--mount") == f"type=bind,src={REQUIREMENTS_DIR},dst=/work"
    assert arguments.count("--mount") == 1
    assert "--volume" not in arguments
    assert f"type=bind,src={REPO_ROOT},dst=/work" not in arguments
    assert _argument_after(arguments, "--workdir") == "/work"


def test_compile_locks_installs_hashed_bootstrap_toolchain():
    source = SCRIPT.read_text(encoding="utf-8")
    bootstrap = REQUIREMENTS_DIR / "bootstrap.txt"

    assert "--require-hashes --requirement bootstrap.txt" in source
    assert "pip-tools==7.5.3" in bootstrap.read_text(encoding="utf-8")
    assert "--hash=sha256:" in bootstrap.read_text(encoding="utf-8")
    bootstrap_lines = bootstrap.read_text(encoding="utf-8").splitlines()
    for dependency in ("build", "click", "packaging", "pip", "pip-tools", "pyproject-hooks", "setuptools", "wheel"):
        dependency_line = next(index for index, line in enumerate(bootstrap_lines) if line.startswith(f"{dependency}=="))
        assert any("--hash=sha256:" in line for line in bootstrap_lines[dependency_line : dependency_line + 3])


def test_compile_locks_uses_the_collector_arm64_base_digest(tmp_path):
    arguments = _run_with_fake_docker(tmp_path)

    assert "python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1" in arguments


def test_compile_locks_default_preserves_existing_transitives(tmp_path):
    arguments = _run_with_fake_docker(tmp_path)
    source = SCRIPT.read_text(encoding="utf-8")

    assert arguments[-1] == "default"
    default_branch = source.split('if [ "$1" = "refresh" ]; then', 1)[1].split("else", 1)[1].split("fi", 1)[0]
    assert "--upgrade" not in default_branch
    assert "--rebuild" not in default_branch
    assert '--constraint "${name}.txt"' in default_branch
    assert "--dry-run" in default_branch


def test_compile_locks_refresh_explicitly_upgrades(tmp_path):
    arguments = _run_with_fake_docker(tmp_path, "--refresh")
    source = SCRIPT.read_text(encoding="utf-8")

    assert arguments[-1] == "refresh"
    refresh_branch = source.split('if [ "$1" = "refresh" ]; then', 1)[1].split("else", 1)[0]
    assert "--upgrade" in refresh_branch
    assert "--rebuild" in refresh_branch
