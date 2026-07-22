import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "dataflow" / "requirements" / "compile-locks.sh"
REQUIREMENTS_DIR = REPO_ROOT / "dataflow" / "requirements"
LOCK_NAMES = ("rss", "market", "settlement", "retrain")


def _run_with_logging_docker(tmp_path: Path, *script_args: str) -> list[str]:
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


def _run_with_executing_docker(
    tmp_path: Path, *, candidate_mismatch: bool = False, refresh: bool = False
) -> tuple[subprocess.CompletedProcess[str], list[str], Path]:
    requirements_copy = tmp_path / "requirements"
    shutil.copytree(REQUIREMENTS_DIR, requirements_copy)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    command_log = tmp_path / "commands.txt"

    (fake_bin / "python").write_text(
        "#!/usr/bin/env sh\n"
        "if [ \"$1\" = -m ] && [ \"$2\" = venv ]; then\n"
        "  mkdir -p \"$3/bin\"\n"
        "  cat > \"$3/bin/pip\" <<'PIP'\n"
        "#!/usr/bin/env sh\n"
        "printf 'pip %s\\n' \"$*\" >> \"$TASK5_COMMAND_LOG\"\n"
        "PIP\n"
        "  cat > \"$3/bin/pip-compile\" <<'COMPILE'\n"
        "#!/usr/bin/env sh\n"
        "printf 'pip-compile %s\\n' \"$*\" >> \"$TASK5_COMMAND_LOG\"\n"
        "output= constraint=\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "  case \"$1\" in\n"
        "    --output-file) output=$2; shift 2 ;;\n"
        "    --constraint) constraint=$2; shift 2 ;;\n"
        "    *) shift ;;\n"
        "  esac\n"
        "done\n"
        "if [ -n \"$constraint\" ]; then\n"
        "  awk 'BEGIN { header = 1 } header && (/^#/ || /^[[:space:]]*$/) { next } /^[[:space:]]*#/ { next } /^[[:space:]]*$/ { next } { header = 0; print }' \"$constraint\" > \"$output\"\n"
        "  if [ \"${TASK5_CANDIDATE_MISMATCH:-0}\" = 1 ]; then printf 'unexpected==1 \\\\n    --hash=sha256:bad\\n' >> \"$output\"; fi\n"
        "else\n"
        "  printf 'refreshed==1 \\\\n    --hash=sha256:refreshed\\n' > \"$output\"\n"
        "fi\n"
        "COMPILE\n"
        "  chmod +x \"$3/bin/pip\" \"$3/bin/pip-compile\"\n"
        "  exit 0\n"
        "fi\n"
        "exec /usr/bin/python3 \"$@\"\n",
        encoding="utf-8",
    )
    (fake_bin / "docker").write_text(
        "#!/usr/bin/env sh\n"
        "printf '%s\\n' \"$@\" > \"$TASK5_DOCKER_LOG\"\n"
        "while [ \"$1\" != sh ]; do shift; done\n"
        "shift\n"
        "[ \"$1\" = -ec ]\n"
        "shift\n"
        "script=$(printf '%s' \"$1\" | sed \"s|/work|$TASK5_REQUIREMENTS_COPY|g\")\n"
        "shift\n"
        "exec sh -ec \"$script\" \"$@\"\n",
        encoding="utf-8",
    )
    for command in (fake_bin / "python", fake_bin / "docker"):
        command.chmod(0o755)

    docker_log = tmp_path / "docker-args.txt"
    environment = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "TASK5_DOCKER_LOG": str(docker_log),
        "TASK5_COMMAND_LOG": str(command_log),
        "TASK5_REQUIREMENTS_COPY": str(requirements_copy),
        "TASK5_CANDIDATE_MISMATCH": "1" if candidate_mismatch else "0",
    }
    result = subprocess.run(
        [str(SCRIPT), *( ["--refresh"] if refresh else [])],
        cwd="/tmp",
        env=environment,
        text=True,
        capture_output=True,
    )
    return result, command_log.read_text(encoding="utf-8").splitlines(), requirements_copy


def _argument_after(arguments: list[str], option: str) -> str:
    return arguments[arguments.index(option) + 1]


def test_compile_locks_default_mounts_requirements_read_only_from_tmp(tmp_path):
    arguments = _run_with_logging_docker(tmp_path)

    assert _argument_after(arguments, "--mount") == f"type=bind,src={REQUIREMENTS_DIR},dst=/work,readonly"
    assert arguments.count("--mount") == 1
    assert "--volume" not in arguments
    assert f"type=bind,src={REPO_ROOT},dst=/work" not in arguments
    assert _argument_after(arguments, "--workdir") == "/work"


def test_compile_locks_installs_hashed_bootstrap_toolchain():
    source = SCRIPT.read_text(encoding="utf-8")
    bootstrap = REQUIREMENTS_DIR / "bootstrap.txt"

    assert "--require-hashes --requirement /work/bootstrap.txt" in source
    assert "pip-tools==7.5.3" in bootstrap.read_text(encoding="utf-8")
    assert "--hash=sha256:" in bootstrap.read_text(encoding="utf-8")
    bootstrap_lines = bootstrap.read_text(encoding="utf-8").splitlines()
    for dependency in ("build", "click", "packaging", "pip", "pip-tools", "pyproject-hooks", "setuptools", "wheel"):
        dependency_line = next(index for index, line in enumerate(bootstrap_lines) if line.startswith(f"{dependency}=="))
        assert any("--hash=sha256:" in line for line in bootstrap_lines[dependency_line : dependency_line + 3])


def test_compile_locks_uses_the_collector_arm64_base_digest(tmp_path):
    arguments = _run_with_logging_docker(tmp_path)

    assert "python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1" in arguments


def test_compile_locks_default_hash_validates_and_compares_candidates(tmp_path):
    result, commands, requirements_copy = _run_with_executing_docker(tmp_path)

    assert result.returncode == 0, result.stderr
    assert sum(command.startswith("pip install --no-cache-dir --dry-run --require-hashes --requirement ") for command in commands) == 4
    candidate_commands = [command for command in commands if command.startswith("pip-compile ")]
    assert len(candidate_commands) == 4
    assert all("--no-header" in command and "--constraint" in command for command in candidate_commands)
    assert all("--output-file /tmp/" in command for command in candidate_commands)
    assert all("--upgrade" not in command and "--rebuild" not in command for command in candidate_commands)
    assert all((requirements_copy / f"{name}.txt").read_bytes() == (REQUIREMENTS_DIR / f"{name}.txt").read_bytes() for name in LOCK_NAMES)


def test_compile_locks_default_fails_when_candidate_dependency_or_hash_body_differs(tmp_path):
    result, commands, _ = _run_with_executing_docker(tmp_path, candidate_mismatch=True)

    assert result.returncode != 0
    assert "lock graph or hashes differ" in result.stderr
    assert any(command.startswith("pip-compile ") for command in commands)


def test_compile_locks_refresh_mounts_writable_and_rewrites_with_upgrade_rebuild(tmp_path):
    result, commands, requirements_copy = _run_with_executing_docker(tmp_path, refresh=True)
    arguments = (tmp_path / "docker-args.txt").read_text(encoding="utf-8").splitlines()

    assert result.returncode == 0, result.stderr
    assert _argument_after(arguments, "--mount") == f"type=bind,src={REQUIREMENTS_DIR},dst=/work"
    refresh_commands = [command for command in commands if command.startswith("pip-compile ")]
    assert len(refresh_commands) == 4
    assert all("--upgrade" in command and "--rebuild" in command for command in refresh_commands)
    assert all((requirements_copy / f"{name}.txt").read_text(encoding="utf-8").startswith("refreshed==1") for name in LOCK_NAMES)
