"""Source-only completion gates for the Phase 2 collector migration."""

from __future__ import annotations

import ast
import ipaddress
import re
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
JAVA_CONTEXT = (
    "datastream/employee-message-processor/src/main/java/"
    "com/expert/bigdata/app/EmployeeMessageProcessor.java"
)
PLAN_CONTEXT = "docs/superpowers/plans/2026-07-16-phase0-production-debts.md"
SCHEMA_CONTEXT = "schema.sql"
FIXTURE_CONTEXT = "dataflow/tests/test_trade_worker_config.py"
HEALTH_RUNTIME = "dataflow/collector_runtime/health.py"

AUDITED_CREDENTIAL_CONTEXTS = {
    JAVA_CONTEXT: "documented Java dbPassword argument",
    PLAN_CONTEXT: "documented MySQL password command",
    SCHEMA_CONTEXT: "administrator password comment",
    FIXTURE_CONTEXT: "Phase 2 injected password fixture",
}
PHASE2_ADDRESS_PATHS = (
    "dataflow/eth_info_dataflow/rss_to_eth_social_stream.py",
    "dataflow/eth_trade_dataflow/market_data_collector.py",
    "dataflow/eth_trade_dataflow/eth_trade_settlement.py",
    "dataflow/eth_info_dataflow/eth_model_retrain.py",
    HEALTH_RUNTIME,
)

_JAVA_PASSWORD_ARGUMENT = re.compile(
    r"(?i)--dbpassword\s*(?:=|\s)\s*"
    r"(?P<value>.*?)(?=\s+--[a-z][\w-]*(?:\s|=)|\s*(?:\*/)?$)"
)
_SHELL_WORD = r'''(?:"[^"\n]*"|'[^'\n]*'|[^\s"']+)+'''
_MYSQL_PASSWORD_ARGUMENT = re.compile(
    rf"(?i)\bmysql\b.*?\s-p(?P<value>{_SHELL_WORD}(?:\s*\+\s*{_SHELL_WORD})?)"
)
_SCHEMA_PASSWORD_COMMENT = re.compile(
    r"(?ix)^\s*(?://|--|\#|/\*).*?\badmin\b\s*/\s*(?P<value>[^)\n]+)"
)
_FIXTURE_PASSWORD_ASSIGNMENT = re.compile(
    r'''(?ix)["']?mysql_password["']?\s*(?:=|:)\s*'''
    r'''(?P<value>.*?)(?=\s*,\s*$|\s*}\s*$|$)'''
)
_CREDENTIAL_PATTERNS = {
    JAVA_CONTEXT: (_JAVA_PASSWORD_ARGUMENT,),
    PLAN_CONTEXT: (_MYSQL_PASSWORD_ARGUMENT,),
    SCHEMA_CONTEXT: (_SCHEMA_PASSWORD_COMMENT,),
    FIXTURE_CONTEXT: (_FIXTURE_PASSWORD_ASSIGNMENT,),
}
_ENV_PLACEHOLDER = re.compile(r"\$\{[A-Z][A-Z0-9_]*\}")
_SAFE_SENTINELS = {"<redacted>", "<placeholder>", "<injected-for-test>"}
_FORBIDDEN_HOST = re.compile(
    r"\b(?:[a-z0-9-]+\.orb\.local|host\.orb\.internal|host\.docker\.internal)\b",
    re.IGNORECASE,
)
_IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
_IPV6_CANDIDATE = re.compile(r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?![\w:])", re.I)


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _is_allowed_credential_value(value: str) -> bool:
    value = _unquote(value)
    return not value or _ENV_PLACEHOLDER.fullmatch(value) is not None or value in _SAFE_SENTINELS


def audit_text(relative_path: str, text: str, *, start_line: int = 1) -> list[str]:
    """Find literals only in the four audited Phase 2 credential contexts."""
    patterns = _CREDENTIAL_PATTERNS.get(relative_path, ())
    findings: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=start_line):
        if any(
            not _is_allowed_credential_value(match.group("value"))
            for pattern in patterns
            for match in pattern.finditer(line)
        ):
            findings.append(f"{relative_path}:{line_number}: literal credential value")
    return findings


def _health_listener_bind_locations(text: str) -> set[tuple[int, int, int]]:
    """Locate the literal in the exact shared-health HTTP listener call."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()

    locations: set[tuple[int, int, int]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id != "ThreadingHTTPServer" or len(node.args) < 2:
            continue
        address_arg, handler_arg = node.args[:2]
        if not isinstance(address_arg, ast.Tuple) or len(address_arg.elts) != 2:
            continue
        host_arg, port_arg = address_arg.elts
        exact_listener = (
            isinstance(host_arg, ast.Constant)
            and host_arg.value == "0.0.0.0"
            and isinstance(port_arg, ast.Name)
            and port_arg.id == "port"
            and isinstance(handler_arg, ast.Name)
            and handler_arg.id == "Handler"
        )
        if exact_listener and host_arg.end_col_offset is not None:
            locations.add((host_arg.lineno, host_arg.col_offset, host_arg.end_col_offset))
    return locations


def _valid_ip(candidate: str) -> bool:
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False
    return True


def address_findings(relative_path: str, text: str) -> list[str]:
    """Find forbidden dependencies only in the five Phase 2 address targets."""
    listener_locations = (
        _health_listener_bind_locations(text) if relative_path == HEALTH_RUNTIME else set()
    )
    findings: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        forbidden = _FORBIDDEN_HOST.search(line) is not None
        for pattern in (_IPV4, _IPV6_CANDIDATE):
            for match in pattern.finditer(line):
                address = match.group()
                if not _valid_ip(address):
                    continue
                allowed_listener = (
                    relative_path == HEALTH_RUNTIME
                    and address == "0.0.0.0"
                    and any(
                        location_line == line_number
                        and location_start <= match.start()
                        and match.end() <= location_end
                        for location_line, location_start, location_end in listener_locations
                    )
                )
                forbidden = forbidden or not allowed_listener
        if forbidden:
            findings.append(f"{relative_path}:{line_number}: forbidden host dependency")
    return findings


def tracked_files() -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
    )
    return tuple(path for path in result.stdout.decode().split("\0") if path)


def _required_text(
    relative_path: str, tracked: set[str], category: str
) -> tuple[str | None, str | None]:
    if relative_path not in tracked:
        return None, f"{relative_path}:0: required {category} target is not tracked"
    path = REPOSITORY_ROOT / relative_path
    if not path.is_file():
        return None, f"{relative_path}:0: required {category} target is missing"
    return path.read_text(errors="replace"), None


def audit_tracked_files() -> list[str]:
    """Run the narrow Phase 2 collector source gate against tracked files."""
    findings: list[str] = []
    tracked = set(tracked_files())
    for relative_path in AUDITED_CREDENTIAL_CONTEXTS:
        text, missing = _required_text(relative_path, tracked, "credential")
        if missing:
            findings.append(missing)
        elif text is not None:
            findings.extend(audit_text(relative_path, text))
    for relative_path in PHASE2_ADDRESS_PATHS:
        text, missing = _required_text(relative_path, tracked, "address")
        if missing:
            findings.append(missing)
        elif text is not None:
            findings.extend(address_findings(relative_path, text))
    return findings


def main() -> int:
    findings = audit_tracked_files()
    for finding in findings:
        print(finding)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
