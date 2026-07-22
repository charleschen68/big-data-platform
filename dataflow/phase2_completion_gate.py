"""Source-only completion gates for the Phase 2 collector migration."""

from __future__ import annotations

import ipaddress
import re
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
HEALTH_RUNTIME = "dataflow/collector_runtime/health.py"
PHASE2_ADDRESS_PATHS = (
    "dataflow/eth_info_dataflow/rss_to_eth_social_stream.py",
    "dataflow/eth_trade_dataflow/market_data_collector.py",
    "dataflow/eth_trade_dataflow/eth_trade_settlement.py",
    "dataflow/eth_info_dataflow/eth_model_retrain.py",
    HEALTH_RUNTIME,
)
AUDITED_CREDENTIAL_CONTEXTS = {
    "datastream/employee-message-processor/src/main/java/com/expert/bigdata/app/EmployeeMessageProcessor.java": (18,),
    "docs/superpowers/plans/2026-07-16-phase0-production-debts.md": (648,),
    "schema.sql": (27,),
    "dataflow/tests/test_trade_worker_config.py": (83,),
}

_CREDENTIAL_ASSIGNMENT = re.compile(
    r"""(?ix)
    \b(?:[a-z_][a-z0-9_]*password|password)\b
    \s*(?:=|:)\s*
    (?P<value>\"[^\"\n]*\"|'[^'\n]*'|[^\s,#`]+)
    """
)
_PASSWORD_ARGUMENT = re.compile(
    r"""(?ix)
    --(?:db)?password\s*(?:=|\s)\s*
    (?P<value>\"[^\"\n]*\"|'[^'\n]*'|[^\s,#`]+)
    """
)
_MYSQL_PASSWORD_ARGUMENT = re.compile(
    r"""(?ix)
    \bmysql\b .*? \s-p
    (?P<value>\"[^\"\n]*\"|'[^'\n]*'|[^\s,#`]+)
    """
)
_PASSWORD_COMMENT = re.compile(
    r"""(?ix)
    ^\s*(?://|--|\#|/\*) .*? \bpassword\b
    \s*(?:is|:|=)\s*
    (?P<value>\"[^\"\n]*\"|'[^'\n]*'|[^\s,#`]+)
    """
)
_ORB_HOST = re.compile(r"\b[a-z0-9-]+\.orb\.local\b", re.IGNORECASE)
_IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")


def _is_allowed_credential_value(value: str) -> bool:
    value = value.strip().strip("\"'")
    return (
        not value
        or value.startswith("${") and value.endswith("}")
        or value.startswith("<") and value.endswith(">")
        or value.startswith("$")
        or value.startswith("%")
    )


def _is_literal_credential_value(value: str) -> bool:
    value = value.strip()
    if value.startswith(("\"", "'")):
        return True
    return re.fullmatch(r"[A-Za-z0-9._/@+=:-]+", value) is not None


def audit_text(relative_path: str, text: str, *, start_line: int = 1) -> list[str]:
    """Return credential findings without including credential values."""
    findings: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=start_line):
        for pattern in (
            _CREDENTIAL_ASSIGNMENT,
            _PASSWORD_ARGUMENT,
            _MYSQL_PASSWORD_ARGUMENT,
            _PASSWORD_COMMENT,
        ):
            for match in pattern.finditer(line):
                value = match.group("value")
                if not _is_allowed_credential_value(value) and _is_literal_credential_value(value):
                    findings.append(f"{relative_path}:{line_number}: literal credential value")
    return findings


def address_findings(relative_path: str, text: str) -> list[str]:
    """Return forbidden Phase 2 host dependencies without scanning legacy scripts."""
    findings: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if _ORB_HOST.search(line):
            findings.append(f"{relative_path}:{line_number}: forbidden host dependency")
        for match in _IPV4.finditer(line):
            address = match.group()
            try:
                ipaddress.IPv4Address(address)
            except ipaddress.AddressValueError:
                continue
            is_health_listener = (
                relative_path == HEALTH_RUNTIME
                and address == "0.0.0.0"
                and 'ThreadingHTTPServer(("0.0.0.0", port), Handler)' in line
            )
            if not is_health_listener:
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


def audit_tracked_files() -> list[str]:
    """Check only the audited Phase 2 collector credential contexts in Git."""
    findings: list[str] = []
    tracked = set(tracked_files())
    for relative_path, line_numbers in AUDITED_CREDENTIAL_CONTEXTS.items():
        if relative_path not in tracked:
            findings.append(f"{relative_path}: audited credential context is not tracked")
            continue
        lines = (REPOSITORY_ROOT / relative_path).read_text(errors="replace").splitlines()
        for line_number in line_numbers:
            if line_number > len(lines):
                findings.append(f"{relative_path}:{line_number}: audited credential context is missing")
                continue
            findings.extend(audit_text(relative_path, lines[line_number - 1], start_line=line_number))
    for relative_path in PHASE2_ADDRESS_PATHS:
        if relative_path in tracked:
            findings.extend(address_findings(relative_path, (REPOSITORY_ROOT / relative_path).read_text()))
    return findings
