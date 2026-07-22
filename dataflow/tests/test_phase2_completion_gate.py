import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import phase2_completion_gate as gate


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
JAVA_CONTEXT = (
    "datastream/employee-message-processor/src/main/java/"
    "com/expert/bigdata/app/EmployeeMessageProcessor.java"
)
PLAN_CONTEXT = "docs/superpowers/plans/2026-07-16-phase0-production-debts.md"
SCHEMA_CONTEXT = "schema.sql"
FIXTURE_CONTEXT = "dataflow/tests/test_trade_worker_config.py"


def test_phase2_completion_gate_accepts_the_tracked_repository():
    assert gate.audit_tracked_files() == []


def test_java_db_password_argument_is_found_after_line_movement():
    literal = "punctuated!@#[]"
    text = "\n" * 6 + f'// --dbPassword "{literal}"\n'

    assert gate.audit_text(JAVA_CONTEXT, text) == [
        f"{JAVA_CONTEXT}:7: literal credential value"
    ]


def test_phase0_mysql_password_argument_is_found_after_line_movement():
    literal = "punctuated!@#[]"
    text = "intro\nmore intro\n" + f'mysql -uroot -p"{literal}" -e "SELECT 1"\n'

    assert gate.audit_text(PLAN_CONTEXT, text) == [
        f"{PLAN_CONTEXT}:3: literal credential value"
    ]


def test_actual_schema_admin_slash_literal_comment_is_found_after_line_movement():
    literal = "punctuated!@#[]"
    text = "header\n" + f"-- 2. 插入管理员账号 (admin / {literal})\n"

    assert gate.audit_text(SCHEMA_CONTEXT, text) == [
        f"{SCHEMA_CONTEXT}:2: literal credential value"
    ]


def test_phase2_fixture_password_literal_is_rejected():
    literal = "punctuated!@#[]"
    text = f'"mysql_password": "{literal}"\n'

    assert gate.audit_text(FIXTURE_CONTEXT, text) == [
        f"{FIXTURE_CONTEXT}:1: literal credential value"
    ]


def test_only_deliberate_credential_placeholders_are_allowed():
    allowed = ("${UPPER_ENV_NAME}", "<redacted>", "<placeholder>", "<injected-for-test>")
    rejected = ("$lower_env", "%s", "<anything>", "punctuated!@#[]")

    for value in allowed:
        assert gate.audit_text(JAVA_CONTEXT, f'// --dbPassword "{value}"') == []
    for value in rejected:
        assert gate.audit_text(JAVA_CONTEXT, f'// --dbPassword "{value}"') == [
            f"{JAVA_CONTEXT}:1: literal credential value"
        ]


@pytest.mark.parametrize(
    ("relative_path", "text"),
    [
        (JAVA_CONTEXT, '// --dbPassword "${UPPER_ENV_NAME}"'),
        (PLAN_CONTEXT, 'mysql -uroot -p"${UPPER_ENV_NAME}" -e "SELECT 1"'),
        (SCHEMA_CONTEXT, "-- 2. 插入管理员账号 (admin / <redacted>)"),
        (FIXTURE_CONTEXT, '"mysql_password": "${UPPER_ENV_NAME}",'),
    ],
)
def test_each_scoped_context_accepts_an_exact_deliberate_placeholder(relative_path, text):
    assert gate.audit_text(relative_path, text) == []


@pytest.mark.parametrize(
    ("relative_path", "text"),
    [
        (JAVA_CONTEXT, '// --dbPassword "${UPPER_ENV}"suffix'),
        (JAVA_CONTEXT, "// --dbPassword prefix${UPPER_ENV}"),
        (JAVA_CONTEXT, '// --dbPassword "${UPPER_ENV}" + "suffix"'),
        (PLAN_CONTEXT, 'mysql -uroot -p"${UPPER_ENV}"suffix -e "SELECT 1"'),
        (PLAN_CONTEXT, 'mysql -uroot -pprefix${UPPER_ENV} -e "SELECT 1"'),
        (PLAN_CONTEXT, 'mysql -uroot -p"${UPPER_ENV}" + "suffix" -e "SELECT 1"'),
        (SCHEMA_CONTEXT, '-- 2. 插入管理员账号 (admin / "<redacted>"suffix)'),
        (SCHEMA_CONTEXT, "-- 2. 插入管理员账号 (admin / prefix<redacted>)"),
        (SCHEMA_CONTEXT, '-- 2. 插入管理员账号 (admin / "<redacted>" + "suffix")'),
        (FIXTURE_CONTEXT, '"mysql_password": "<injected-for-test>"suffix,'),
        (FIXTURE_CONTEXT, '"mysql_password": prefix<injected-for-test>,'),
        (FIXTURE_CONTEXT, '"mysql_password": "<injected-for-test>" + "suffix",'),
    ],
)
def test_safe_placeholder_must_be_the_complete_context_value(relative_path, text):
    assert gate.audit_text(relative_path, text) == [
        f"{relative_path}:1: literal credential value"
    ]


def test_service_and_schema_identifiers_are_not_credential_contexts():
    text = "\n".join(
        [
            "jdbc:mysql://mysql:3306/service_schema",
            "CREATE SCHEMA service_schema",
            'String serviceName = "streampark";',
        ]
    )

    assert gate.audit_text(JAVA_CONTEXT, text) == []


def test_address_gate_rejects_orb_host_and_concrete_ipv4():
    target = "dataflow/eth_trade_dataflow/market_data_collector.py"
    text = 'host = "api.orb.local"\nfallback = "192.0.2.17"\n'

    assert gate.address_findings(target, text) == [
        f"{target}:1: forbidden host dependency",
        f"{target}:2: forbidden host dependency",
    ]


def test_address_gate_rejects_concrete_ipv6_and_explicit_host_aliases():
    target = "dataflow/eth_info_dataflow/rss_to_eth_social_stream.py"
    text = "\n".join(
        [
            'ipv6 = "2001:db8::17"',
            'orb_alias = "host.orb.internal"',
            'docker_alias = "host.docker.internal"',
        ]
    )

    assert gate.address_findings(target, text) == [
        f"{target}:1: forbidden host dependency",
        f"{target}:2: forbidden host dependency",
        f"{target}:3: forbidden host dependency",
    ]


def test_address_gate_allows_exact_shared_health_listener_bind():
    text = 'server = ThreadingHTTPServer(("0.0.0.0", port), Handler)\n'

    assert gate.address_findings(gate.HEALTH_RUNTIME, text) == []


def test_address_gate_rejects_non_listener_zero_address():
    text = 'dependency = "0.0.0.0"\n'

    assert gate.address_findings(gate.HEALTH_RUNTIME, text) == [
        f"{gate.HEALTH_RUNTIME}:1: forbidden host dependency"
    ]


@pytest.mark.parametrize("missing", gate.PHASE2_ADDRESS_PATHS)
def test_repository_gate_fails_closed_when_required_address_target_is_missing(
    monkeypatch, missing
):
    tracked = set(gate.AUDITED_CREDENTIAL_CONTEXTS) | set(gate.PHASE2_ADDRESS_PATHS)
    tracked.remove(missing)
    monkeypatch.setattr(gate, "tracked_files", lambda: tuple(tracked))

    findings = gate.audit_tracked_files()

    assert f"{missing}:0: required address target is not tracked" in findings


def test_cli_prints_only_findings_and_returns_nonzero(monkeypatch, capsys):
    finding = "fixture.txt:9: literal credential value"
    monkeypatch.setattr(gate, "audit_tracked_files", lambda: [finding])

    assert gate.main() == 1
    assert capsys.readouterr().out == f"{finding}\n"


def test_cli_is_quiet_and_returns_zero_when_clean(monkeypatch, capsys):
    monkeypatch.setattr(gate, "audit_tracked_files", lambda: [])

    assert gate.main() == 0
    assert capsys.readouterr().out == ""


def test_cli_entrypoint_is_cwd_independent_when_repository_is_clean(tmp_path):
    script = REPOSITORY_ROOT / "dataflow/phase2_completion_gate.py"

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


def test_retrain_cronjob_has_read_only_root_and_declared_writable_mounts():
    manifest_path = REPOSITORY_ROOT / "infra/k8s/collectors/retrain-cronjob.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    pod_spec = manifest["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    container = next(item for item in pod_spec["containers"] if item["name"] == "model-retrain")

    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    mounts = {item["name"]: item for item in container["volumeMounts"]}
    assert {name: item["mountPath"] for name, item in mounts.items()} == {
        "artifacts": "/artifacts",
        "tmp": "/tmp",
    }
    assert mounts["artifacts"].get("readOnly", False) is False
    assert mounts["tmp"].get("readOnly", False) is False
    volumes = {item["name"]: item for item in pod_spec["volumes"]}
    assert volumes["artifacts"]["persistentVolumeClaim"]["claimName"] == "collector-artifacts"
    assert volumes["tmp"]["emptyDir"] == {}


def test_schema_comment_describes_fixed_hash_without_environment_provisioning():
    schema = (REPOSITORY_ROOT / SCHEMA_CONTEXT).read_text()

    assert "Fixed development-only administrator credential hash" in schema
    assert "administrator credential comes from ${MYSQL_PASSWORD}" not in schema
