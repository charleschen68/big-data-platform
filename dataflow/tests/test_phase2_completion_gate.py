from pathlib import Path

from phase2_completion_gate import address_findings, audit_text, audit_tracked_files


def test_phase2_completion_gate_accepts_the_tracked_repository():
    assert audit_tracked_files() == []


def test_credential_gate_distinguishes_password_contexts_from_identifiers():
    credential_key = "mysql_" + "password"
    findings = audit_text(
        "fixture.txt",
        "\n".join(
            [
                "jdbc:mysql://mysql:3306/service_schema",
                "CREATE SCHEMA service_schema",
                f'{credential_key} = "literal"',
            ]
        ),
    )
    assert findings == ["fixture.txt:3: literal credential value"]


def test_address_gate_allows_only_the_health_listener_bind_address():
    listener = 'ThreadingHTTPServer(("0." + "0.0.0", port), Handler)'
    assert address_findings("dataflow/collector_runtime/health.py", listener) == []

    dependency = "api." + "orb.local"
    assert address_findings(
        "dataflow/eth_trade_dataflow/market_data_collector.py", dependency
    ) == ["dataflow/eth_trade_dataflow/market_data_collector.py:1: forbidden host dependency"]


def test_retrain_cronjob_uses_a_read_only_root_filesystem():
    manifest = Path("infra/k8s/collectors/retrain-cronjob.yaml").read_text()
    container = manifest.split("- name: model-retrain", maxsplit=1)[1]
    assert "readOnlyRootFilesystem: true" in container.split("volumeMounts:", maxsplit=1)[0]
