from collector_runtime.health import WorkloadHealth


def test_health_is_not_ready_before_initialization():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    assert health.status("/live") == (200, b"live\n")
    assert health.status("/ready") == (503, b"not ready\n")


def test_health_becomes_ready_after_heartbeat():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    health.mark_ready()
    health.heartbeat()
    assert health.status("/ready") == (200, b"ready\n")


def test_stale_heartbeat_fails_both_probes_after_startup():
    now = [10.0]
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: now[0])
    health.mark_ready()
    health.heartbeat()
    now[0] = 71.0
    assert health.status("/ready") == (503, b"stale\n")
    assert health.status("/live") == (503, b"stale\n")


def test_unknown_health_path_returns_not_found():
    health = WorkloadHealth(stale_after_seconds=60, clock=lambda: 10.0)
    assert health.status("/unknown") == (404, b"not found\n")
