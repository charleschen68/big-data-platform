EXPECTED_COLLECTOR_DEPLOYMENTS = {
    "rss-collector",
    "market-collector",
    "settlement-worker",
}


def collector_errors(deployments: dict, cronjob: dict, secret: dict) -> list[str]:
    errors = []
    available = {
        item["metadata"]["name"]: item.get("status", {}).get("availableReplicas", 0)
        for item in deployments.get("items", [])
    }
    for name in sorted(EXPECTED_COLLECTOR_DEPLOYMENTS):
        if available.get(name) != 1:
            errors.append(f"deployment {name} availableReplicas={available.get(name, 0)}")
    if cronjob.get("spec", {}).get("suspend", False):
        errors.append("cronjob model-retrain is suspended")
    if "MYSQL_PASSWORD" not in secret.get("data", {}):
        errors.append("collector-secrets is missing MYSQL_PASSWORD")
    return errors
