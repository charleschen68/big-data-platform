"""Train the ETH sentiment model from settled Milvus trading records.

Historical records are retained by default.  Enabling deletion requires both an
explicit environment setting and a successfully written Parquet backup.
"""

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from collector_runtime.config import env_bool, env_int, env_str


logger = logging.getLogger(__name__)


def load_retrain_settings() -> dict[str, object]:
    artifact_root = Path(env_str("RETRAIN_ARTIFACT_ROOT", "/artifacts"))
    return {
        "milvus_host": env_str("MILVUS_HOST", "milvus"),
        "milvus_port": env_int("MILVUS_PORT", 19530),
        "collection": env_str("MILVUS_COLLECTION", "eth_sentiment_analysis"),
        "minimum_samples": env_int("RETRAIN_MINIMUM_SAMPLES", 100),
        "delete_after_backup": env_bool("RETRAIN_DELETE_AFTER_BACKUP", False),
        "model_path": artifact_root / "models" / "eth_sentiment_xgb.joblib",
        "backup_dir": artifact_root / "backups",
    }


def build_training_filter(now: datetime) -> str:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    six_months_ago = int((now - timedelta(days=180)).timestamp() * 1000)
    three_months_ago = int((now - timedelta(days=90)).timestamp() * 1000)
    return f"is_settled == true and pub_date >= {six_months_ago} and pub_date < {three_months_ago}"


def _temporary_path(directory: Path, suffix: str) -> Path:
    descriptor, path = tempfile.mkstemp(
        prefix=".eth_model_retrain_",
        suffix=suffix,
        dir=directory,
    )
    os.close(descriptor)
    return Path(path)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as artifact:
        os.fsync(artifact.fileno())


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_backup_atomically(dataframe: object, backup_dir: Path, now: datetime) -> Path:
    run_timestamp = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup_file = backup_dir / f"eth_data_backup_{run_timestamp}_{uuid4().hex}.parquet"
    if backup_file.exists():
        raise FileExistsError(f"refusing to overwrite existing backup: {backup_file}")

    temporary_file = _temporary_path(backup_dir, ".parquet.tmp")
    try:
        dataframe.to_parquet(temporary_file, engine="pyarrow")
        _fsync_file(temporary_file)
        os.replace(temporary_file, backup_file)
        _fsync_directory(backup_dir)
    except Exception:
        temporary_file.unlink(missing_ok=True)
        raise
    return backup_file


def _publish_model_atomically(joblib: object, model: object, model_path: Path) -> None:
    temporary_file = _temporary_path(model_path.parent, ".joblib.tmp")
    try:
        joblib.dump(model, temporary_file)
        _fsync_file(temporary_file)
        os.replace(temporary_file, model_path)
        _fsync_directory(model_path.parent)
    except Exception:
        temporary_file.unlink(missing_ok=True)
        raise


def train_and_cleanup() -> None:
    try:
        # Keep optional and native-backed dependencies out of module import so
        # configuration validation and safety controls remain testable.
        import joblib
        import numpy as np
        import pandas as pd
        from pymilvus import Collection, connections
        from xgboost import XGBRegressor

        settings = load_retrain_settings()
        model_path = settings["model_path"]
        backup_dir = settings["backup_dir"]
        if not isinstance(model_path, Path) or not isinstance(backup_dir, Path):
            raise TypeError("retraining artifact paths must be Path instances")

        connections.connect(
            "default",
            host=settings["milvus_host"],
            port=settings["milvus_port"],
        )
        collection = Collection(settings["collection"])
        collection.load()

        now = datetime.now(timezone.utc)
        training_filter = build_training_filter(now)
        six_months_ago = now - timedelta(days=180)
        three_months_ago = now - timedelta(days=90)
        logger.info(
            "Training settled records from %s through %s",
            six_months_ago.date(),
            three_months_ago.date(),
        )

        records = collection.query(
            expr=training_filter,
            output_fields=["event_id", "vector", "sentiment_score", "return", "pub_date"],
        )
        minimum_samples = settings["minimum_samples"]
        if not isinstance(minimum_samples, int):
            raise TypeError("RETRAIN_MINIMUM_SAMPLES must be an integer")
        if len(records) < minimum_samples:
            logger.warning(
                "Not retraining: %d samples available, %d required",
                len(records),
                minimum_samples,
            )
            return

        # A backup must be durable before any deletion is even considered.
        model_path.parent.mkdir(parents=True, exist_ok=True)
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_file = _write_backup_atomically(pd.DataFrame(records), backup_dir, now)
        logger.info("Backed up %d records to %s", len(records), backup_file)

        features = np.array(
            [[float(row["sentiment_score"]), *row["vector"]] for row in records]
        )
        labels = np.array([row["return"] for row in records])
        event_ids = [row["event_id"] for row in records]

        model = XGBRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            objective="reg:squarederror",
            tree_method="hist",
        )
        model.fit(features, labels)
        _publish_model_atomically(joblib, model, model_path)
        logger.info("Saved trained model to %s", model_path)

        delete_after_backup = settings["delete_after_backup"]
        if not isinstance(delete_after_backup, bool):
            raise TypeError("RETRAIN_DELETE_AFTER_BACKUP must be a boolean")
        if not delete_after_backup:
            logger.info("Retaining training records: RETRAIN_DELETE_AFTER_BACKUP is false")
            return

        for start in range(0, len(event_ids), 500):
            batch_ids = event_ids[start : start + 500]
            collection.delete(f"event_id in {json.dumps(batch_ids)}")
        collection.flush()
        logger.info("Deleted %d backed-up training records", len(event_ids))
    except Exception:
        logger.exception("Model retraining job failed")
        raise


if __name__ == "__main__":
    started_at = time.monotonic()
    try:
        train_and_cleanup()
    finally:
        logger.info("Model retraining job finished in %.2fs", time.monotonic() - started_at)
