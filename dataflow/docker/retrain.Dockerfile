FROM python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector \
    && mkdir -p /artifacts/models /artifacts/backups \
    && chown -R collector:collector /artifacts
WORKDIR /app

COPY dataflow/requirements/retrain.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_info_dataflow/eth_model_retrain.py /app/dataflow/eth_info_dataflow/eth_model_retrain.py

USER collector
CMD ["python", "/app/dataflow/eth_info_dataflow/eth_model_retrain.py"]
