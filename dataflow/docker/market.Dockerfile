FROM python:3.11-slim-bookworm@sha256:3df1d95e3529533d0b646640edb63a0fde8a68597c0e7c62d34c4176678bb7d1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/dataflow \
    HOME=/tmp

RUN groupadd --system --gid 10001 collector \
    && useradd --system --uid 10001 --gid collector --home /app collector
WORKDIR /app

COPY dataflow/requirements/market.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes --requirement /tmp/requirements.txt

COPY --chown=collector:collector dataflow/collector_runtime /app/dataflow/collector_runtime
COPY --chown=collector:collector dataflow/eth_trade_dataflow/market_data_collector.py /app/dataflow/eth_trade_dataflow/market_data_collector.py

USER collector
EXPOSE 8080
CMD ["python", "/app/dataflow/eth_trade_dataflow/market_data_collector.py"]
