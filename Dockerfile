FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src
COPY config /app/config

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -e . \
    && useradd --create-home --shell /bin/bash etl \
    && mkdir -p /runtime/state /runtime/output /runtime/published \
    && chown -R etl:etl /app /runtime

USER etl

EXPOSE 8000

ENTRYPOINT ["etl-identity-engine"]
CMD ["--help"]
