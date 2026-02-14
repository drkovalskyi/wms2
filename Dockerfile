FROM python:3.11-slim AS base

WORKDIR /app

FROM base AS builder

COPY pyproject.toml .
RUN pip install --no-cache-dir .

FROM base AS runtime

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src/ src/
COPY alembic.ini .

EXPOSE 8000

CMD ["uvicorn", "wms2.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
