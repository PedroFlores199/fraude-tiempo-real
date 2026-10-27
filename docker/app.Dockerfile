FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements/ requirements/
RUN pip install -r requirements/app.txt

RUN useradd --uid 50000 --gid 0 --create-home app \
    && mkdir -p /opt/modelos \
    && chown -R 50000:0 /opt/modelos \
    && chmod -R 775 /opt/modelos

COPY esquema/ esquema/
COPY comun/ comun/
COPY generador/ generador/
COPY streaming/ streaming/
COPY entrenamiento/ entrenamiento/
COPY api/ api/
COPY panel/ panel/
COPY carga/ carga/
COPY docs/ docs/

USER 50000
