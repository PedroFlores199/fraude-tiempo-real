FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PYSPARK_PYTHON=python \
    PIP_NO_CACHE_DIR=1 \
    SPARK_IVY=/opt/ivy

WORKDIR /app

COPY requirements/ requirements/
RUN pip install -r requirements/streaming.txt

ARG SPARK_PAQUETES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
ENV SPARK_PAQUETES=${SPARK_PAQUETES}
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', '${SPARK_PAQUETES}').config('spark.jars.ivy', '/opt/ivy').getOrCreate().stop()"

RUN useradd --uid 50000 --gid 0 --create-home app \
    && mkdir -p /opt/modelos /opt/checkpoints \
    && chown -R 50000:0 /opt/ivy /opt/modelos /opt/checkpoints \
    && chmod -R 775 /opt/ivy /opt/modelos /opt/checkpoints

COPY esquema/ esquema/
COPY comun/ comun/
COPY streaming/ streaming/

USER 50000

CMD ["python", "-m", "streaming.app"]
