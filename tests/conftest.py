import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import fakeredis
import mongomock
import pytest

from comun.tiempo import a_iso


@pytest.fixture
def perfil() -> dict:
    return {
        "_id": "CLI-00001",
        "tipo": "particular",
        "ciudad": "Madrid",
        "ubicacion": {"lat": 40.4168, "lon": -3.7038},
        "tarjetas": [
            {
                "tarjeta_id": "TAR-00000000000a",
                "tipo": "debito",
                "activa": True,
                "limite_mensual": 3000.0,
            }
        ],
        "comercios_frecuentes": ["Mercadona", "Zara"],
        "categorias": {
            "alimentacion": {"gasto_medio": 40.0, "compras_mes": 12},
            "moda": {"gasto_medio": 60.0, "compras_mes": 3},
        },
        "horario_tipico": {"inicio": 8, "fin": 22},
        "fecha_alta": datetime(2024, 1, 1, tzinfo=UTC),
        "nombre": "Lucía",
        "apellidos": "García López",
        "edad": 34,
        "ocupacion": "docencia",
    }


@pytest.fixture
def nueva_transaccion():
    def crear(**cambios) -> dict:
        transaccion = {
            "id_transaccion": str(uuid.uuid4()),
            "cliente_id": "CLI-00001",
            "tarjeta_id": "TAR-00000000000a",
            "importe": 42.5,
            "moneda": "EUR",
            "comercio": "Mercadona",
            "categoria": "alimentacion",
            "canal": "presencial",
            "ciudad": "Madrid",
            "lat": 40.4168,
            "lon": -3.7038,
            "fecha_hora": a_iso(datetime(2026, 10, 27, 10, 30, tzinfo=UTC)),
        }
        if "fecha" in cambios:
            cambios["fecha_hora"] = a_iso(cambios.pop("fecha"))
        transaccion.update(cambios)
        return transaccion

    return crear


@pytest.fixture
def redis_falso():
    return fakeredis.FakeRedis()


@pytest.fixture
def mongo():
    return mongomock.MongoClient(tz_aware=True)["fraude"]


def version_java() -> int:
    salida = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    coincidencia = re.search(r'version "(\d+)', salida)
    return int(coincidencia.group(1)) if coincidencia else 0


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Spark necesita Java instalado")
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    raiz = str(Path(__file__).resolve().parent.parent)
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [raiz, os.environ.get("PYTHONPATH")]))
    sesion = (
        SparkSession.builder.master("local[2]")
        .appName("pruebas-fraude")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()


@pytest.fixture
def spark_con_arrow(spark):
    if version_java() > 17:
        pytest.skip("Arrow en Spark 3.5 necesita Java 17; mapInPandas no funciona con Java 21")
    return spark
