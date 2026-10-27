from datetime import UTC, datetime

import pytest

from comun.config import cargar_config
from streaming.app import ProcesadorDecisiones
from streaming.flujos import ESQUEMA_DECISION
from streaming.metricas import AcumuladorMetricas

pytestmark = pytest.mark.spark

FECHA = datetime(2026, 10, 27, 10, tzinfo=UTC)


class EscritorFalso:
    def __init__(self) -> None:
        self.filas = []

    def insertar(self, fila) -> None:
        self.filas.append(fila)


class Reloj:
    def __init__(self, *instantes: float) -> None:
        self._instantes = list(instantes)

    def __call__(self) -> float:
        return self._instantes.pop(0)


def test_publica_las_decisiones_y_guarda_las_metricas_del_lote(spark):
    lote = spark.createDataFrame(
        [
            ("t1", "TAR-1", "CLI-00001", FECHA, "BLOQUEADA", ["viaje"], 0.4, 250.0, FECHA),
            ("t2", "TAR-2", "CLI-00002", FECHA, "APROBADA", [], 0.01, 150.0, FECHA),
        ],
        ESQUEMA_DECISION,
    )
    publicados = []
    escritor = EscritorFalso()
    procesador = ProcesadorDecisiones(
        cargar_config(),
        AcumuladorMetricas(10, reloj=Reloj(0.0, 10.0)),
        escritor,
        publicar=lambda mensajes: publicados.append(mensajes.count()),
    )
    procesador(lote, 7)
    assert publicados == [2]
    fila = escritor.filas[0]
    assert fila.transacciones == 2
    assert fila.porcentaje_bloqueadas == 50.0
    assert fila.latencia_media_ms == 200.0


def test_un_lote_vacio_no_publica_nada(spark):
    publicados = []
    procesador = ProcesadorDecisiones(
        cargar_config(),
        AcumuladorMetricas(10, reloj=Reloj(0.0)),
        EscritorFalso(),
        publicar=publicados.append,
    )
    procesador(spark.createDataFrame([], ESQUEMA_DECISION), 0)
    assert publicados == []
