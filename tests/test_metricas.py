from datetime import UTC, datetime

import pytest

from streaming.metricas import AcumuladorMetricas, EscritorMetricas, FilaMetricas, ResumenLote

FECHA = datetime(2026, 10, 27, 10, tzinfo=UTC)


class Reloj:
    def __init__(self) -> None:
        self.ahora = 0.0

    def __call__(self) -> float:
        return self.ahora


def test_agrega_cada_10_segundos():
    reloj = Reloj()
    acumulador = AcumuladorMetricas(10, reloj=reloj, fecha=lambda: FECHA)
    reloj.ahora = 4
    assert acumulador.registrar(ResumenLote(400, 4, 10, 400_000.0, 1500.0)) is None
    reloj.ahora = 10.5
    fila = acumulador.registrar(ResumenLote(650, 6, 5, 520_000.0, 2500.0))
    assert fila.transacciones == 1050
    assert fila.transacciones_por_segundo == pytest.approx(100.0)
    assert fila.porcentaje_bloqueadas == pytest.approx(100 * 10 / 1050, abs=1e-3)
    assert fila.latencia_media_ms == pytest.approx(876.2, abs=0.1)
    assert fila.latencia_max_ms == 2500.0
    reloj.ahora = 12
    assert acumulador.registrar(ResumenLote(10, 0, 0, 100.0, 10.0)) is None


class Cursor:
    def __init__(self, ejecutadas: list) -> None:
        self._ejecutadas = ejecutadas

    def __enter__(self):
        return self

    def __exit__(self, *excepcion):
        return False

    def execute(self, sql, parametros):
        self._ejecutadas.append(parametros)


class Conexion:
    closed = False

    def __init__(self) -> None:
        self.ejecutadas: list = []

    def __enter__(self):
        return self

    def __exit__(self, *excepcion):
        return False

    def cursor(self):
        return Cursor(self.ejecutadas)


def test_escribe_en_postgres_y_reconecta_tras_un_error():
    conexion = Conexion()
    intentos = []

    def conectar(dsn):
        intentos.append(dsn)
        if len(intentos) == 1:
            raise ConnectionError("PostgreSQL no disponible")
        return conexion

    escritor = EscritorMetricas("postgresql://prueba", conectar=conectar)
    fila = FilaMetricas(FECHA, 10.0, 1000, 100.0, 1.5, 3.0, 800.0, 2000.0)
    escritor.insertar(fila)
    escritor.insertar(fila)
    assert len(intentos) == 2
    assert conexion.ejecutadas == [fila.como_tupla()]
