import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import psycopg2

from comun.tiempo import ahora_utc

logger = logging.getLogger(__name__)

SQL_INSERTAR = """
    INSERT INTO metricas (
        fecha, ventana_segundos, transacciones, transacciones_por_segundo,
        porcentaje_bloqueadas, porcentaje_revisadas, latencia_media_ms, latencia_max_ms
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (fecha) DO NOTHING
"""


@dataclass(frozen=True)
class ResumenLote:
    transacciones: int
    bloqueadas: int
    revisadas: int
    suma_latencia_ms: float
    max_latencia_ms: float


@dataclass(frozen=True)
class FilaMetricas:
    fecha: datetime
    ventana_segundos: float
    transacciones: int
    transacciones_por_segundo: float
    porcentaje_bloqueadas: float
    porcentaje_revisadas: float
    latencia_media_ms: float
    latencia_max_ms: float

    def como_tupla(self) -> tuple:
        return (
            self.fecha,
            self.ventana_segundos,
            self.transacciones,
            self.transacciones_por_segundo,
            self.porcentaje_bloqueadas,
            self.porcentaje_revisadas,
            self.latencia_media_ms,
            self.latencia_max_ms,
        )


class AcumuladorMetricas:
    def __init__(
        self,
        intervalo_s: float = 10.0,
        reloj: Callable[[], float] = time.monotonic,
        fecha: Callable[[], datetime] = ahora_utc,
    ) -> None:
        self._intervalo_s = intervalo_s
        self._reloj = reloj
        self._fecha = fecha
        self._reiniciar(reloj())

    def _reiniciar(self, inicio: float) -> None:
        self._inicio = inicio
        self._transacciones = 0
        self._bloqueadas = 0
        self._revisadas = 0
        self._suma_latencia = 0.0
        self._max_latencia = 0.0

    def registrar(self, resumen: ResumenLote) -> FilaMetricas | None:
        self._transacciones += resumen.transacciones
        self._bloqueadas += resumen.bloqueadas
        self._revisadas += resumen.revisadas
        self._suma_latencia += resumen.suma_latencia_ms
        self._max_latencia = max(self._max_latencia, resumen.max_latencia_ms)
        ahora = self._reloj()
        transcurrido = ahora - self._inicio
        if transcurrido < self._intervalo_s:
            return None
        total = max(self._transacciones, 1)
        fila = FilaMetricas(
            fecha=self._fecha(),
            ventana_segundos=round(transcurrido, 3),
            transacciones=self._transacciones,
            transacciones_por_segundo=round(self._transacciones / transcurrido, 2),
            porcentaje_bloqueadas=round(100 * self._bloqueadas / total, 3),
            porcentaje_revisadas=round(100 * self._revisadas / total, 3),
            latencia_media_ms=round(self._suma_latencia / total, 1),
            latencia_max_ms=round(self._max_latencia, 1),
        )
        self._reiniciar(ahora)
        return fila


class EscritorMetricas:
    def __init__(self, dsn: str, conectar: Callable = psycopg2.connect) -> None:
        self._dsn = dsn
        self._conectar = conectar
        self._conexion = None

    def insertar(self, fila: FilaMetricas) -> None:
        try:
            if self._conexion is None or getattr(self._conexion, "closed", False):
                self._conexion = self._conectar(self._dsn)
            with self._conexion, self._conexion.cursor() as cursor:
                cursor.execute(SQL_INSERTAR, fila.como_tupla())
        except Exception:
            logger.exception("No se pudieron guardar las métricas en PostgreSQL")
            self._conexion = None
