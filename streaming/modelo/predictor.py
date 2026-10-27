import logging
import time
from collections.abc import Callable
from pathlib import Path

from streaming.modelo.modelo import ModeloFraude, matriz

logger = logging.getLogger(__name__)


class Predictor:
    def __init__(
        self,
        ruta: str | Path,
        intervalo_comprobacion_s: float = 30.0,
        reloj: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ruta = Path(ruta)
        self._intervalo = intervalo_comprobacion_s
        self._reloj = reloj
        self._modelo: ModeloFraude | None = None
        self._version: float | None = None
        self._ultima_comprobacion: float | None = None
        self._aviso_emitido = False

    @property
    def version(self) -> float | None:
        return self._version

    @property
    def disponible(self) -> bool:
        self._recargar_si_cambia()
        return self._modelo is not None

    def _recargar_si_cambia(self) -> None:
        ahora = self._reloj()
        if (
            self._ultima_comprobacion is not None
            and ahora - self._ultima_comprobacion < self._intervalo
        ):
            return
        self._ultima_comprobacion = ahora
        if not self._ruta.exists():
            if not self._aviso_emitido:
                logger.warning("No hay modelo en %s; solo se aplicarán las reglas", self._ruta)
                self._aviso_emitido = True
            return
        version = self._ruta.stat().st_mtime
        if version == self._version:
            return
        self._modelo = ModeloFraude.cargar(self._ruta)
        self._version = version
        logger.info("Modelo cargado desde %s (%s)", self._ruta, self._modelo.metadatos)

    def puntuar(self, features: list[dict]) -> list[float]:
        self._recargar_si_cambia()
        if self._modelo is None:
            return [0.0] * len(features)
        return [float(p) for p in self._modelo.puntuar(matriz(features))]
