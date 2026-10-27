import logging
import threading
from collections import OrderedDict
from datetime import datetime

from pymongo import DESCENDING
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from comun.tiempo import ahora_utc

logger = logging.getLogger(__name__)

PIPELINE_CAMBIOS = [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]
CAMPOS_OMITIDOS = ("perfil_cliente", "features")


class OyenteAlertas:
    def __init__(
        self, coleccion: Collection, capacidad: int = 500, espera_reintento_s: float = 2.0
    ) -> None:
        self._coleccion = coleccion
        self._capacidad = capacidad
        self._espera_reintento_s = espera_reintento_s
        self._alertas: OrderedDict[str, dict] = OrderedDict()
        self._candado = threading.Lock()
        self._parar = threading.Event()
        self._hilo: threading.Thread | None = None
        self._token = None
        self.recibidas = 0
        self.ultimo_evento: datetime | None = None

    def cargar_recientes(self) -> int:
        proyeccion = {campo: 0 for campo in CAMPOS_OMITIDOS}
        documentos = list(
            self._coleccion.find({}, proyeccion).sort("fecha", DESCENDING).limit(self._capacidad)
        )
        with self._candado:
            for documento in reversed(documentos):
                self._guardar(documento)
        return len(documentos)

    def aplicar(self, cambio: dict) -> None:
        self._token = cambio.get("_id", self._token)
        documento = cambio.get("fullDocument")
        if documento is None:
            return
        limpio = {k: v for k, v in documento.items() if k not in CAMPOS_OMITIDOS}
        with self._candado:
            self._guardar(limpio)
            self.recibidas += 1
            self.ultimo_evento = ahora_utc()

    def _guardar(self, documento: dict) -> None:
        self._alertas[documento["_id"]] = documento
        self._alertas.move_to_end(documento["_id"])
        while len(self._alertas) > self._capacidad:
            self._alertas.popitem(last=False)

    def instantanea(self) -> list[dict]:
        with self._candado:
            alertas = list(self._alertas.values())
        return sorted(alertas, key=lambda alerta: alerta["fecha"], reverse=True)

    def _escuchar(self) -> None:
        while not self._parar.is_set():
            try:
                with self._coleccion.watch(
                    PIPELINE_CAMBIOS,
                    full_document="updateLookup",
                    resume_after=self._token,
                    max_await_time_ms=1000,
                ) as flujo:
                    while not self._parar.is_set() and flujo.alive:
                        cambio = flujo.try_next()
                        if cambio is not None:
                            self.aplicar(cambio)
            except PyMongoError:
                logger.exception("Change stream interrumpido; se reintenta la conexión")
                self._parar.wait(self._espera_reintento_s)

    def iniciar(self) -> None:
        if self._hilo is not None and self._hilo.is_alive():
            return
        self._parar.clear()
        self._hilo = threading.Thread(target=self._escuchar, name="oyente-alertas", daemon=True)
        self._hilo.start()

    def detener(self) -> None:
        self._parar.set()
        if self._hilo is not None:
            self._hilo.join(timeout=5)
