import time
from collections.abc import Callable, Iterable

from pymongo.collection import Collection

PROYECCION = {"fecha_alta": 0}


class CachePerfiles:
    def __init__(
        self,
        coleccion: Collection,
        ttl_s: float,
        reloj: Callable[[], float] = time.monotonic,
    ) -> None:
        self._coleccion = coleccion
        self._ttl_s = ttl_s
        self._reloj = reloj
        self._perfiles: dict[str, tuple[dict | None, float]] = {}

    def __len__(self) -> int:
        return len(self._perfiles)

    def precargar(self) -> int:
        ahora = self._reloj()
        for documento in self._coleccion.find({}, PROYECCION):
            self._perfiles[documento["_id"]] = (documento, ahora)
        return len(self._perfiles)

    def obtener_varios(self, ids: Iterable[str]) -> dict[str, dict]:
        ahora = self._reloj()
        ids = set(ids)
        caducados = [
            cliente_id
            for cliente_id in ids
            if cliente_id not in self._perfiles
            or ahora - self._perfiles[cliente_id][1] >= self._ttl_s
        ]
        if caducados:
            encontrados = {
                documento["_id"]: documento
                for documento in self._coleccion.find({"_id": {"$in": caducados}}, PROYECCION)
            }
            for cliente_id in caducados:
                self._perfiles[cliente_id] = (encontrados.get(cliente_id), ahora)
        return {
            cliente_id: self._perfiles[cliente_id][0]
            for cliente_id in ids
            if self._perfiles[cliente_id][0] is not None
        }

    def obtener(self, cliente_id: str) -> dict | None:
        return self.obtener_varios([cliente_id]).get(cliente_id)
