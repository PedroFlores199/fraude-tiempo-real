from datetime import datetime

from pymongo.collection import Collection

from comun.tiempo import ahora_utc

ESTADO_PENDIENTE = "PENDIENTE"
FRAUDE_CONFIRMADO = "FRAUDE_CONFIRMADO"
FALSO_POSITIVO = "FALSO_POSITIVO"
ESTADOS_REVISION = (FRAUDE_CONFIRMADO, FALSO_POSITIVO)
ESTADOS = (ESTADO_PENDIENTE, *ESTADOS_REVISION)


def marcar_alerta(
    coleccion: Collection, alerta_id: str, estado: str, ahora: datetime | None = None
) -> bool:
    if estado not in ESTADOS_REVISION:
        raise ValueError(f"Estado no válido: {estado}. Permitidos: {', '.join(ESTADOS_REVISION)}")
    ahora = ahora or ahora_utc()
    cambios: dict = {"$set": {"estado": estado, "revisado_en": ahora}}
    if estado == FALSO_POSITIVO:
        cambios["$set"]["descartada_en"] = ahora
    else:
        cambios["$unset"] = {"descartada_en": ""}
    return coleccion.update_one({"_id": alerta_id}, cambios).matched_count > 0
