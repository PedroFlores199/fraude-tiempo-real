from datetime import datetime

from pymongo.collection import Collection

from comun.alertas import ESTADOS_REVISION, FALSO_POSITIVO


def _filtro_fechas(desde: datetime | None, hasta: datetime | None) -> dict:
    rango = {}
    if desde is not None:
        rango["$gte"] = desde
    if hasta is not None:
        rango["$lte"] = hasta
    return {"fecha": rango} if rango else {}


def pipeline_alertas_por_motivo_y_dia(
    desde: datetime | None = None, hasta: datetime | None = None
) -> list[dict]:
    return [
        {"$match": _filtro_fechas(desde, hasta)},
        {"$unwind": "$motivos"},
        {
            "$group": {
                "_id": {
                    "dia": {"$dateToString": {"format": "%Y-%m-%d", "date": "$fecha"}},
                    "motivo": "$motivos",
                },
                "alertas": {"$sum": 1},
                "bloqueadas": {"$sum": {"$cond": [{"$eq": ["$decision", "BLOQUEADA"]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "dia": "$_id.dia",
                "motivo": "$_id.motivo",
                "alertas": 1,
                "bloqueadas": 1,
            }
        },
        {"$sort": {"dia": -1, "alertas": -1}},
    ]


def pipeline_falsos_positivos_por_regla() -> list[dict]:
    return [
        {"$match": {"estado": {"$in": list(ESTADOS_REVISION)}}},
        {"$unwind": "$motivos"},
        {
            "$group": {
                "_id": "$motivos",
                "revisadas": {"$sum": 1},
                "falsos_positivos": {
                    "$sum": {"$cond": [{"$eq": ["$estado", FALSO_POSITIVO]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "motivo": "$_id",
                "revisadas": 1,
                "falsos_positivos": 1,
                "porcentaje_falsos_positivos": {
                    "$multiply": [{"$divide": ["$falsos_positivos", "$revisadas"]}, 100]
                },
            }
        },
        {"$sort": {"porcentaje_falsos_positivos": -1}},
    ]


def pipeline_precision_por_motivo() -> list[dict]:
    return [
        {
            "$lookup": {
                "from": "etiquetas",
                "localField": "_id",
                "foreignField": "_id",
                "as": "etiqueta",
            }
        },
        {"$unwind": {"path": "$etiqueta", "preserveNullAndEmptyArrays": True}},
        {"$unwind": "$motivos"},
        {
            "$group": {
                "_id": "$motivos",
                "alertas": {"$sum": 1},
                "fraude_real": {"$sum": {"$cond": [{"$ifNull": ["$etiqueta", False]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "motivo": "$_id",
                "alertas": 1,
                "fraude_real": 1,
                "precision": {"$divide": ["$fraude_real", "$alertas"]},
            }
        },
        {"$sort": {"alertas": -1}},
    ]


def alertas_por_motivo_y_dia(
    alertas: Collection, desde: datetime | None = None, hasta: datetime | None = None
) -> list[dict]:
    return list(alertas.aggregate(pipeline_alertas_por_motivo_y_dia(desde, hasta)))


def _redondear(filas: list[dict], campo: str, decimales: int) -> list[dict]:
    return [{**fila, campo: round(fila[campo], decimales)} for fila in filas]


def falsos_positivos_por_regla(alertas: Collection) -> list[dict]:
    filas = list(alertas.aggregate(pipeline_falsos_positivos_por_regla()))
    return _redondear(filas, "porcentaje_falsos_positivos", 2)


def precision_por_motivo(alertas: Collection) -> list[dict]:
    return _redondear(list(alertas.aggregate(pipeline_precision_por_motivo())), "precision", 4)
