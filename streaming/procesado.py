from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from pymongo import UpdateOne
from pymongo.collection import Collection

from comun.alertas import ESTADO_PENDIENTE
from comun.esquema import cargar_esquema
from comun.features import calcular_features
from comun.tiempo import a_utc, ahora_utc, desde_iso
from streaming.enriquecimiento.perfiles import CachePerfiles
from streaming.modelo.predictor import Predictor
from streaming.reglas.motor import APROBADA, decidir, evaluar_reglas

CAMPOS_TRANSACCION: tuple[str, ...] = tuple(cargar_esquema()["properties"])
CAMPOS_DECIMALES = frozenset(
    campo
    for campo, definicion in cargar_esquema()["properties"].items()
    if definicion["type"] == "number"
)
COLUMNAS_DECISION = [
    "id_transaccion",
    "tarjeta_id",
    "cliente_id",
    "fecha_hora",
    "decision",
    "motivos",
    "puntuacion",
    "latencia_ms",
    "decidido_en",
]


@dataclass
class ContextoProcesado:
    estado: object
    perfiles: CachePerfiles
    predictor: Predictor
    alertas: Collection
    umbral_revision: float
    umbral_bloqueo: float
    reloj: Callable[[], datetime] = ahora_utc


def transaccion_desde_fila(fila: dict) -> dict:
    return {
        campo: float(fila[campo]) if campo in CAMPOS_DECIMALES else str(fila[campo])
        for campo in CAMPOS_TRANSACCION
    }


def _a_fecha_utc(valor) -> datetime:
    return a_utc(pd.Timestamp(valor).to_pydatetime())


def _sin_zona(valor: datetime) -> datetime:
    return a_utc(valor).replace(tzinfo=None)


def construir_alerta(
    transaccion: dict,
    fecha: datetime,
    perfil: dict | None,
    features: dict,
    resultados: list,
    decision,
    puntuacion: float,
    latencia_ms: float,
    decidido_en: datetime,
) -> dict:
    return {
        "_id": transaccion["id_transaccion"],
        "transaccion": {**transaccion, "fecha_hora": fecha},
        "cliente_id": transaccion["cliente_id"],
        "tarjeta_id": transaccion["tarjeta_id"],
        "fecha": fecha,
        "decision": decision.decision,
        "motivos": list(decision.motivos),
        "detalle_reglas": [resultado.como_dict() for resultado in resultados],
        "puntuacion_modelo": round(puntuacion, 4),
        "features": features,
        "perfil_cliente": perfil,
        "estado": ESTADO_PENDIENTE,
        "latencia_ms": round(latencia_ms, 1),
        "procesado_en": decidido_en,
    }


def procesar_lote(lote: pd.DataFrame, contexto: ContextoProcesado) -> pd.DataFrame:
    if lote.empty:
        return pd.DataFrame(columns=COLUMNAS_DECISION)

    filas = lote.to_dict("records")
    perfiles = contexto.perfiles.obtener_varios({str(fila["cliente_id"]) for fila in filas})
    evaluaciones = []
    for fila in filas:
        transaccion = transaccion_desde_fila(fila)
        fecha = desde_iso(transaccion["fecha_hora"])
        estado = contexto.estado.leer(
            transaccion["tarjeta_id"], fecha, transaccion["id_transaccion"]
        )
        perfil = perfiles.get(transaccion["cliente_id"])
        features = calcular_features(transaccion, estado, perfil)
        resultados = evaluar_reglas(transaccion, features, perfil)
        contexto.estado.registrar(transaccion)
        evaluaciones.append(
            (transaccion, fecha, perfil, features, resultados, _a_fecha_utc(fila["kafka_ts"]))
        )

    puntuaciones = contexto.predictor.puntuar([evaluacion[3] for evaluacion in evaluaciones])
    decidido_en = contexto.reloj()
    salida = []
    operaciones = []
    for (transaccion, fecha, perfil, features, resultados, recibido), puntuacion in zip(
        evaluaciones, puntuaciones, strict=True
    ):
        decision = decidir(
            resultados, puntuacion, contexto.umbral_revision, contexto.umbral_bloqueo
        )
        latencia_ms = max(0.0, (decidido_en - recibido).total_seconds() * 1000)
        if decision.decision != APROBADA:
            alerta = construir_alerta(
                transaccion,
                fecha,
                perfil,
                features,
                resultados,
                decision,
                puntuacion,
                latencia_ms,
                decidido_en,
            )
            operaciones.append(
                UpdateOne({"_id": alerta["_id"]}, {"$setOnInsert": alerta}, upsert=True)
            )
        salida.append(
            {
                "id_transaccion": transaccion["id_transaccion"],
                "tarjeta_id": transaccion["tarjeta_id"],
                "cliente_id": transaccion["cliente_id"],
                "fecha_hora": _sin_zona(fecha),
                "decision": decision.decision,
                "motivos": list(decision.motivos),
                "puntuacion": float(puntuacion),
                "latencia_ms": float(latencia_ms),
                "decidido_en": _sin_zona(decidido_en),
            }
        )
    if operaciones:
        contexto.alertas.bulk_write(operaciones, ordered=False)
    return pd.DataFrame(salida, columns=COLUMNAS_DECISION)
