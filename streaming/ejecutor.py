from collections.abc import Iterator
from datetime import UTC, datetime
from functools import lru_cache

import pandas as pd
import redis
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from comun.config import cargar_config
from comun.tiempo import a_iso, ahora_utc
from streaming.enriquecimiento.estado_tarjetas import EstadoTarjetasRedis
from streaming.enriquecimiento.perfiles import CachePerfiles
from streaming.modelo.predictor import Predictor
from streaming.procesado import ContextoProcesado, procesar_lote


@lru_cache(maxsize=1)
def _base_mongo() -> Database:
    config = cargar_config()
    return MongoClient(config.mongo_uri, tz_aware=True)[config.mongo_db]


@lru_cache(maxsize=1)
def _estado_tarjetas() -> EstadoTarjetasRedis:
    config = cargar_config()
    return EstadoTarjetasRedis(
        redis.Redis(host=config.redis_host, port=config.redis_puerto),
        ttl_historial_s=config.ttl_historial_s,
        ttl_rafaga_s=config.ttl_rafaga_s,
    )


@lru_cache(maxsize=1)
def obtener_contexto() -> ContextoProcesado:
    config = cargar_config()
    base = _base_mongo()
    perfiles = CachePerfiles(base["clientes"], ttl_s=config.perfil_cache_ttl_s)
    perfiles.precargar()
    return ContextoProcesado(
        estado=_estado_tarjetas(),
        perfiles=perfiles,
        predictor=Predictor(config.modelo_ruta),
        alertas=base["alertas"],
        umbral_revision=config.umbral_revision,
        umbral_bloqueo=config.umbral_bloqueo,
    )


def evaluar_particiones(lotes: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    contexto = obtener_contexto()
    for lote in lotes:
        yield procesar_lote(lote, contexto)


def _fecha_de_fila(valor: datetime) -> datetime:
    return valor.astimezone(UTC)


def documento_rafaga(fila: dict) -> dict:
    inicio = _fecha_de_fila(fila["ventana_inicio"])
    return {
        "_id": f"{fila['tarjeta_id']}|{a_iso(inicio)}",
        "tarjeta_id": fila["tarjeta_id"],
        "cliente_id": fila["cliente_id"],
        "ventana_inicio": inicio,
        "ventana_fin": _fecha_de_fila(fila["ventana_fin"]),
        "compras": int(fila["compras"]),
        "importe_total": float(fila["importe_total"]),
        "ids_transaccion": sorted(fila["ids_transaccion"]),
        "actualizado_en": ahora_utc(),
    }


def escribir_rafagas_particion(filas) -> None:
    documentos = [documento_rafaga(fila.asDict()) for fila in filas]
    if not documentos:
        return
    _base_mongo()["rafagas"].bulk_write(
        [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in documentos],
        ordered=False,
    )
    estado = _estado_tarjetas()
    for tarjeta_id in {d["tarjeta_id"] for d in documentos}:
        estado.marcar_rafaga(tarjeta_id)
