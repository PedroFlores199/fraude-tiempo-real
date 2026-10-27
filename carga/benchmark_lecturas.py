import argparse
import json
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import redis
from pymongo import MongoClient

from carga.estadisticas import percentiles, tabla_markdown
from comun.config import cargar_config
from comun.registro import configurar_logging
from comun.tiempo import a_iso, ahora_utc
from streaming.enriquecimiento.estado_tarjetas import EstadoTarjetasRedis

logger = logging.getLogger(__name__)

COLECCION_BENCHMARK = "benchmark_estado_tarjetas"
PREFIJO_TARJETA = "TAR-bench"


def transacciones_sinteticas(tarjeta_id: str, ahora: datetime, cantidad: int) -> list[dict]:
    return [
        {
            "id_transaccion": f"{tarjeta_id}-{i}",
            "tarjeta_id": tarjeta_id,
            "importe": round(random.uniform(5, 200), 2),
            "ciudad": "Madrid",
            "lat": 40.4168,
            "lon": -3.7038,
            "fecha_hora": a_iso(ahora - timedelta(minutes=10 * (cantidad - i))),
        }
        for i in range(cantidad)
    ]


def documento_mongo(tarjeta_id: str, transacciones: list[dict]) -> dict:
    ultima = transacciones[-1]
    return {
        "_id": tarjeta_id,
        "ultima_ubicacion": {
            "lat": ultima["lat"],
            "lon": ultima["lon"],
            "fecha": ultima["fecha_hora"],
        },
        "compras": [
            {"id": t["id_transaccion"], "importe": t["importe"], "fecha": t["fecha_hora"]}
            for t in transacciones
        ],
        "rafaga": False,
    }


def medir(funcion, claves: list[str], repeticiones: int) -> list[float]:
    latencias = []
    for _ in range(repeticiones):
        clave = random.choice(claves)
        inicio = time.perf_counter()
        funcion(clave)
        latencias.append((time.perf_counter() - inicio) * 1000)
    return latencias


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Compara lecturas de variables en Redis y MongoDB")
    parser.add_argument("--tarjetas", type=int, default=10_000)
    parser.add_argument("--compras-por-tarjeta", type=int, default=20)
    parser.add_argument("--lecturas", type=int, default=20_000)
    parser.add_argument("--salida", default="carga/resultados")
    args = parser.parse_args()

    ahora = ahora_utc()
    cliente_redis = redis.Redis(host=config.redis_host, port=config.redis_puerto)
    estado = EstadoTarjetasRedis(cliente_redis, config.ttl_historial_s, config.ttl_rafaga_s)
    with MongoClient(config.mongo_uri, tz_aware=True) as cliente_mongo:
        coleccion = cliente_mongo[config.mongo_db][COLECCION_BENCHMARK]
        coleccion.drop()
        claves = [f"{PREFIJO_TARJETA}{i:08x}" for i in range(args.tarjetas)]
        documentos = []
        for tarjeta_id in claves:
            transacciones = transacciones_sinteticas(tarjeta_id, ahora, args.compras_por_tarjeta)
            for transaccion in transacciones:
                estado.registrar(transaccion)
            documentos.append(documento_mongo(tarjeta_id, transacciones))
        coleccion.insert_many(documentos)

        medir(lambda c: estado.leer(c, ahora, "nueva"), claves, 1000)
        medir(lambda c: coleccion.find_one({"_id": c}), claves, 1000)
        resultados = {
            "Redis (3 comandos en pipeline)": percentiles(
                medir(lambda c: estado.leer(c, ahora, "nueva"), claves, args.lecturas)
            ),
            "MongoDB (find_one por _id)": percentiles(
                medir(lambda c: coleccion.find_one({"_id": c}), claves, args.lecturas)
            ),
        }
        coleccion.drop()
    for clave in cliente_redis.scan_iter(f"tarjeta:{PREFIJO_TARJETA}*"):
        cliente_redis.delete(clave)

    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)
    fichero = salida / f"benchmark_lecturas_{datetime.now():%Y%m%d_%H%M%S}.json"
    fichero.write_text(json.dumps(resultados, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Lecturas de variables de una tarjeta:\n%s", tabla_markdown(resultados))
    logger.info("Resultados guardados en %s", fichero)


if __name__ == "__main__":
    main()
