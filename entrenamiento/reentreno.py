import hashlib
import json
import logging
import shutil
from pathlib import Path

import numpy as np
from pymongo.collection import Collection
from sklearn.metrics import average_precision_score

from comun.alertas import ESTADOS_REVISION, FRAUDE_CONFIRMADO
from entrenamiento.historico import dividir_temporal, generar_dataset, matriz_y_etiquetas
from streaming.modelo.modelo import ModeloFraude, matriz

logger = logging.getLogger(__name__)

PESO_FEEDBACK = 5.0
FRACCION_VALIDACION_FEEDBACK = 0.3


def exportar_feedback(alertas: Collection, destino: Path) -> int:
    consulta = {"estado": {"$in": list(ESTADOS_REVISION)}, "features": {"$exists": True}}
    proyeccion = {"features": 1, "estado": 1}
    destino.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with destino.open("w", encoding="utf-8") as fichero:
        for documento in alertas.find(consulta, proyeccion):
            registro = {
                "id_transaccion": documento["_id"],
                "es_fraude": int(documento["estado"] == FRAUDE_CONFIRMADO),
                "features": documento["features"],
            }
            fichero.write(json.dumps(registro, ensure_ascii=False) + "\n")
            total += 1
    return total


def cargar_feedback(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    with ruta.open(encoding="utf-8") as fichero:
        return [json.loads(linea) for linea in fichero if linea.strip()]


def es_de_validacion(id_transaccion: str) -> bool:
    resumen = hashlib.sha256(id_transaccion.encode("utf-8")).digest()
    return resumen[0] / 255 < FRACCION_VALIDACION_FEEDBACK


def _particion_feedback(feedback: list[dict]) -> tuple[list[dict], list[dict]]:
    validacion = [r for r in feedback if es_de_validacion(r["id_transaccion"])]
    entrenamiento = [r for r in feedback if not es_de_validacion(r["id_transaccion"])]
    return entrenamiento, validacion


def _con_feedback(x: np.ndarray, y: np.ndarray, registros: list[dict], peso: float):
    if not registros:
        return x, y, np.ones(len(y))
    x_feedback = matriz([r["features"] for r in registros])
    y_feedback = np.asarray([r["es_fraude"] for r in registros], dtype=int)
    return (
        np.vstack([x, x_feedback]),
        np.concatenate([y, y_feedback]),
        np.concatenate([np.ones(len(y)), np.full(len(y_feedback), peso)]),
    )


def preparar_conjuntos(
    feedback: list[dict], clientes: int, dias: float, tps_medio: float, semilla: int
) -> dict:
    historico = generar_dataset(clientes, dias, tps_medio, semilla)
    entrenamiento, validacion = dividir_temporal(historico)
    feedback_entrenamiento, feedback_validacion = _particion_feedback(feedback)
    x_entrenamiento, y_entrenamiento = matriz_y_etiquetas(entrenamiento)
    x_validacion, y_validacion = matriz_y_etiquetas(validacion)
    return {
        "entrenamiento": _con_feedback(
            x_entrenamiento, y_entrenamiento, feedback_entrenamiento, PESO_FEEDBACK
        ),
        "validacion": _con_feedback(x_validacion, y_validacion, feedback_validacion, PESO_FEEDBACK),
    }


def entrenar_candidato(conjuntos: dict, destino: Path, semilla: int) -> Path:
    x, y, pesos = conjuntos["entrenamiento"]
    modelo = ModeloFraude(semilla=semilla).entrenar(x, y, pesos)
    return modelo.guardar(destino)


def puntuacion_validacion(ruta_modelo: Path, conjuntos: dict) -> float | None:
    if not ruta_modelo.exists():
        return None
    x, y, pesos = conjuntos["validacion"]
    modelo = ModeloFraude.cargar(ruta_modelo)
    return float(average_precision_score(y, modelo.puntuar(x), sample_weight=pesos))


def comparar(ruta_candidato: Path, ruta_produccion: Path, conjuntos: dict) -> dict:
    candidato = puntuacion_validacion(ruta_candidato, conjuntos)
    produccion = puntuacion_validacion(ruta_produccion, conjuntos)
    mejora = produccion is None or (candidato is not None and candidato > produccion)
    return {"candidato": candidato, "produccion": produccion, "desplegar": mejora}


def desplegar(ruta_candidato: Path, ruta_produccion: Path) -> Path:
    ruta_produccion.parent.mkdir(parents=True, exist_ok=True)
    temporal = ruta_produccion.with_suffix(ruta_produccion.suffix + ".nuevo")
    shutil.copyfile(ruta_candidato, temporal)
    temporal.replace(ruta_produccion)
    logger.info("Modelo %s desplegado en %s", ruta_candidato, ruta_produccion)
    return ruta_produccion
