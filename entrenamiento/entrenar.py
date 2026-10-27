import argparse
import json
import logging
from pathlib import Path

from comun.config import cargar_config
from comun.registro import configurar_logging
from entrenamiento.historico import dividir_temporal, generar_dataset, matriz_y_etiquetas
from entrenamiento.metricas import metricas_puntuacion
from streaming.modelo.modelo import ModeloFraude

logger = logging.getLogger(__name__)


def entrenar(
    clientes: int,
    dias: float,
    tps_medio: float,
    semilla: int,
    umbral_revision: float,
    umbral_bloqueo: float,
) -> tuple[ModeloFraude, dict]:
    datos = generar_dataset(clientes, dias, tps_medio, semilla)
    entrenamiento, validacion = dividir_temporal(datos)
    x_entrenamiento, y_entrenamiento = matriz_y_etiquetas(entrenamiento)
    modelo = ModeloFraude(semilla=semilla).entrenar(x_entrenamiento, y_entrenamiento)
    x_validacion, y_validacion = matriz_y_etiquetas(validacion)
    metricas = metricas_puntuacion(
        y_validacion,
        modelo.puntuar(x_validacion),
        {"revision": umbral_revision, "bloqueo": umbral_bloqueo},
    )
    metricas["transacciones_entrenamiento"] = int(len(entrenamiento))
    metricas["transacciones_validacion"] = int(len(validacion))
    metricas["fraudes_validacion"] = int(y_validacion.sum())
    modelo.metadatos["validacion"] = metricas
    modelo.metadatos["origen"] = {
        "clientes": clientes,
        "dias": dias,
        "tps_medio": tps_medio,
        "semilla": semilla,
    }
    return modelo, metricas


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Entrena el modelo de fraude con histórico")
    parser.add_argument("--clientes", type=int, default=1000)
    parser.add_argument("--dias", type=float, default=2.0)
    parser.add_argument("--tps-medio", type=float, default=1.0)
    parser.add_argument("--semilla", type=int, default=7)
    parser.add_argument("--salida", default=config.modelo_ruta)
    parser.add_argument("--si-no-existe", action="store_true")
    args = parser.parse_args()

    if args.si_no_existe and Path(args.salida).exists():
        logger.info("Ya existe un modelo en %s; no se reentrena", args.salida)
        return

    modelo, metricas = entrenar(
        args.clientes,
        args.dias,
        args.tps_medio,
        args.semilla,
        config.umbral_revision,
        config.umbral_bloqueo,
    )
    ruta = modelo.guardar(args.salida)
    ruta.with_suffix(".metricas.json").write_text(
        json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info(
        "Modelo guardado en %s. Validación: precisión media %.3f, AUC %.3f, "
        "precisión %.3f y recall %.3f con umbral de revisión %.2f",
        ruta,
        metricas["precision_media"],
        metricas["auc_roc"],
        metricas["revision"]["precision"],
        metricas["revision"]["recall"],
        config.umbral_revision,
    )


if __name__ == "__main__":
    main()
