import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from comun.config import cargar_config
from comun.registro import configurar_logging
from entrenamiento.historico import generar_dataset, matriz_y_etiquetas
from entrenamiento.metricas import matriz_confusion
from streaming.modelo.modelo import ModeloFraude
from streaming.reglas.motor import APROBADA, decidir, evaluar_reglas

logger = logging.getLogger(__name__)

RUTA_INFORME = Path("docs/evaluacion.md")


def decisiones(
    datos: pd.DataFrame, puntuaciones: np.ndarray, umbral_revision: float, umbral_bloqueo: float
) -> pd.DataFrame:
    filas = []
    for fila, puntuacion in zip(datos.itertuples(index=False), puntuaciones, strict=True):
        resultados = evaluar_reglas(fila.transaccion, fila.features, fila.perfil)
        solo_reglas = decidir(resultados, 0.0, umbral_revision, umbral_bloqueo).decision
        combinada = decidir(resultados, puntuacion, umbral_revision, umbral_bloqueo).decision
        filas.append(
            {
                "patron": fila.patron,
                "es_fraude": bool(fila.es_fraude),
                "solo_reglas": solo_reglas != APROBADA,
                "solo_modelo": puntuacion >= umbral_revision,
                "reglas_y_modelo": combinada != APROBADA,
            }
        )
    return pd.DataFrame(filas)


def comparar(resultado: pd.DataFrame) -> pd.DataFrame:
    filas = []
    for estrategia in ("solo_reglas", "solo_modelo", "reglas_y_modelo"):
        metricas = matriz_confusion(resultado["es_fraude"], resultado[estrategia])
        filas.append({"estrategia": estrategia, **metricas})
    return pd.DataFrame(filas)


def recall_por_patron(resultado: pd.DataFrame) -> pd.DataFrame:
    fraudes = resultado[resultado["es_fraude"]]
    return (
        fraudes.groupby("patron")[["solo_reglas", "solo_modelo", "reglas_y_modelo"]]
        .mean()
        .round(3)
        .assign(fraudes=fraudes.groupby("patron").size())
        .reset_index()
    )


def _tabla_markdown(tabla: pd.DataFrame) -> str:
    columnas = list(tabla.columns)
    lineas = ["| " + " | ".join(columnas) + " |", "|" + "---|" * len(columnas)]
    for fila in tabla.itertuples(index=False):
        celdas = [f"{v:.3f}" if isinstance(v, float) else str(v) for v in fila]
        lineas.append("| " + " | ".join(celdas) + " |")
    return "\n".join(lineas)


def informe(
    comparativa: pd.DataFrame,
    por_patron: pd.DataFrame,
    transacciones: int,
    fraudes: int,
    parametros: dict,
) -> str:
    descripcion = ", ".join(f"{clave}={valor}" for clave, valor in parametros.items())
    secciones = [
        "# Evaluación de reglas y modelo",
        "Generado con `python -m entrenamiento.evaluar` sobre un histórico sintético "
        f"independiente del de entrenamiento: {transacciones} transacciones, {fraudes} "
        f"fraudulentas. Parámetros: {descripcion}.",
        "## Comparativa",
        _tabla_markdown(comparativa),
        "## Recall por patrón de fraude",
        _tabla_markdown(por_patron),
    ]
    return "\n\n".join(secciones) + "\n"


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Compara reglas, modelo y combinación")
    parser.add_argument("--clientes", type=int, default=1000)
    parser.add_argument("--dias", type=float, default=1.0)
    parser.add_argument("--tps-medio", type=float, default=1.0)
    parser.add_argument("--semilla", type=int, default=2026)
    parser.add_argument("--modelo", default=config.modelo_ruta)
    parser.add_argument("--salida", default=str(RUTA_INFORME))
    args = parser.parse_args()

    modelo = ModeloFraude.cargar(args.modelo)
    datos = generar_dataset(args.clientes, args.dias, args.tps_medio, args.semilla)
    x, y = matriz_y_etiquetas(datos)
    resultado = decisiones(datos, modelo.puntuar(x), config.umbral_revision, config.umbral_bloqueo)
    comparativa = comparar(resultado)
    por_patron = recall_por_patron(resultado)
    parametros = {
        "clientes": args.clientes,
        "dias": args.dias,
        "semilla": args.semilla,
        "umbral_revision": config.umbral_revision,
        "umbral_bloqueo": config.umbral_bloqueo,
    }
    salida = Path(args.salida)
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(
        informe(comparativa, por_patron, len(datos), int(y.sum()), parametros), encoding="utf-8"
    )
    logger.info("Comparativa:\n%s", comparativa.to_string(index=False))
    logger.info("Recall por patrón:\n%s", por_patron.to_string(index=False))
    logger.info("Informe escrito en %s", salida)


if __name__ == "__main__":
    main()
