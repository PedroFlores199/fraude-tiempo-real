import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score


def matriz_confusion(y: np.ndarray, prediccion: np.ndarray) -> dict:
    y = np.asarray(y, dtype=bool)
    prediccion = np.asarray(prediccion, dtype=bool)
    verdaderos = int(np.sum(prediccion & y))
    falsos = int(np.sum(prediccion & ~y))
    perdidos = int(np.sum(~prediccion & y))
    return {
        "alertas": int(prediccion.sum()),
        "verdaderos_positivos": verdaderos,
        "falsos_positivos": falsos,
        "fraudes_no_detectados": perdidos,
        "precision": verdaderos / (verdaderos + falsos) if verdaderos + falsos else 0.0,
        "recall": verdaderos / (verdaderos + perdidos) if verdaderos + perdidos else 0.0,
    }


def metricas_puntuacion(
    y: np.ndarray, puntuaciones: np.ndarray, umbrales: dict[str, float]
) -> dict:
    resultado = {
        "precision_media": float(average_precision_score(y, puntuaciones)),
        "auc_roc": float(roc_auc_score(y, puntuaciones)),
    }
    for nombre, umbral in umbrales.items():
        resultado[nombre] = {"umbral": umbral, **matriz_confusion(y, puntuaciones >= umbral)}
    return resultado
