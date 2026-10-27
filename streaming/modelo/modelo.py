from datetime import UTC, datetime
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression

from comun.features import COLUMNAS_MODELO, vector_modelo


def matriz(features: list[dict]) -> np.ndarray:
    if not features:
        return np.empty((0, len(COLUMNAS_MODELO)))
    return np.asarray([vector_modelo(f) for f in features], dtype=float)


class ModeloFraude:
    def __init__(self, n_estimadores: int = 200, semilla: int = 42) -> None:
        self._bosque = IsolationForest(
            n_estimators=n_estimadores, contamination="auto", random_state=semilla, n_jobs=-1
        )
        self._calibrador = LogisticRegression()
        self.metadatos: dict = {}

    def entrenar(
        self, x: np.ndarray, y: np.ndarray, pesos: np.ndarray | None = None
    ) -> "ModeloFraude":
        y = np.asarray(y, dtype=int)
        if len(np.unique(y)) != 2:
            raise ValueError("Se necesitan ejemplos de fraude y de no fraude para calibrar")
        pesos = np.ones(len(y)) if pesos is None else np.asarray(pesos, dtype=float)
        normales = y == 0
        self._bosque.fit(x[normales], sample_weight=pesos[normales])
        self._calibrador.fit(self._anomalia(x), y, sample_weight=pesos)
        self.metadatos = {
            "columnas": list(COLUMNAS_MODELO),
            "entrenado_en": datetime.now(UTC).isoformat(),
            "ejemplos": int(len(y)),
            "fraudes": int(y.sum()),
        }
        return self

    def _anomalia(self, x: np.ndarray) -> np.ndarray:
        return -self._bosque.score_samples(x).reshape(-1, 1)

    def puntuar(self, x: np.ndarray) -> np.ndarray:
        if len(x) == 0:
            return np.empty(0)
        return self._calibrador.predict_proba(self._anomalia(x))[:, 1]

    def guardar(self, ruta: str | Path) -> Path:
        destino = Path(ruta)
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporal = destino.with_suffix(destino.suffix + ".tmp")
        joblib.dump(self, temporal)
        temporal.replace(destino)
        return destino

    @staticmethod
    def cargar(ruta: str | Path) -> "ModeloFraude":
        modelo = joblib.load(ruta)
        if not isinstance(modelo, ModeloFraude):
            raise TypeError(f"{ruta} no contiene un ModeloFraude")
        return modelo
