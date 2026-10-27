import numpy as np


def percentiles(valores_ms: list[float]) -> dict[str, float]:
    if not valores_ms:
        return {"media": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    datos = np.asarray(valores_ms, dtype=float)
    return {
        "media": round(float(datos.mean()), 2),
        "p50": round(float(np.percentile(datos, 50)), 2),
        "p95": round(float(np.percentile(datos, 95)), 2),
        "p99": round(float(np.percentile(datos, 99)), 2),
        "max": round(float(datos.max()), 2),
    }


def tabla_markdown(filas: dict[str, dict[str, float]]) -> str:
    columnas = ["media", "p50", "p95", "p99", "max"]
    lineas = ["| | " + " | ".join(f"{c} (ms)" for c in columnas) + " |"]
    lineas.append("|---|" + "---|" * len(columnas))
    for nombre, valores in filas.items():
        lineas.append(f"| {nombre} | " + " | ".join(f"{valores[c]:.2f}" for c in columnas) + " |")
    return "\n".join(lineas)
