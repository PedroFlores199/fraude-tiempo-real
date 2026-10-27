import math
from dataclasses import dataclass
from datetime import datetime

from comun.geo import distancia_km
from comun.tiempo import desde_iso, dia_semana_local, hora_local

GASTO_REFERENCIA_SIN_PERFIL = 50.0
GASTO_DIARIO_MINIMO = 10.0
MINUTOS_SIN_HISTORIAL = 1440.0

COLUMNAS_MODELO: tuple[str, ...] = (
    "log_importe",
    "hora_seno",
    "hora_coseno",
    "es_online",
    "categoria_habitual",
    "comercio_habitual",
    "fuera_de_horario",
    "log_ratio_importe_categoria",
    "log_minutos_desde_ultima",
    "log_distancia_km_ultima",
    "log_velocidad_kmh",
    "compras_ultimo_minuto",
    "log_ratio_gasto_24h",
)


@dataclass(frozen=True)
class EstadoTarjeta:
    ultima_lat: float | None = None
    ultima_lon: float | None = None
    ultima_fecha: datetime | None = None
    compras_ultimo_minuto: int = 0
    gasto_24h: float = 0.0
    rafaga_detectada: bool = False


def _gasto_diario_esperado(categorias: dict) -> float:
    total_mes = sum(c["gasto_medio"] * c.get("compras_mes", 1) for c in categorias.values())
    return max(total_mes / 30.0, GASTO_DIARIO_MINIMO)


def calcular_features(transaccion: dict, estado: EstadoTarjeta, perfil: dict | None) -> dict:
    perfil = perfil or {}
    fecha = desde_iso(transaccion["fecha_hora"])
    importe = float(transaccion["importe"])
    categoria = transaccion["categoria"]
    categorias = perfil.get("categorias", {})
    horario = perfil.get("horario_tipico", {"inicio": 0, "fin": 24})
    hora = hora_local(fecha)

    gasto_medio = categorias.get(categoria, {}).get("gasto_medio")
    if gasto_medio:
        referencia = float(gasto_medio)
    elif categorias:
        referencia = sum(c["gasto_medio"] for c in categorias.values()) / len(categorias)
    else:
        referencia = GASTO_REFERENCIA_SIN_PERFIL

    minutos = distancia = velocidad = None
    if estado.ultima_fecha is not None and estado.ultima_lat is not None:
        minutos = abs((fecha - estado.ultima_fecha).total_seconds()) / 60.0
        distancia = distancia_km(
            estado.ultima_lat,
            estado.ultima_lon,
            float(transaccion["lat"]),
            float(transaccion["lon"]),
        )
        velocidad = distancia / (max(minutos, 1.0) / 60.0)

    gasto_24h = estado.gasto_24h + importe
    return {
        "importe": importe,
        "hora_local": hora,
        "dia_semana": dia_semana_local(fecha),
        "es_online": transaccion.get("canal") == "online",
        "categoria_habitual": categoria in categorias,
        "comercio_habitual": transaccion["comercio"] in perfil.get("comercios_frecuentes", []),
        "fuera_de_horario": not horario["inicio"] <= hora < horario["fin"],
        "gasto_medio_categoria": float(gasto_medio) if gasto_medio else None,
        "ratio_importe_categoria": importe / referencia,
        "minutos_desde_ultima": minutos,
        "distancia_km_ultima": distancia,
        "velocidad_kmh": velocidad,
        "compras_ultimo_minuto": estado.compras_ultimo_minuto + 1,
        "gasto_24h": gasto_24h,
        "ratio_gasto_24h": gasto_24h / _gasto_diario_esperado(categorias)
        if categorias
        else gasto_24h / GASTO_DIARIO_MINIMO,
        "rafaga_detectada": estado.rafaga_detectada,
    }


def vector_modelo(features: dict) -> list[float]:
    angulo = 2 * math.pi * features["hora_local"] / 24
    minutos = features["minutos_desde_ultima"]
    return [
        math.log1p(features["importe"]),
        math.sin(angulo),
        math.cos(angulo),
        float(features["es_online"]),
        float(features["categoria_habitual"]),
        float(features["comercio_habitual"]),
        float(features["fuera_de_horario"]),
        math.log1p(features["ratio_importe_categoria"]),
        math.log1p(MINUTOS_SIN_HISTORIAL if minutos is None else minutos),
        math.log1p(features["distancia_km_ultima"] or 0.0),
        math.log1p(features["velocidad_kmh"] or 0.0),
        float(features["compras_ultimo_minuto"]),
        math.log1p(features["ratio_gasto_24h"]),
    ]
