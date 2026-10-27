from streaming.reglas.resultado import GRAVEDAD_ALTA, ResultadoRegla

DISTANCIA_MINIMA_KM = 300.0
MINUTOS_MAXIMOS = 30.0


def regla_viaje_imposible(
    transaccion: dict,
    features: dict,
    perfil: dict | None,
    distancia_minima_km: float = DISTANCIA_MINIMA_KM,
    minutos_maximos: float = MINUTOS_MAXIMOS,
) -> ResultadoRegla | None:
    distancia = features["distancia_km_ultima"]
    minutos = features["minutos_desde_ultima"]
    if distancia is None or minutos is None:
        return None
    if distancia <= distancia_minima_km or minutos >= minutos_maximos:
        return None
    return ResultadoRegla(
        regla="viaje_imposible",
        motivo="mas_de_300_km_en_menos_de_30_min",
        gravedad=GRAVEDAD_ALTA,
        detalle={"distancia_km": round(distancia, 1), "minutos": round(minutos, 1)},
    )
