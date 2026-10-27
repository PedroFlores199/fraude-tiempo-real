from streaming.reglas.resultado import GRAVEDAD_ALTA, ResultadoRegla

MAXIMO_COMPRAS_POR_MINUTO = 5


def regla_frecuencia(
    transaccion: dict,
    features: dict,
    perfil: dict | None,
    maximo: int = MAXIMO_COMPRAS_POR_MINUTO,
) -> ResultadoRegla | None:
    compras = features["compras_ultimo_minuto"]
    if compras <= maximo and not features["rafaga_detectada"]:
        return None
    return ResultadoRegla(
        regla="frecuencia",
        motivo="mas_de_5_compras_en_1_minuto",
        gravedad=GRAVEDAD_ALTA,
        detalle={
            "compras_ultimo_minuto": compras,
            "rafaga_en_ventana": features["rafaga_detectada"],
        },
    )
