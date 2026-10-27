from streaming.reglas.resultado import GRAVEDAD_MEDIA, ResultadoRegla


def regla_comportamiento(
    transaccion: dict, features: dict, perfil: dict | None
) -> ResultadoRegla | None:
    if perfil is None:
        return None
    if features["comercio_habitual"] or features["categoria_habitual"]:
        return None
    if not features["fuera_de_horario"]:
        return None
    return ResultadoRegla(
        regla="comportamiento",
        motivo="comercio_y_categoria_no_habituales_fuera_de_horario",
        gravedad=GRAVEDAD_MEDIA,
        detalle={
            "comercio": transaccion["comercio"],
            "categoria": transaccion["categoria"],
            "hora_local": features["hora_local"],
        },
    )
