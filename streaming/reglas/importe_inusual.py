from streaming.reglas.resultado import GRAVEDAD_MEDIA, ResultadoRegla

MULTIPLO_MAXIMO = 5.0


def regla_importe_inusual(
    transaccion: dict,
    features: dict,
    perfil: dict | None,
    multiplo: float = MULTIPLO_MAXIMO,
) -> ResultadoRegla | None:
    gasto_medio = features["gasto_medio_categoria"]
    importe = features["importe"]
    if gasto_medio is None or importe <= multiplo * gasto_medio:
        return None
    return ResultadoRegla(
        regla="importe_inusual",
        motivo="importe_mayor_que_5_veces_la_media",
        gravedad=GRAVEDAD_MEDIA,
        detalle={"importe": importe, "gasto_medio_categoria": gasto_medio},
    )
