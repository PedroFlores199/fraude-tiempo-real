from collections.abc import Callable
from dataclasses import dataclass

from streaming.reglas.comportamiento import regla_comportamiento
from streaming.reglas.frecuencia import regla_frecuencia
from streaming.reglas.importe_inusual import regla_importe_inusual
from streaming.reglas.resultado import GRAVEDAD_ALTA, GRAVEDAD_MEDIA, ResultadoRegla
from streaming.reglas.viaje_imposible import regla_viaje_imposible

APROBADA = "APROBADA"
REVISAR = "REVISAR"
BLOQUEADA = "BLOQUEADA"
MOTIVO_MODELO = "puntuacion_alta_del_modelo"

Regla = Callable[[dict, dict, dict | None], ResultadoRegla | None]

REGLAS: tuple[Regla, ...] = (
    regla_frecuencia,
    regla_viaje_imposible,
    regla_importe_inusual,
    regla_comportamiento,
)


@dataclass(frozen=True)
class Decision:
    decision: str
    motivos: tuple[str, ...]


def evaluar_reglas(
    transaccion: dict,
    features: dict,
    perfil: dict | None,
    reglas: tuple[Regla, ...] = REGLAS,
) -> list[ResultadoRegla]:
    resultados = (regla(transaccion, features, perfil) for regla in reglas)
    return [resultado for resultado in resultados if resultado is not None]


def decidir(
    resultados: list[ResultadoRegla],
    puntuacion: float,
    umbral_revision: float,
    umbral_bloqueo: float,
) -> Decision:
    motivos = [resultado.motivo for resultado in resultados]
    if puntuacion >= umbral_revision:
        motivos.append(MOTIVO_MODELO)
    altas = sum(1 for r in resultados if r.gravedad == GRAVEDAD_ALTA)
    medias = sum(1 for r in resultados if r.gravedad == GRAVEDAD_MEDIA)
    modelo_revisa = puntuacion >= umbral_revision

    if altas or medias >= 2 or puntuacion >= umbral_bloqueo or (medias and modelo_revisa):
        decision = BLOQUEADA
    elif medias or modelo_revisa:
        decision = REVISAR
    else:
        decision = APROBADA
    return Decision(decision, tuple(motivos))
