from datetime import UTC, datetime, timedelta

import pytest

from comun.features import EstadoTarjeta, calcular_features
from streaming.reglas.comportamiento import regla_comportamiento
from streaming.reglas.frecuencia import regla_frecuencia
from streaming.reglas.importe_inusual import regla_importe_inusual
from streaming.reglas.motor import (
    APROBADA,
    BLOQUEADA,
    MOTIVO_MODELO,
    REVISAR,
    decidir,
    evaluar_reglas,
)
from streaming.reglas.resultado import GRAVEDAD_ALTA, GRAVEDAD_MEDIA, ResultadoRegla
from streaming.reglas.viaje_imposible import regla_viaje_imposible

BASE = {
    "importe": 40.0,
    "hora_local": 11,
    "es_online": False,
    "categoria_habitual": True,
    "comercio_habitual": True,
    "fuera_de_horario": False,
    "gasto_medio_categoria": 40.0,
    "ratio_importe_categoria": 1.0,
    "minutos_desde_ultima": 120.0,
    "distancia_km_ultima": 2.0,
    "velocidad_kmh": 1.0,
    "compras_ultimo_minuto": 1,
    "gasto_24h": 40.0,
    "ratio_gasto_24h": 1.0,
    "rafaga_detectada": False,
}


def features(**cambios) -> dict:
    return {**BASE, **cambios}


@pytest.mark.parametrize(
    ("cambios", "dispara"),
    [
        ({"compras_ultimo_minuto": 6}, True),
        ({"compras_ultimo_minuto": 5}, False),
        ({"compras_ultimo_minuto": 2, "rafaga_detectada": True}, True),
    ],
)
def test_regla_frecuencia(nueva_transaccion, perfil, cambios, dispara):
    resultado = regla_frecuencia(nueva_transaccion(), features(**cambios), perfil)
    assert (resultado is not None) == dispara
    if dispara:
        assert resultado.gravedad == GRAVEDAD_ALTA


@pytest.mark.parametrize(
    ("distancia", "minutos", "dispara"),
    [(505.0, 10.0, True), (505.0, 45.0, False), (5.0, 5.0, False), (None, None, False)],
)
def test_regla_viaje_imposible(nueva_transaccion, perfil, distancia, minutos, dispara):
    resultado = regla_viaje_imposible(
        nueva_transaccion(),
        features(distancia_km_ultima=distancia, minutos_desde_ultima=minutos),
        perfil,
    )
    assert (resultado is not None) == dispara


@pytest.mark.parametrize(
    ("importe", "gasto_medio", "dispara"),
    [(240.0, 40.0, True), (160.0, 40.0, False), (5000.0, None, False)],
)
def test_regla_importe_inusual(nueva_transaccion, perfil, importe, gasto_medio, dispara):
    resultado = regla_importe_inusual(
        nueva_transaccion(),
        features(importe=importe, gasto_medio_categoria=gasto_medio),
        perfil,
    )
    assert (resultado is not None) == dispara


@pytest.mark.parametrize(
    ("comercio", "categoria", "fuera", "dispara"),
    [
        (False, False, True, True),
        (True, False, True, False),
        (False, True, True, False),
        (False, False, False, False),
    ],
)
def test_regla_comportamiento_exige_las_tres_condiciones(
    nueva_transaccion, perfil, comercio, categoria, fuera, dispara
):
    resultado = regla_comportamiento(
        nueva_transaccion(),
        features(comercio_habitual=comercio, categoria_habitual=categoria, fuera_de_horario=fuera),
        perfil,
    )
    assert (resultado is not None) == dispara


def test_regla_comportamiento_sin_perfil(nueva_transaccion):
    datos = features(comercio_habitual=False, categoria_habitual=False, fuera_de_horario=True)
    assert regla_comportamiento(nueva_transaccion(), datos, None) is None


def test_las_reglas_son_puras(nueva_transaccion, perfil):
    transaccion = nueva_transaccion()
    datos = features(compras_ultimo_minuto=8)
    copia = dict(datos)
    assert evaluar_reglas(transaccion, datos, perfil) == evaluar_reglas(transaccion, datos, perfil)
    assert datos == copia


ALTA = ResultadoRegla("viaje_imposible", "viaje", GRAVEDAD_ALTA)
MEDIA = ResultadoRegla("importe_inusual", "importe", GRAVEDAD_MEDIA)
OTRA_MEDIA = ResultadoRegla("comportamiento", "comportamiento", GRAVEDAD_MEDIA)


@pytest.mark.parametrize(
    ("resultados", "puntuacion", "esperada"),
    [
        ([], 0.05, APROBADA),
        ([MEDIA], 0.05, REVISAR),
        ([ALTA], 0.05, BLOQUEADA),
        ([MEDIA, OTRA_MEDIA], 0.05, BLOQUEADA),
        ([], 0.3, REVISAR),
        ([], 0.6, BLOQUEADA),
        ([MEDIA], 0.3, BLOQUEADA),
    ],
)
def test_decision(resultados, puntuacion, esperada):
    decision = decidir(resultados, puntuacion, umbral_revision=0.2, umbral_bloqueo=0.5)
    assert decision.decision == esperada
    assert (MOTIVO_MODELO in decision.motivos) == (puntuacion >= 0.2)
    assert all(r.motivo in decision.motivos for r in resultados)


def test_viaje_imposible_de_principio_a_fin(nueva_transaccion, perfil):
    momento = datetime(2026, 10, 27, 10, 30, tzinfo=UTC)
    estado = EstadoTarjeta(
        ultima_lat=41.3874, ultima_lon=2.1686, ultima_fecha=momento - timedelta(minutes=12)
    )
    transaccion = nueva_transaccion(fecha=momento)
    resultados = evaluar_reglas(transaccion, calcular_features(transaccion, estado, perfil), perfil)
    assert [r.regla for r in resultados] == ["viaje_imposible"]
    assert decidir(resultados, 0.0, 0.2, 0.5).decision == BLOQUEADA
