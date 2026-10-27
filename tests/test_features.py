import math
from datetime import UTC, datetime, timedelta

from comun.features import COLUMNAS_MODELO, EstadoTarjeta, calcular_features, vector_modelo

MOMENTO = datetime(2026, 10, 27, 10, 30, tzinfo=UTC)


def test_primera_compra_sin_historial(nueva_transaccion, perfil):
    features = calcular_features(nueva_transaccion(), EstadoTarjeta(), perfil)
    assert features["minutos_desde_ultima"] is None
    assert features["distancia_km_ultima"] is None
    assert features["compras_ultimo_minuto"] == 1
    assert features["gasto_24h"] == 42.5
    assert features["categoria_habitual"] and features["comercio_habitual"]
    assert features["hora_local"] == 11
    assert not features["fuera_de_horario"]
    assert math.isclose(features["ratio_importe_categoria"], 42.5 / 40.0)


def test_distancia_y_velocidad_respecto_a_la_ultima_compra(nueva_transaccion, perfil):
    estado = EstadoTarjeta(
        ultima_lat=41.3874,
        ultima_lon=2.1686,
        ultima_fecha=MOMENTO - timedelta(minutes=10),
        compras_ultimo_minuto=2,
        gasto_24h=100.0,
    )
    features = calcular_features(nueva_transaccion(fecha=MOMENTO), estado, perfil)
    assert 490 < features["distancia_km_ultima"] < 520
    assert features["minutos_desde_ultima"] == 10
    assert features["velocidad_kmh"] > 2900
    assert features["compras_ultimo_minuto"] == 3
    assert features["gasto_24h"] == 142.5


def test_categoria_y_comercio_no_habituales_de_madrugada(nueva_transaccion, perfil):
    transaccion = nueva_transaccion(
        comercio="Bet365",
        categoria="apuestas",
        canal="online",
        fecha=datetime(2026, 10, 27, 2, 15, tzinfo=UTC),
    )
    features = calcular_features(transaccion, EstadoTarjeta(), perfil)
    assert features["hora_local"] == 3
    assert features["fuera_de_horario"]
    assert not features["categoria_habitual"]
    assert not features["comercio_habitual"]
    assert features["gasto_medio_categoria"] is None
    assert features["es_online"]


def test_cliente_sin_perfil(nueva_transaccion):
    features = calcular_features(nueva_transaccion(), EstadoTarjeta(), None)
    assert not features["categoria_habitual"]
    assert not features["fuera_de_horario"]
    assert features["ratio_importe_categoria"] > 0


def test_vector_del_modelo(nueva_transaccion, perfil):
    vector = vector_modelo(calcular_features(nueva_transaccion(), EstadoTarjeta(), perfil))
    assert len(vector) == len(COLUMNAS_MODELO)
    assert all(math.isfinite(valor) for valor in vector)
