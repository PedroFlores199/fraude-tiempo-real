import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator

from comun.alertas import FRAUDE_CONFIRMADO, marcar_alerta
from comun.tiempo import desde_iso
from streaming.enriquecimiento.estado_tarjetas import EstadoTarjetasRedis
from streaming.enriquecimiento.perfiles import CachePerfiles
from streaming.procesado import COLUMNAS_DECISION, ContextoProcesado, procesar_lote

ESQUEMA_ALERTAS = Path(__file__).resolve().parent.parent / "mongo" / "esquemas" / "alertas.json"
INICIO = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
DECIDIDO_EN = INICIO + timedelta(hours=1)


class PredictorFijo:
    def __init__(self, puntuacion: float = 0.01) -> None:
        self.puntuacion = puntuacion

    def puntuar(self, features: list[dict]) -> list[float]:
        return [self.puntuacion] * len(features)


@pytest.fixture
def contexto(mongo, redis_falso, perfil):
    mongo["clientes"].insert_one(perfil)
    return ContextoProcesado(
        estado=EstadoTarjetasRedis(redis_falso, 90000, 600),
        perfiles=CachePerfiles(mongo["clientes"], ttl_s=300),
        predictor=PredictorFijo(),
        alertas=mongo["alertas"],
        umbral_revision=0.2,
        umbral_bloqueo=0.5,
        reloj=lambda: DECIDIDO_EN,
    )


def lote(transacciones: list[dict], retraso_kafka_s: float = 0.5) -> pd.DataFrame:
    filas = []
    for transaccion in transacciones:
        recibido = desde_iso(transaccion["fecha_hora"]) + timedelta(seconds=retraso_kafka_s)
        filas.append({**transaccion, "kafka_ts": recibido.replace(tzinfo=None)})
    return pd.DataFrame(filas)


def test_una_compra_normal_se_aprueba_sin_alerta(contexto, nueva_transaccion, mongo):
    salida = procesar_lote(lote([nueva_transaccion(fecha=INICIO)]), contexto)
    assert list(salida.columns) == COLUMNAS_DECISION
    assert salida.loc[0, "decision"] == "APROBADA"
    assert mongo["alertas"].count_documents({}) == 0


def test_viaje_imposible_dentro_del_mismo_lote(contexto, nueva_transaccion, mongo):
    madrid = nueva_transaccion(fecha=INICIO)
    barcelona = nueva_transaccion(
        fecha=INICIO + timedelta(minutes=9),
        ciudad="Barcelona",
        lat=41.3874,
        lon=2.1686,
        comercio="Zara",
        categoria="moda",
    )
    salida = procesar_lote(lote([madrid, barcelona]), contexto)
    assert list(salida["decision"]) == ["APROBADA", "BLOQUEADA"]
    alerta = mongo["alertas"].find_one({"_id": barcelona["id_transaccion"]})
    assert alerta["motivos"] == ["mas_de_300_km_en_menos_de_30_min"]
    assert alerta["estado"] == "PENDIENTE"
    assert alerta["fecha"] == INICIO + timedelta(minutes=9)
    assert alerta["transaccion"]["fecha_hora"] == alerta["fecha"]
    assert alerta["perfil_cliente"]["_id"] == "CLI-00001"
    assert alerta["features"]["distancia_km_ultima"] > 300
    assert alerta["latencia_ms"] == pytest.approx(
        (DECIDIDO_EN - INICIO - timedelta(minutes=9, seconds=0.5)).total_seconds() * 1000
    )


def test_rafaga_de_compras_pequenas(contexto, nueva_transaccion):
    compras = [
        nueva_transaccion(
            fecha=INICIO + timedelta(seconds=5 * i),
            importe=1.5,
            comercio="Steam",
            categoria="ocio",
            canal="online",
        )
        for i in range(8)
    ]
    salida = procesar_lote(lote(compras), contexto)
    assert list(salida["decision"][:5]) == ["APROBADA"] * 5
    assert list(salida["decision"][5:]) == ["BLOQUEADA"] * 3
    assert all("mas_de_5_compras_en_1_minuto" in motivos for motivos in salida["motivos"][5:])


def test_el_modelo_puede_mandar_a_revisar(contexto, nueva_transaccion, mongo):
    contexto.predictor = PredictorFijo(0.3)
    salida = procesar_lote(lote([nueva_transaccion(fecha=INICIO)]), contexto)
    assert salida.loc[0, "decision"] == "REVISAR"
    assert mongo["alertas"].find_one()["puntuacion_modelo"] == 0.3


def test_reprocesar_un_lote_es_idempotente(contexto, nueva_transaccion, mongo):
    madrid = nueva_transaccion(fecha=INICIO)
    bilbao = nueva_transaccion(
        fecha=INICIO + timedelta(minutes=5), ciudad="Bilbao", lat=43.263, lon=-2.935
    )
    primera = procesar_lote(lote([madrid, bilbao]), contexto)
    marcar_alerta(mongo["alertas"], bilbao["id_transaccion"], FRAUDE_CONFIRMADO)
    segunda = procesar_lote(lote([madrid, bilbao]), contexto)
    assert list(primera["decision"]) == list(segunda["decision"])
    assert mongo["alertas"].count_documents({}) == 1
    assert mongo["alertas"].find_one()["estado"] == FRAUDE_CONFIRMADO


def test_cliente_desconocido_no_rompe_el_procesado(contexto, nueva_transaccion):
    salida = procesar_lote(lote([nueva_transaccion(cliente_id="CLI-99999")]), contexto)
    assert salida.loc[0, "decision"] == "APROBADA"


def test_lote_vacio(contexto):
    assert procesar_lote(pd.DataFrame(), contexto).empty


def test_las_alertas_cumplen_el_json_schema_de_mongo(contexto, nueva_transaccion, mongo):
    contexto.predictor = PredictorFijo(0.7)
    procesar_lote(lote([nueva_transaccion(fecha=INICIO)]), contexto)
    validador = Draft202012Validator(json.loads(ESQUEMA_ALERTAS.read_text(encoding="utf-8")))
    alerta = mongo["alertas"].find_one()
    assert [e.message for e in validador.iter_errors(alerta)] == []
