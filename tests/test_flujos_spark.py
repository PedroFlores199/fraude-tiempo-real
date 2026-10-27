import json
from datetime import UTC, datetime, timedelta

import fakeredis
import mongomock
import pytest
from pyspark.sql import types as T

from comun.esquema import cargar_esquema
from comun.tiempo import a_iso
from generador.perfiles import generar_perfil
from streaming.enriquecimiento.estado_tarjetas import EstadoTarjetasRedis
from streaming.enriquecimiento.perfiles import CachePerfiles
from streaming.esquema_spark import struct_desde_esquema
from streaming.flujos import (
    ESQUEMA_DECISION,
    compras_por_ventana,
    mensajes_decision,
    mensajes_error,
    parsear,
    resumen_lote,
    separar,
)
from streaming.procesado import ContextoProcesado, procesar_lote

pytestmark = pytest.mark.spark

ESQUEMA = cargar_esquema()
INICIO = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
COLUMNAS_KAFKA = "value binary, timestamp timestamp, topic string, partition int, offset long"
PERFIL = generar_perfil(1)


class PredictorNulo:
    def puntuar(self, features: list[dict]) -> list[float]:
        return [0.0] * len(features)


def evaluar_con_servicios_en_memoria(lotes):
    base = mongomock.MongoClient(tz_aware=True)["fraude"]
    base["clientes"].insert_one(PERFIL)
    contexto = ContextoProcesado(
        estado=EstadoTarjetasRedis(fakeredis.FakeRedis(), 90000, 600),
        perfiles=CachePerfiles(base["clientes"], ttl_s=300),
        predictor=PredictorNulo(),
        alertas=base["alertas"],
        umbral_revision=0.2,
        umbral_bloqueo=0.5,
    )
    for lote in lotes:
        yield procesar_lote(lote, contexto)


def kafka(spark, valores: list[str]):
    filas = [
        (valor.encode("utf-8"), INICIO, "transacciones", 0, i) for i, valor in enumerate(valores)
    ]
    return spark.createDataFrame(filas, COLUMNAS_KAFKA)


def test_struct_derivado_del_json_schema():
    struct = struct_desde_esquema(ESQUEMA)
    assert struct.fieldNames() == list(ESQUEMA["properties"])
    assert struct["importe"].dataType == T.DoubleType()
    assert struct["fecha_hora"].dataType == T.StringType()


def test_separa_mensajes_validos_y_rotos(spark, nueva_transaccion):
    valida = nueva_transaccion()
    sin_ciudad = {k: v for k, v in valida.items() if k != "ciudad"}
    casos = {
        "valida": json.dumps(valida),
        "json_mal_formado": '{"id_transaccion": "abc"',
        "tipo_invalido:importe": json.dumps({**valida, "importe": "doce"}),
        "fuera_de_rango:importe": json.dumps({**valida, "importe": -3}),
        "campos_no_permitidos": json.dumps({**valida, "es_fraude": True}),
        "falta_campo:ciudad": json.dumps(sin_ciudad),
        "valor_no_permitido:categoria": json.dumps({**valida, "categoria": "casino"}),
        "formato_invalido:tarjeta_id": json.dumps({**valida, "tarjeta_id": "1234"}),
        "fecha_invalida:fecha_hora": json.dumps({**valida, "fecha_hora": "2026-13-45T99:00:00Z"}),
    }
    validas, errores = separar(parsear(kafka(spark, list(casos.values())), ESQUEMA), ESQUEMA)
    motivos = [fila.motivo_error for fila in errores.orderBy("offset").collect()]
    assert motivos == list(casos)[1:]
    fila = validas.collect()[0]
    assert fila.id_transaccion == valida["id_transaccion"]
    assert fila.fecha_evento.astimezone(UTC) == INICIO.replace(minute=30)
    assert dict(validas.dtypes)["fecha_evento"] == "timestamp"


def test_los_errores_van_a_la_cola_con_el_mensaje_original(spark):
    _, errores = separar(parsear(kafka(spark, ["no es json"]), ESQUEMA), ESQUEMA)
    mensaje = json.loads(mensajes_error(errores).collect()[0].value)
    assert mensaje["mensaje_original"] == "no es json"
    assert mensaje["motivo"] == "json_mal_formado"
    assert mensaje["topico_origen"] == "transacciones"


def test_ventana_de_un_minuto_con_watermark(spark, nueva_transaccion):
    rafaga = [
        nueva_transaccion(tarjeta_id="TAR-0000000000aa", fecha=INICIO + timedelta(seconds=6 * i))
        for i in range(7)
    ]
    normales = [
        nueva_transaccion(tarjeta_id="TAR-0000000000bb", fecha=INICIO + timedelta(seconds=20 * i))
        for i in range(3)
    ]
    validas, _ = separar(
        parsear(kafka(spark, [json.dumps(t) for t in rafaga + normales]), ESQUEMA), ESQUEMA
    )
    ventanas = compras_por_ventana(validas, "2 minutes").collect()
    assert ventanas
    assert {fila.tarjeta_id for fila in ventanas} == {"TAR-0000000000aa"}
    assert max(fila.compras for fila in ventanas) == 7
    completa = max(ventanas, key=lambda fila: fila.compras)
    assert set(completa.ids_transaccion) == {t["id_transaccion"] for t in rafaga}
    assert (completa.ventana_fin - completa.ventana_inicio) == timedelta(minutes=1)


def test_map_in_pandas_decide_con_el_esquema_de_salida(spark_con_arrow, nueva_transaccion):
    tarjeta = PERFIL["tarjetas"][0]["tarjeta_id"]
    origen = nueva_transaccion(cliente_id=PERFIL["_id"], tarjeta_id=tarjeta, fecha=INICIO)
    lejana = nueva_transaccion(
        cliente_id=PERFIL["_id"],
        tarjeta_id=tarjeta,
        fecha=INICIO + timedelta(minutes=9),
        ciudad="A Coruña",
        lat=43.3623,
        lon=-8.4115,
    )
    valores = [json.dumps(origen), json.dumps(lejana)]
    validas, _ = separar(parsear(kafka(spark_con_arrow, valores), ESQUEMA), ESQUEMA)
    decisiones = validas.coalesce(1).mapInPandas(evaluar_con_servicios_en_memoria, ESQUEMA_DECISION)
    filas = decisiones.orderBy("fecha_hora").collect()
    assert [fila.decision for fila in filas] == ["APROBADA", "BLOQUEADA"]
    assert filas[1].motivos == ["mas_de_300_km_en_menos_de_30_min"]
    assert filas[1].latencia_ms >= 0


def test_decisiones_a_kafka_y_resumen_del_lote(spark):
    fecha = INICIO
    decisiones = spark.createDataFrame(
        [
            ("t1", "TAR-1", "CLI-00001", fecha, "APROBADA", [], 0.01, 100.0, fecha),
            (
                "t2",
                "TAR-1",
                "CLI-00001",
                fecha,
                "BLOQUEADA",
                ["mas_de_5_compras_en_1_minuto"],
                0.4,
                300.0,
                fecha,
            ),
            (
                "t3",
                "TAR-2",
                "CLI-00001",
                fecha,
                "REVISAR",
                ["puntuacion_alta_del_modelo"],
                0.25,
                200.0,
                fecha,
            ),
        ],
        ESQUEMA_DECISION,
    )
    mensaje = mensajes_decision(decisiones).orderBy("key").collect()[0]
    cuerpo = json.loads(mensaje.value)
    assert mensaje.key == "TAR-1"
    assert cuerpo["fecha_hora"] == a_iso(INICIO)
    resumen = resumen_lote(decisiones).first()
    assert (resumen.transacciones, resumen.bloqueadas, resumen.revisadas) == (3, 1, 1)
    assert resumen.suma_latencia_ms == 600.0
    assert resumen.max_latencia_ms == 300.0
