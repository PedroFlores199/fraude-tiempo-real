import json
from datetime import UTC, datetime, timedelta

import pytest

from comun.esquema import es_valida
from comun.tiempo import ZONA_LOCAL, a_epoch_ms, desde_iso
from generador.perfiles import generar_perfiles
from generador.productor import (
    AlmacenEtiquetas,
    PlanificadorEnvios,
    ProductorTransacciones,
    clientes_necesarios,
    desfase_para_hora,
)
from generador.transacciones import Evento, GeneradorTransacciones

INICIO = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)


class ProductorKafkaFalso:
    def __init__(self) -> None:
        self.mensajes: list[dict] = []
        self.vaciado = False

    def produce(self, topico, key, value, timestamp, on_delivery):
        self.mensajes.append({"topico": topico, "clave": key, "valor": value, "ts": timestamp})

    def poll(self, espera):
        return 0

    def flush(self, espera):
        self.vaciado = True


class RelojFalso:
    def __init__(self, inicio: datetime) -> None:
        self.ahora = inicio.timestamp()

    def __call__(self) -> float:
        return self.ahora

    def dormir(self, segundos: float) -> None:
        self.ahora += max(segundos, 0.1)


def _evento(nueva_transaccion, segundos: int, retraso: int = 0, fraude: bool = False) -> Evento:
    fecha = INICIO + timedelta(seconds=segundos)
    return Evento(
        nueva_transaccion(fecha=fecha),
        fecha,
        fecha + timedelta(seconds=retraso),
        fraude,
        "prueba_tarjeta" if fraude else None,
    )


def test_el_planificador_entrega_por_hora_de_envio(nueva_transaccion):
    planificador = PlanificadorEnvios()
    tardio = _evento(nueva_transaccion, 0, retraso=30)
    normal = _evento(nueva_transaccion, 10)
    planificador.agregar([tardio, normal])
    assert planificador.extraer_listos(INICIO + timedelta(seconds=15)) == [normal]
    assert planificador.extraer_listos(INICIO + timedelta(seconds=40)) == [tardio]
    assert len(planificador) == 0


@pytest.fixture
def ejecucion(mongo):
    kafka = ProductorKafkaFalso()
    reloj = RelojFalso(INICIO)
    productor = ProductorTransacciones(kafka, "transacciones", AlmacenEtiquetas(mongo["etiquetas"]))
    generador = GeneradorTransacciones(generar_perfiles(500), tasa_retraso=0.05, semilla=5)
    productor.ejecutar(generador, tps=50, duracion_s=60, reloj=reloj, dormir=reloj.dormir)
    return kafka, productor, mongo


def test_envia_mensajes_validos_con_clave_de_tarjeta(ejecucion):
    kafka, productor, _ = ejecucion
    assert kafka.vaciado
    assert 2800 <= len(kafka.mensajes) <= 3400
    assert productor.enviadas == len(kafka.mensajes)
    for mensaje in kafka.mensajes:
        transaccion = json.loads(mensaje["valor"])
        assert es_valida(transaccion)
        assert mensaje["clave"].decode() == transaccion["tarjeta_id"]
        assert mensaje["topico"] == "transacciones"


def test_nunca_envia_antes_de_la_hora_del_evento_y_hay_mensajes_tardios(ejecucion):
    kafka, _, _ = ejecucion
    retrasos = [
        mensaje["ts"] - a_epoch_ms(desde_iso(json.loads(mensaje["valor"])["fecha_hora"]))
        for mensaje in kafka.mensajes
    ]
    assert min(retrasos) >= -1
    assert sum(1 for r in retrasos if r >= 5000) > 0.02 * len(retrasos)


def test_guarda_la_verdad_del_fraude_fuera_del_mensaje(ejecucion):
    kafka, productor, mongo = ejecucion
    etiquetas = list(mongo["etiquetas"].find())
    assert len(etiquetas) == productor.fraudes > 0
    assert all(e["es_fraude"] and e["patron"] for e in etiquetas)
    assert all("es_fraude" not in json.loads(m["valor"]) for m in kafka.mensajes)


def test_mensajes_rotos_para_probar_la_cola_de_errores(nueva_transaccion):
    kafka = ProductorKafkaFalso()
    productor = ProductorTransacciones(kafka, "transacciones", tasa_mensajes_rotos=1.0, semilla=1)
    productor.enviar([_evento(nueva_transaccion, 0)])
    with pytest.raises(json.JSONDecodeError):
        json.loads(kafka.mensajes[0]["valor"])
    assert productor.rotas == 1


def test_descarta_transacciones_que_no_cumplen_el_esquema(nueva_transaccion):
    kafka = ProductorKafkaFalso()
    productor = ProductorTransacciones(kafka, "transacciones")
    fecha = INICIO
    invalido = Evento(nueva_transaccion(importe=-5), fecha, fecha, False)
    productor.enviar([invalido])
    assert kafka.mensajes == []
    assert productor.rechazadas == 1


def test_etiquetas_duplicadas_no_rompen_el_productor(mongo, nueva_transaccion):
    almacen = AlmacenEtiquetas(mongo["etiquetas"])
    evento = _evento(nueva_transaccion, 0, fraude=True)
    almacen.guardar([evento])
    almacen.guardar([evento])
    assert mongo["etiquetas"].count_documents({}) == 1


def test_desfase_para_simular_otra_hora_local():
    ahora = datetime(2026, 10, 27, 15, 20, tzinfo=UTC)
    simulado = ahora + desfase_para_hora("03:00", ahora)
    assert simulado.astimezone(ZONA_LOCAL).strftime("%H:%M") == "03:00"


def test_clientes_necesarios_para_500_tps():
    assert clientes_necesarios(500, 15, 2.0) == 4501


def test_los_mensajes_rotos_no_se_etiquetan(mongo, nueva_transaccion):
    kafka = ProductorKafkaFalso()
    productor = ProductorTransacciones(
        kafka, "transacciones", AlmacenEtiquetas(mongo["etiquetas"]), tasa_mensajes_rotos=1.0
    )
    productor.enviar([_evento(nueva_transaccion, 0, fraude=True)])
    assert len(kafka.mensajes) == 1
    assert mongo["etiquetas"].count_documents({}) == 0
    assert productor.fraudes == 0
