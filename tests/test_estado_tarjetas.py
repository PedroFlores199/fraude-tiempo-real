import random
from datetime import UTC, datetime, timedelta

import pytest

from comun.tiempo import desde_iso
from streaming.enriquecimiento.estado_tarjetas import (
    EstadoTarjetasMemoria,
    EstadoTarjetasRedis,
    clave_historial,
    clave_rafaga,
)

TARJETA = "TAR-00000000000a"
INICIO = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)


@pytest.fixture
def estado(redis_falso):
    return EstadoTarjetasRedis(redis_falso, ttl_historial_s=90000, ttl_rafaga_s=600)


def test_claves_con_formato_y_ttl_explicito(estado, redis_falso, nueva_transaccion):
    estado.registrar(nueva_transaccion())
    estado.marcar_rafaga(TARJETA)
    assert clave_historial(TARJETA) == f"tarjeta:{TARJETA}:historial"
    assert clave_rafaga(TARJETA) == f"tarjeta:{TARJETA}:rafaga"
    assert 0 < redis_falso.ttl(clave_historial(TARJETA)) <= 90000
    assert 0 < redis_falso.ttl(clave_rafaga(TARJETA)) <= 600


def test_compras_del_ultimo_minuto_por_hora_del_evento(estado, nueva_transaccion):
    for segundos in (0, 20, 40, 50, 130):
        estado.registrar(nueva_transaccion(fecha=INICIO + timedelta(seconds=segundos)))
    leido = estado.leer(TARJETA, INICIO + timedelta(seconds=55), "nueva")
    assert leido.compras_ultimo_minuto == 4
    assert leido.gasto_24h == pytest.approx(5 * 42.5 - 42.5)


def test_gasto_de_las_ultimas_24_horas(estado, nueva_transaccion):
    estado.registrar(nueva_transaccion(importe=100.0, fecha=INICIO - timedelta(hours=25)))
    estado.registrar(nueva_transaccion(importe=30.0, fecha=INICIO - timedelta(hours=2)))
    estado.registrar(nueva_transaccion(importe=20.0, fecha=INICIO - timedelta(minutes=5)))
    assert estado.leer(TARJETA, INICIO, "nueva").gasto_24h == pytest.approx(50.0)


def test_la_ultima_ubicacion_es_la_del_evento_anterior_en_el_tiempo(estado, nueva_transaccion):
    madrid = nueva_transaccion(fecha=INICIO - timedelta(minutes=5))
    barcelona = nueva_transaccion(fecha=INICIO, ciudad="Barcelona", lat=41.38, lon=2.17)
    estado.registrar(barcelona)
    estado.registrar(madrid)
    despues = estado.leer(TARJETA, INICIO + timedelta(minutes=1), "nueva")
    assert (despues.ultima_lat, despues.ultima_lon) == (41.38, 2.17)
    assert despues.ultima_fecha == INICIO
    tardio = estado.leer(TARJETA, INICIO - timedelta(minutes=2), "tardio")
    assert (tardio.ultima_lat, tardio.ultima_lon) == (madrid["lat"], madrid["lon"])


def test_el_historial_descarta_lo_anterior_a_24_horas(estado, redis_falso, nueva_transaccion):
    estado.registrar(nueva_transaccion(fecha=INICIO - timedelta(hours=30)))
    estado.registrar(nueva_transaccion(fecha=INICIO))
    assert redis_falso.zcard(clave_historial(TARJETA)) == 1


def test_reprocesar_una_transaccion_devuelve_la_ubicacion_anterior(estado, nueva_transaccion):
    madrid = nueva_transaccion(fecha=INICIO)
    barcelona = nueva_transaccion(
        fecha=INICIO + timedelta(minutes=8), ciudad="Barcelona", lat=41.38, lon=2.17
    )
    estado.registrar(madrid)
    estado.registrar(barcelona)
    leido = estado.leer(TARJETA, desde_iso(barcelona["fecha_hora"]), barcelona["id_transaccion"])
    assert (leido.ultima_lat, leido.ultima_lon) == (madrid["lat"], madrid["lon"])
    assert leido.compras_ultimo_minuto == 0
    primero = estado.leer(TARJETA, desde_iso(madrid["fecha_hora"]), madrid["id_transaccion"])
    assert primero.ultima_fecha is None


def test_rafaga_marcada_por_la_ventana(estado):
    assert not estado.leer(TARJETA, INICIO, "x").rafaga_detectada
    estado.marcar_rafaga(TARJETA)
    assert estado.leer(TARJETA, INICIO, "x").rafaga_detectada


def test_redis_y_memoria_calculan_lo_mismo(estado, nueva_transaccion):
    memoria = EstadoTarjetasMemoria()
    rng = random.Random(11)
    momento = INICIO
    for _ in range(300):
        momento += timedelta(seconds=rng.choice([1, 5, 20, 90, 900]))
        desfase = timedelta(seconds=-rng.randint(0, 120) if rng.random() < 0.1 else 0)
        transaccion = nueva_transaccion(
            tarjeta_id=rng.choice(["TAR-00000000000a", "TAR-00000000000b"]),
            importe=round(rng.uniform(1, 300), 2),
            lat=round(rng.uniform(36, 43), 4),
            lon=round(rng.uniform(-8, 3), 4),
            fecha=momento + desfase,
        )
        fecha = desde_iso(transaccion["fecha_hora"])
        de_redis = estado.leer(transaccion["tarjeta_id"], fecha, transaccion["id_transaccion"])
        de_memoria = memoria.leer(transaccion["tarjeta_id"], fecha, transaccion["id_transaccion"])
        assert de_redis.ultima_fecha == de_memoria.ultima_fecha
        assert de_redis.ultima_lat == de_memoria.ultima_lat
        assert de_redis.compras_ultimo_minuto == de_memoria.compras_ultimo_minuto
        assert de_redis.gasto_24h == pytest.approx(de_memoria.gasto_24h)
        estado.registrar(transaccion)
        memoria.registrar(transaccion)
