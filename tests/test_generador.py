from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from comun.geo import distancia_km
from comun.tiempo import desde_iso, hora_local
from generador.catalogo import CATEGORIA_DE_COMERCIO
from generador.perfiles import generar_perfiles
from generador.transacciones import (
    IMPORTE_DESPROPORCIONADO,
    MADRUGADA_INUSUAL,
    PRUEBA_TARJETA,
    VIAJE_IMPOSIBLE,
    GeneradorTransacciones,
)

INICIO_DIURNO = datetime(2026, 10, 27, 9, 0, tzinfo=UTC)
INICIO_NOCTURNO = datetime(2026, 10, 27, 2, 0, tzinfo=UTC)


def _simular(perfiles, inicio, minutos, tps, semilla=3, **opciones):
    generador = GeneradorTransacciones(perfiles, semilla=semilla, **opciones)
    eventos = []
    for segundo in range(minutos * 60):
        eventos.extend(generador.generar(inicio + timedelta(seconds=segundo), 1.0, tps))
    return generador, eventos


@pytest.fixture(scope="module")
def perfiles():
    return {p["_id"]: p for p in generar_perfiles(1000)}


@pytest.fixture(scope="module")
def diurno(perfiles):
    return _simular(list(perfiles.values()), INICIO_DIURNO, 20, 50)[1]


@pytest.fixture(scope="module")
def nocturno(perfiles):
    return _simular(list(perfiles.values()), INICIO_NOCTURNO, 20, 50)[1]


def test_tasa_de_fraude_entre_1_y_2_por_ciento_a_50_tps(diurno):
    fraudes = sum(evento.es_fraude for evento in diurno)
    assert 0.01 <= fraudes / len(diurno) <= 0.02


def test_volumen_coherente_con_los_tps(diurno):
    assert abs(len(diurno) - 50 * 20 * 60) / (50 * 20 * 60) < 0.02


def test_las_compras_normales_respetan_el_intervalo_por_tarjeta(diurno):
    por_tarjeta = defaultdict(list)
    for evento in diurno:
        if not evento.es_fraude and evento.patron is None:
            por_tarjeta[evento.transaccion["tarjeta_id"]].append(evento.fecha_hora)
    for fechas in por_tarjeta.values():
        fechas.sort()
        for anterior, siguiente in zip(fechas, fechas[1:], strict=False):
            assert (siguiente - anterior).total_seconds() >= 15


def test_prueba_de_tarjeta_es_una_rafaga_de_6_a_10_compras_pequenas(diurno):
    rafagas = defaultdict(list)
    for evento in diurno:
        if evento.patron == PRUEBA_TARJETA:
            rafagas[evento.transaccion["tarjeta_id"]].append(evento)
    assert rafagas
    for eventos in rafagas.values():
        assert 6 <= len(eventos) <= 10
        fechas = sorted(e.fecha_hora for e in eventos)
        assert (fechas[-1] - fechas[0]).total_seconds() < 60
        assert all(e.transaccion["importe"] <= 3.0 for e in eventos)
        assert all(e.transaccion["canal"] == "online" for e in eventos)


def test_viaje_imposible_son_dos_ciudades_lejanas_en_pocos_minutos(diurno):
    por_tarjeta = defaultdict(list)
    for evento in diurno:
        por_tarjeta[evento.transaccion["tarjeta_id"]].append(evento)
    viajes = [e for e in diurno if e.patron == VIAJE_IMPOSIBLE]
    assert viajes
    for viaje in viajes:
        anteriores = [
            e
            for e in por_tarjeta[viaje.transaccion["tarjeta_id"]]
            if e.fecha_hora < viaje.fecha_hora and not e.es_fraude
        ]
        origen = max(anteriores, key=lambda e: e.fecha_hora)
        minutos = (viaje.fecha_hora - origen.fecha_hora).total_seconds() / 60
        distancia = distancia_km(
            origen.transaccion["lat"],
            origen.transaccion["lon"],
            viaje.transaccion["lat"],
            viaje.transaccion["lon"],
        )
        assert minutos < 30
        assert distancia > 300


def test_importe_desproporcionado_multiplica_por_diez_la_media(diurno, perfiles):
    casos = [e for e in diurno if e.patron == IMPORTE_DESPROPORCIONADO]
    assert casos
    for caso in casos:
        perfil = perfiles[caso.transaccion["cliente_id"]]
        media = perfil["categorias"][caso.transaccion["categoria"]]["gasto_medio"]
        assert caso.transaccion["importe"] >= 9.99 * media


def test_la_madrugada_inusual_solo_aparece_de_noche(diurno, nocturno, perfiles):
    assert not any(e.patron == MADRUGADA_INUSUAL for e in diurno)
    casos = [e for e in nocturno if e.patron == MADRUGADA_INUSUAL]
    assert casos
    for caso in casos:
        perfil = perfiles[caso.transaccion["cliente_id"]]
        assert hora_local(caso.fecha_hora) < 6
        assert caso.transaccion["categoria"] not in perfil["categorias"]
        assert caso.transaccion["comercio"] not in perfil["comercios_frecuentes"]


def test_los_retrasos_conservan_la_hora_original_del_evento(diurno):
    retrasados = [e for e in diurno if e.enviar_en > e.fecha_hora]
    assert 0.02 <= len(retrasados) / len(diurno) <= 0.04
    for evento in retrasados:
        assert desde_iso(evento.transaccion["fecha_hora"]) == evento.fecha_hora
        assert 5 <= (evento.enviar_en - evento.fecha_hora).total_seconds() <= 150


def test_los_comercios_y_categorias_son_coherentes(diurno):
    assert all(
        CATEGORIA_DE_COMERCIO[e.transaccion["comercio"]] == e.transaccion["categoria"]
        for e in diurno
    )


def test_la_tasa_de_fraude_no_se_trunca_con_pocos_tps(perfiles):
    _, eventos = _simular(list(perfiles.values()), INICIO_DIURNO, 10, 5)
    assert sum(e.es_fraude for e in eventos) > 0


def test_avisa_cuando_no_hay_tarjetas_suficientes(perfiles):
    pocos = list(perfiles.values())[:20]
    generador, _ = _simular(pocos, INICIO_DIURNO, 1, 50)
    assert generador.capacidad_tps < 50
    assert generador.saturado
