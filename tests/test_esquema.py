from datetime import UTC, datetime

import pytest

from comun.esquema import cargar_esquema, categorias, errores_validacion, es_valida
from generador.catalogo import COMERCIOS
from generador.perfiles import generar_perfiles
from generador.transacciones import GeneradorTransacciones


def test_transaccion_valida(nueva_transaccion):
    assert es_valida(nueva_transaccion())


@pytest.mark.parametrize(
    ("cambios", "fragmento"),
    [
        ({"importe": 0}, "importe"),
        ({"importe": "12"}, "importe"),
        ({"categoria": "casino"}, "casino"),
        ({"fecha_hora": "27/10/2026 10:00"}, "27/10/2026"),
        ({"fecha_hora": "2026-13-45T10:00:00Z"}, "2026-13-45"),
        ({"lat": 123.0}, "123"),
        ({"cliente_id": "cliente-1"}, "cliente-1"),
        ({"campo_extra": 1}, "campo_extra"),
    ],
)
def test_transaccion_invalida(nueva_transaccion, cambios, fragmento):
    errores = errores_validacion(nueva_transaccion(**cambios))
    assert errores
    assert any(fragmento in error for error in errores)


def test_falta_campo_obligatorio(nueva_transaccion):
    transaccion = nueva_transaccion()
    del transaccion["tarjeta_id"]
    assert any("tarjeta_id" in e for e in errores_validacion(transaccion))


def test_categorias_del_esquema_coinciden_con_el_catalogo():
    assert set(categorias()) == set(COMERCIOS)


def test_el_esquema_no_lleva_la_etiqueta_de_fraude():
    assert "es_fraude" not in cargar_esquema()["properties"]


def test_el_generador_produce_mensajes_validos():
    generador = GeneradorTransacciones(generar_perfiles(50), semilla=1)
    eventos = generador.generar(datetime(2026, 10, 27, 11, tzinfo=UTC), 60, 50)
    assert eventos
    assert all(es_valida(evento.transaccion) for evento in eventos)
