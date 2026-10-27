from datetime import UTC, datetime

import pytest

from carga.estadisticas import percentiles, tabla_markdown
from carga.prueba_carga import RegistroLatencias, resumen
from generador.transacciones import Evento


def evento(identificador: str) -> Evento:
    fecha = datetime(2026, 10, 27, tzinfo=UTC)
    return Evento({"id_transaccion": identificador}, fecha, fecha, False)


def test_percentiles():
    valores = percentiles([float(i) for i in range(1, 101)])
    assert valores["p50"] == 50.5
    assert valores["p99"] == 99.01
    assert valores["max"] == 100.0
    assert percentiles([])["p95"] == 0.0


def test_tabla_markdown():
    tabla = tabla_markdown({"Redis": percentiles([1.0, 2.0])})
    assert tabla.splitlines()[0].startswith("| | media (ms)")
    assert "| Redis | 1.50 |" in tabla


def test_latencia_extremo_a_extremo():
    registro = RegistroLatencias()
    registro.al_enviar(evento("a"), 100.0)
    registro.al_enviar(evento("b"), 100.5)
    registro.al_enviar(evento("c"), 101.0)
    registro.al_decidir("a", 100.8)
    registro.al_decidir("b", 106.5)
    registro.al_decidir("a", 109.0)
    assert sorted(registro.latencias_ms()) == pytest.approx([800.0, 6000.0])
    assert registro.pendientes() == 1
    informe = resumen(registro, tps=3, duracion_s=1.0, poblacion={})
    assert informe["decisiones_recibidas"] == 2
    assert informe["sin_decision"] == 1
    assert informe["porcentaje_bajo_5_s"] == 50.0
