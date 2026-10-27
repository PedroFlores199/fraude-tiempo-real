from datetime import UTC, datetime, timedelta

import pytest

from comun.alertas import FALSO_POSITIVO, FRAUDE_CONFIRMADO, marcar_alerta
from comun.informes import (
    alertas_por_motivo_y_dia,
    falsos_positivos_por_regla,
    pipeline_alertas_por_motivo_y_dia,
    pipeline_precision_por_motivo,
    precision_por_motivo,
)

DIA = datetime(2026, 10, 27, 10, tzinfo=UTC)


@pytest.fixture
def alertas(mongo):
    documentos = [
        ("a1", DIA, ["viaje"], "BLOQUEADA", FRAUDE_CONFIRMADO),
        ("a2", DIA, ["viaje", "importe"], "BLOQUEADA", FALSO_POSITIVO),
        ("a3", DIA + timedelta(days=1), ["importe"], "REVISAR", FALSO_POSITIVO),
        ("a4", DIA + timedelta(days=1), ["importe"], "REVISAR", "PENDIENTE"),
    ]
    mongo["alertas"].insert_many(
        [
            {"_id": i, "fecha": fecha, "motivos": motivos, "decision": decision, "estado": estado}
            for i, fecha, motivos, decision, estado in documentos
        ]
    )
    mongo["etiquetas"].insert_many(
        [{"_id": "a1", "es_fraude": True}, {"_id": "a4", "es_fraude": True}]
    )
    return mongo["alertas"]


def test_marcar_fraude_confirmado(alertas):
    ahora = DIA + timedelta(hours=2)
    assert marcar_alerta(alertas, "a4", FRAUDE_CONFIRMADO, ahora)
    alerta = alertas.find_one({"_id": "a4"})
    assert alerta["estado"] == FRAUDE_CONFIRMADO
    assert alerta["revisado_en"] == ahora
    assert "descartada_en" not in alerta


def test_marcar_falso_positivo_programa_el_borrado_por_ttl(alertas):
    ahora = DIA + timedelta(hours=3)
    assert marcar_alerta(alertas, "a4", FALSO_POSITIVO, ahora)
    assert alertas.find_one({"_id": "a4"})["descartada_en"] == ahora
    marcar_alerta(alertas, "a4", FRAUDE_CONFIRMADO, ahora)
    assert "descartada_en" not in alertas.find_one({"_id": "a4"})


def test_marcar_con_estado_no_valido_o_alerta_inexistente(alertas):
    with pytest.raises(ValueError):
        marcar_alerta(alertas, "a1", "PENDIENTE")
    assert not marcar_alerta(alertas, "no-existe", FRAUDE_CONFIRMADO)


def test_alertas_por_motivo_y_dia(alertas):
    filas = alertas_por_motivo_y_dia(alertas)
    conteo = {(f["dia"], f["motivo"]): f["alertas"] for f in filas}
    assert conteo == {
        ("2026-10-27", "viaje"): 2,
        ("2026-10-27", "importe"): 1,
        ("2026-10-28", "importe"): 2,
    }
    solo_primer_dia = alertas_por_motivo_y_dia(alertas, hasta=DIA + timedelta(hours=1))
    assert {f["dia"] for f in solo_primer_dia} == {"2026-10-27"}


def test_porcentaje_de_falsos_positivos_por_regla(alertas):
    filas = {f["motivo"]: f for f in falsos_positivos_por_regla(alertas)}
    assert filas["viaje"]["revisadas"] == 2
    assert filas["viaje"]["porcentaje_falsos_positivos"] == 50.0
    assert filas["importe"]["porcentaje_falsos_positivos"] == 100.0


def test_precision_con_lookup_a_las_etiquetas(alertas):
    filas = {f["motivo"]: f for f in precision_por_motivo(alertas)}
    assert filas["viaje"]["fraude_real"] == 1
    assert filas["importe"]["precision"] == pytest.approx(1 / 3, abs=1e-4)


def test_los_pipelines_usan_las_etapas_pedidas():
    etapas = {
        etapa
        for pipeline in (pipeline_alertas_por_motivo_y_dia(), pipeline_precision_por_motivo())
        for paso in pipeline
        for etapa in paso
    }
    assert {"$match", "$group", "$lookup", "$unwind"} <= etapas
