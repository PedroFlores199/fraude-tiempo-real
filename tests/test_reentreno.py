from datetime import UTC, datetime

import pytest

from comun.alertas import FALSO_POSITIVO, FRAUDE_CONFIRMADO
from comun.features import EstadoTarjeta, calcular_features
from entrenamiento.reentreno import (
    cargar_feedback,
    comparar,
    desplegar,
    entrenar_candidato,
    es_de_validacion,
    exportar_feedback,
    preparar_conjuntos,
)


@pytest.fixture
def alertas_revisadas(mongo, nueva_transaccion, perfil):
    documentos = []
    for indice in range(40):
        transaccion = nueva_transaccion(importe=10.0 + indice)
        estado = [FRAUDE_CONFIRMADO, FALSO_POSITIVO, "PENDIENTE"][indice % 3]
        documentos.append(
            {
                "_id": transaccion["id_transaccion"],
                "estado": estado,
                "fecha": datetime(2026, 10, 27, tzinfo=UTC),
                "features": calcular_features(transaccion, EstadoTarjeta(), perfil),
            }
        )
    mongo["alertas"].insert_many(documentos)
    return mongo["alertas"]


def test_exporta_solo_las_alertas_revisadas(alertas_revisadas, tmp_path):
    destino = tmp_path / "feedback" / "feedback.jsonl"
    total = exportar_feedback(alertas_revisadas, destino)
    registros = cargar_feedback(destino)
    assert total == len(registros) == 27
    assert sum(r["es_fraude"] for r in registros) == 14
    assert all("features" in r for r in registros)


def test_particion_de_validacion_determinista():
    identificadores = [f"id-{i}" for i in range(2000)]
    primera = [es_de_validacion(i) for i in identificadores]
    assert primera == [es_de_validacion(i) for i in identificadores]
    assert 0.25 < sum(primera) / len(primera) < 0.35


def test_entrena_compara_y_despliega_solo_si_mejora(alertas_revisadas, tmp_path):
    ruta_feedback = tmp_path / "feedback.jsonl"
    exportar_feedback(alertas_revisadas, ruta_feedback)
    conjuntos = preparar_conjuntos(
        cargar_feedback(ruta_feedback), clientes=200, dias=0.4, tps_medio=2.0, semilla=8
    )
    produccion = tmp_path / "produccion" / "modelo.joblib"
    candidato = entrenar_candidato(conjuntos, tmp_path / "candidato.joblib", semilla=8)

    primera = comparar(candidato, produccion, conjuntos)
    assert primera["produccion"] is None
    assert primera["desplegar"]
    desplegar(candidato, produccion)
    assert produccion.exists()

    segunda = comparar(candidato, produccion, conjuntos)
    assert segunda["candidato"] == pytest.approx(segunda["produccion"])
    assert not segunda["desplegar"]
