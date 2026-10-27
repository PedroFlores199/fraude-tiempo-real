import pytest

from entrenamiento.evaluar import comparar, decisiones, informe, recall_por_patron
from entrenamiento.historico import dividir_temporal, generar_dataset, matriz_y_etiquetas
from streaming.modelo.modelo import ModeloFraude


@pytest.fixture(scope="module")
def resultado():
    datos = generar_dataset(clientes=300, dias=0.5, tps_medio=2.0, semilla=31)
    entrenamiento, validacion = dividir_temporal(datos)
    modelo = ModeloFraude(n_estimadores=100).entrenar(*matriz_y_etiquetas(entrenamiento))
    x, _ = matriz_y_etiquetas(validacion)
    return decisiones(validacion, modelo.puntuar(x), umbral_revision=0.2, umbral_bloqueo=0.5)


def test_compara_las_tres_estrategias(resultado):
    tabla = comparar(resultado).set_index("estrategia")
    assert list(tabla.index) == ["solo_reglas", "solo_modelo", "reglas_y_modelo"]
    combinado = tabla.loc["reglas_y_modelo"]
    assert combinado["recall"] >= tabla.loc["solo_reglas", "recall"]
    assert combinado["recall"] >= tabla.loc["solo_modelo", "recall"]
    assert tabla.loc["solo_reglas", "precision"] > 0.5


def test_recall_por_patron_y_tabla_markdown(resultado):
    por_patron = recall_por_patron(resultado)
    assert set(por_patron["patron"]) >= {"prueba_tarjeta", "viaje_imposible"}
    texto = informe(comparar(resultado), por_patron, len(resultado), 10, {"semilla": 31})
    assert "| estrategia |" in texto
    assert "## Recall por patrón de fraude" in texto
