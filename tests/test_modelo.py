import os

import joblib
import numpy as np
import pytest
from sklearn.metrics import average_precision_score

from entrenamiento.entrenar import entrenar
from entrenamiento.historico import dividir_temporal, generar_dataset, matriz_y_etiquetas
from streaming.modelo.modelo import ModeloFraude
from streaming.modelo.predictor import Predictor


@pytest.fixture(scope="session")
def datos():
    return generar_dataset(clientes=300, dias=0.5, tps_medio=2.0, semilla=21)


@pytest.fixture(scope="session")
def modelo_entrenado(datos):
    entrenamiento, _ = dividir_temporal(datos)
    x, y = matriz_y_etiquetas(entrenamiento)
    return ModeloFraude(n_estimadores=100).entrenar(x, y)


def test_el_historico_tiene_fraude_etiquetado(datos):
    tasa = datos["es_fraude"].mean()
    assert 0.008 < tasa < 0.03
    assert {"prueba_tarjeta", "viaje_imposible", "importe_desproporcionado"} <= set(datos["patron"])


def test_puntua_entre_0_y_1_y_separa_el_fraude(datos, modelo_entrenado):
    _, validacion = dividir_temporal(datos)
    x, y = matriz_y_etiquetas(validacion)
    puntuaciones = modelo_entrenado.puntuar(x)
    assert puntuaciones.min() >= 0 and puntuaciones.max() <= 1
    assert puntuaciones[y == 1].mean() > 5 * puntuaciones[y == 0].mean()
    assert average_precision_score(y, puntuaciones) > 10 * y.mean()


def test_necesita_las_dos_clases():
    with pytest.raises(ValueError):
        ModeloFraude().entrenar(np.zeros((10, 13)), np.zeros(10))


def test_guardar_y_cargar(datos, modelo_entrenado, tmp_path):
    ruta = modelo_entrenado.guardar(tmp_path / "modelo.joblib")
    x, _ = matriz_y_etiquetas(datos.head(50))
    assert np.allclose(ModeloFraude.cargar(ruta).puntuar(x), modelo_entrenado.puntuar(x))
    assert not list(tmp_path.glob("*.tmp"))


def test_cargar_un_fichero_ajeno_falla(tmp_path):
    ruta = tmp_path / "otro.joblib"
    joblib.dump({"no": "es un modelo"}, ruta)
    with pytest.raises(TypeError):
        ModeloFraude.cargar(ruta)


def test_predictor_sin_modelo_solo_deja_las_reglas(tmp_path):
    predictor = Predictor(tmp_path / "no_existe.joblib", intervalo_comprobacion_s=0)
    assert not predictor.disponible
    assert predictor.puntuar([{}, {}]) == [0.0, 0.0]


def test_predictor_recarga_el_modelo_cuando_cambia(datos, modelo_entrenado, tmp_path):
    ruta = tmp_path / "modelo.joblib"
    predictor = Predictor(ruta, intervalo_comprobacion_s=0)
    assert not predictor.disponible
    modelo_entrenado.guardar(ruta)
    assert predictor.disponible
    features = list(datos["features"].head(3))
    assert len(predictor.puntuar(features)) == 3
    estadisticas = ruta.stat()
    os.utime(ruta, (estadisticas.st_atime, estadisticas.st_mtime + 10))
    predictor.puntuar(features)
    assert predictor.version == ruta.stat().st_mtime


def test_entrenamiento_completo_con_metricas_de_validacion():
    modelo, metricas = entrenar(150, 0.3, 2.0, 4, umbral_revision=0.2, umbral_bloqueo=0.5)
    assert metricas["fraudes_validacion"] > 0
    assert 0 <= metricas["revision"]["precision"] <= 1
    assert 0 <= metricas["revision"]["recall"] <= 1
    assert modelo.metadatos["validacion"] is metricas
    assert modelo.metadatos["origen"]["clientes"] == 150
