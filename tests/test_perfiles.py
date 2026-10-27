import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from generador.catalogo import CATEGORIA_DE_COMERCIO
from generador.perfiles import (
    asegurar_poblacion,
    cargar_perfiles,
    generar_perfil,
    generar_perfiles,
)

ESQUEMA_CLIENTES = Path(__file__).resolve().parent.parent / "mongo" / "esquemas" / "clientes.json"


@pytest.fixture(scope="module")
def perfiles() -> list[dict]:
    return generar_perfiles(1000)


@pytest.fixture(scope="module")
def validador() -> Draft202012Validator:
    return Draft202012Validator(json.loads(ESQUEMA_CLIENTES.read_text(encoding="utf-8")))


def test_genera_mil_clientes_con_identificadores_unicos(perfiles):
    assert len(perfiles) == 1000
    assert len({p["_id"] for p in perfiles}) == 1000
    tarjetas = [t["tarjeta_id"] for p in perfiles for t in p["tarjetas"]]
    assert len(tarjetas) == len(set(tarjetas))


def test_la_generacion_es_determinista():
    assert generar_perfil(7) == generar_perfil(7)
    assert generar_perfil(7) != generar_perfil(8)


def test_hay_particulares_y_empresas_con_campos_distintos(perfiles):
    empresas = [p for p in perfiles if p["tipo"] == "empresa"]
    particulares = [p for p in perfiles if p["tipo"] == "particular"]
    assert empresas and particulares
    assert all({"razon_social", "cif", "sector", "num_empleados"} <= set(p) for p in empresas)
    assert all({"nombre", "apellidos", "edad", "ocupacion"} <= set(p) for p in particulares)
    assert not any("cif" in p for p in particulares)


def test_datos_anidados_coherentes(perfiles):
    for perfil in perfiles:
        assert perfil["categorias"]
        assert all(c["gasto_medio"] > 0 for c in perfil["categorias"].values())
        assert all(
            CATEGORIA_DE_COMERCIO[comercio] in perfil["categorias"]
            for comercio in perfil["comercios_frecuentes"]
        )
        horario = perfil["horario_tipico"]
        assert 0 <= horario["inicio"] < horario["fin"] <= 24


def test_todos_los_perfiles_cumplen_el_json_schema_de_mongo(perfiles, validador):
    errores = [e.message for p in perfiles for e in validador.iter_errors(p)]
    assert errores == []


def test_el_json_schema_rechaza_una_empresa_sin_cif(validador):
    empresa = next(p for p in generar_perfiles(200) if p["tipo"] == "empresa")
    del empresa["cif"]
    assert not validador.is_valid(empresa)


def test_el_json_schema_rechaza_un_particular_sin_edad(validador):
    particular = next(p for p in generar_perfiles(50) if p["tipo"] == "particular")
    del particular["edad"]
    assert not validador.is_valid(particular)


def test_carga_idempotente_y_ampliacion_de_poblacion(mongo):
    coleccion = mongo["clientes"]
    cargar_perfiles(coleccion, generar_perfiles(10))
    cargar_perfiles(coleccion, generar_perfiles(10))
    assert coleccion.count_documents({}) == 10
    assert asegurar_poblacion(coleccion, 25) == 15
    assert coleccion.count_documents({}) == 25
    guardado = coleccion.find_one({"_id": "CLI-00024"})
    esperado = generar_perfil(24)
    assert guardado["fecha_alta"] == esperado.pop("fecha_alta")
    assert {k: v for k, v in guardado.items() if k != "fecha_alta"} == esperado
    assert asegurar_poblacion(coleccion, 20) == 0
