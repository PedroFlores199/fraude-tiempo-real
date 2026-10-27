from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from api.main import app, obtener_db, obtener_postgres

DIA = datetime(2026, 10, 27, 10, tzinfo=UTC)


class PostgresFalso:
    def __init__(self) -> None:
        self.parametros = None

    def consultar_uno(self, sql: str, parametros: tuple) -> dict:
        self.parametros = parametros
        return {"transacciones": 6000, "transacciones_por_segundo": 100.0, "latencia_media_ms": 850}

    def cerrar(self) -> None:
        return None


@pytest.fixture
def postgres():
    return PostgresFalso()


@pytest.fixture
def cliente(mongo, perfil, postgres):
    mongo["clientes"].insert_one(perfil)
    mongo["alertas"].insert_many(
        [
            {
                "_id": f"alerta-{i}",
                "cliente_id": "CLI-00001",
                "fecha": DIA + timedelta(hours=i),
                "decision": "BLOQUEADA" if i % 2 else "REVISAR",
                "motivos": ["mas_de_300_km_en_menos_de_30_min"],
                "estado": "PENDIENTE",
                "perfil_cliente": perfil,
                "features": {"importe": 10.0},
                "transaccion": {"importe": 10.0},
            }
            for i in range(5)
        ]
    )
    app.dependency_overrides[obtener_db] = lambda: mongo
    app.dependency_overrides[obtener_postgres] = lambda: postgres
    with TestClient(app) as cliente_http:
        yield cliente_http
    app.dependency_overrides.clear()


def test_salud(cliente):
    assert cliente.get("/salud").json() == {"estado": "ok"}


def test_listado_paginado_y_ordenado_por_fecha(cliente):
    respuesta = cliente.get("/alertas", params={"limite": 2, "salto": 1})
    cuerpo = respuesta.json()
    assert respuesta.status_code == 200
    assert cuerpo["total"] == 5
    assert [a["_id"] for a in cuerpo["alertas"]] == ["alerta-3", "alerta-2"]
    assert "perfil_cliente" not in cuerpo["alertas"][0]
    assert "features" not in cuerpo["alertas"][0]


def test_filtros_por_fecha_y_decision(cliente):
    respuesta = cliente.get(
        "/alertas",
        params={
            "desde": (DIA + timedelta(hours=1)).isoformat(),
            "hasta": (DIA + timedelta(hours=3)).isoformat(),
            "decision": "BLOQUEADA",
        },
    )
    assert [a["_id"] for a in respuesta.json()["alertas"]] == ["alerta-3", "alerta-1"]


def test_filtro_con_valor_no_permitido(cliente):
    assert cliente.get("/alertas", params={"estado": "OTRO"}).status_code == 422


def test_detalle_de_alerta(cliente):
    assert cliente.get("/alertas/alerta-0").json()["perfil_cliente"]["_id"] == "CLI-00001"
    assert cliente.get("/alertas/no-existe").status_code == 404


def test_marcar_falso_positivo(cliente):
    respuesta = cliente.patch("/alertas/alerta-1/estado", json={"estado": "FALSO_POSITIVO"})
    assert respuesta.status_code == 200
    assert respuesta.json()["estado"] == "FALSO_POSITIVO"
    assert respuesta.json()["descartada_en"]


def test_marcar_con_estado_no_valido(cliente):
    respuesta = cliente.patch("/alertas/alerta-1/estado", json={"estado": "PENDIENTE"})
    assert respuesta.status_code == 422
    assert "Estado no válido" in respuesta.json()["detail"]


def test_marcar_alerta_inexistente(cliente):
    respuesta = cliente.patch("/alertas/nada/estado", json={"estado": "FRAUDE_CONFIRMADO"})
    assert respuesta.status_code == 404


def test_perfil_de_cliente(cliente):
    assert cliente.get("/clientes/CLI-00001").json()["ciudad"] == "Madrid"
    assert cliente.get("/clientes/CLI-99999").status_code == 404


def test_informes(cliente):
    motivos = cliente.get("/informes/alertas-por-motivo").json()
    assert sum(fila["alertas"] for fila in motivos) == 5
    cliente.patch("/alertas/alerta-1/estado", json={"estado": "FALSO_POSITIVO"})
    falsos = cliente.get("/informes/falsos-positivos").json()
    assert falsos[0]["porcentaje_falsos_positivos"] == 100.0
    assert cliente.get("/informes/precision").json()[0]["alertas"] == 5


def test_resumen_de_metricas(cliente, postgres):
    respuesta = cliente.get("/metricas/resumen", params={"minutos": 15})
    assert respuesta.json()["transacciones_por_segundo"] == 100.0
    assert postgres.parametros == (15,)
