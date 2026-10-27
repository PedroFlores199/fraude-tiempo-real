import threading
import time
from datetime import UTC, datetime, timedelta

from panel.oyente import OyenteAlertas

DIA = datetime(2026, 10, 27, 10, tzinfo=UTC)


def alerta(indice: int, **cambios) -> dict:
    return {
        "_id": f"a{indice}",
        "fecha": DIA + timedelta(minutes=indice),
        "estado": "PENDIENTE",
        "perfil_cliente": {"_id": "CLI-00001"},
        "features": {},
        **cambios,
    }


class FlujoFalso:
    def __init__(self, cambios: list[dict], parar: threading.Event) -> None:
        self._cambios = list(cambios)
        self._parar = parar
        self.alive = True

    def __enter__(self):
        return self

    def __exit__(self, *excepcion):
        return False

    def try_next(self):
        if self._cambios:
            return self._cambios.pop(0)
        self._parar.wait(0.05)
        return None


class ColeccionFalsa:
    def __init__(self, cambios: list[dict]) -> None:
        self.cambios = cambios
        self.argumentos = None
        self.parar = threading.Event()

    def watch(self, pipeline, **opciones):
        self.argumentos = (pipeline, opciones)
        cambios, self.cambios = self.cambios, []
        return FlujoFalso(cambios, self.parar)


def test_carga_las_alertas_recientes_ordenadas(mongo):
    mongo["alertas"].insert_many([alerta(i) for i in range(5)])
    oyente = OyenteAlertas(mongo["alertas"], capacidad=3)
    assert oyente.cargar_recientes() == 3
    instantanea = oyente.instantanea()
    assert [a["_id"] for a in instantanea] == ["a4", "a3", "a2"]
    assert "perfil_cliente" not in instantanea[0]


def test_aplica_inserciones_y_actualizaciones(mongo):
    oyente = OyenteAlertas(mongo["alertas"], capacidad=2)
    oyente.aplicar({"_id": "t1", "operationType": "insert", "fullDocument": alerta(1)})
    oyente.aplicar({"_id": "t2", "operationType": "insert", "fullDocument": alerta(2)})
    oyente.aplicar(
        {"_id": "t3", "operationType": "update", "fullDocument": alerta(1, estado="FALSO_POSITIVO")}
    )
    oyente.aplicar({"_id": "t4", "operationType": "insert", "fullDocument": alerta(3)})
    assert {a["_id"]: a["estado"] for a in oyente.instantanea()} == {
        "a3": "PENDIENTE",
        "a1": "FALSO_POSITIVO",
    }
    assert oyente.recibidas == 4


def test_escucha_el_change_stream_en_segundo_plano():
    coleccion = ColeccionFalsa(
        [
            {"_id": "t1", "operationType": "insert", "fullDocument": alerta(1)},
            {"_id": "t2", "operationType": "insert", "fullDocument": alerta(2)},
        ]
    )
    oyente = OyenteAlertas(coleccion)
    oyente.iniciar()
    limite = time.monotonic() + 5
    while oyente.recibidas < 2 and time.monotonic() < limite:
        time.sleep(0.01)
    oyente.detener()
    coleccion.parar.set()
    assert oyente.recibidas == 2
    pipeline, opciones = coleccion.argumentos
    assert opciones["full_document"] == "updateLookup"
    assert pipeline[0]["$match"]["operationType"]["$in"] == ["insert", "update", "replace"]
