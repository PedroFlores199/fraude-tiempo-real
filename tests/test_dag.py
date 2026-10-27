from pathlib import Path

import pytest

DagBag = pytest.importorskip("airflow.models").DagBag

CARPETA_DAGS = Path(__file__).resolve().parent.parent / "dags"


def test_el_dag_de_reentreno_carga_sin_errores():
    bolsa = DagBag(dag_folder=str(CARPETA_DAGS), include_examples=False)
    assert bolsa.import_errors == {}
    dag = bolsa.dags["reentrenar_modelo_fraude"]
    assert dag.schedule_interval == "0 3 * * *"
    assert {tarea.task_id for tarea in dag.tasks} == {
        "exportar_feedback",
        "entrenar_y_comparar",
        "mejora_produccion",
        "desplegar",
    }
    assert all(tarea.retries == 2 for tarea in dag.tasks)
