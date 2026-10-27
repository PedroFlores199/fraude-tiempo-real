from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "fraude",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="reentrenar_modelo_fraude",
    description="Reentreno nocturno del modelo con el feedback de los analistas",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Madrid"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"clientes": 1000, "dias": 2.0, "tps_medio": 1.0},
    tags=["ml", "fraude"],
)
def reentrenar_modelo_fraude():
    @task
    def exportar_feedback(ds_nodash: str | None = None) -> str:
        from pymongo import MongoClient

        from comun.config import cargar_config
        from entrenamiento.reentreno import exportar_feedback as exportar

        config = cargar_config()
        destino = Path(config.modelo_ruta).parent / "feedback" / f"feedback_{ds_nodash}.jsonl"
        with MongoClient(config.mongo_uri, tz_aware=True) as cliente:
            exportar(cliente[config.mongo_db]["alertas"], destino)
        return str(destino)

    @task
    def entrenar_y_comparar(ruta_feedback: str, ds_nodash: str | None = None, params=None) -> dict:
        from comun.config import cargar_config
        from entrenamiento.reentreno import (
            cargar_feedback,
            comparar,
            entrenar_candidato,
            preparar_conjuntos,
        )

        config = cargar_config()
        semilla = int(ds_nodash)
        conjuntos = preparar_conjuntos(
            cargar_feedback(Path(ruta_feedback)),
            int(params["clientes"]),
            float(params["dias"]),
            float(params["tps_medio"]),
            semilla,
        )
        produccion = Path(config.modelo_ruta)
        candidato = entrenar_candidato(
            conjuntos, produccion.parent / "candidatos" / f"modelo_{ds_nodash}.joblib", semilla
        )
        return {"ruta_candidato": str(candidato), **comparar(candidato, produccion, conjuntos)}

    @task.short_circuit
    def mejora_produccion(resultado: dict) -> bool:
        return bool(resultado["desplegar"])

    @task
    def desplegar(resultado: dict) -> str:
        from comun.config import cargar_config
        from entrenamiento.reentreno import desplegar as desplegar_modelo

        config = cargar_config()
        return str(desplegar_modelo(Path(resultado["ruta_candidato"]), Path(config.modelo_ruta)))

    resultado = entrenar_y_comparar(exportar_feedback())
    mejora_produccion(resultado) >> desplegar(resultado)


reentrenar_modelo_fraude()
