import threading
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel
from pymongo import DESCENDING, MongoClient
from pymongo.database import Database

from comun.alertas import ESTADOS_REVISION, marcar_alerta
from comun.config import cargar_config
from comun.informes import (
    alertas_por_motivo_y_dia,
    falsos_positivos_por_regla,
    precision_por_motivo,
)

SQL_RESUMEN = """
    SELECT
        COALESCE(SUM(transacciones), 0) AS transacciones,
        COALESCE(AVG(transacciones_por_segundo), 0) AS transacciones_por_segundo,
        COALESCE(AVG(porcentaje_bloqueadas), 0) AS porcentaje_bloqueadas,
        COALESCE(AVG(latencia_media_ms), 0) AS latencia_media_ms,
        COALESCE(MAX(latencia_max_ms), 0) AS latencia_max_ms
    FROM metricas
    WHERE fecha >= NOW() - make_interval(mins => %s)
"""


class CambioEstado(BaseModel):
    estado: str


class ConexionesPostgres:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool = None
        self._candado = threading.Lock()

    def _obtener_pool(self):
        with self._candado:
            if self._pool is None:
                self._pool = ThreadedConnectionPool(1, 5, self._dsn)
            return self._pool

    def consultar_uno(self, sql: str, parametros: tuple) -> dict:
        pool = self._obtener_pool()
        conexion = pool.getconn()
        try:
            with conexion.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, parametros)
                return dict(cursor.fetchone() or {})
        finally:
            conexion.rollback()
            pool.putconn(conexion)

    def cerrar(self) -> None:
        if self._pool is not None:
            self._pool.closeall()


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    config = cargar_config()
    app.state.mongo = MongoClient(config.mongo_uri, tz_aware=True)
    app.state.db = app.state.mongo[config.mongo_db]
    app.state.postgres = ConexionesPostgres(config.postgres_dsn)
    yield
    app.state.postgres.cerrar()
    app.state.mongo.close()


app = FastAPI(
    title="API de detección de fraude",
    description="Consulta y revisión de alertas generadas en tiempo real",
    version="2.0.0",
    lifespan=ciclo_de_vida,
)


def obtener_db(request: Request) -> Database:
    return request.app.state.db


def obtener_postgres(request: Request) -> ConexionesPostgres:
    return request.app.state.postgres


EstadoAlerta = Literal["PENDIENTE", "FRAUDE_CONFIRMADO", "FALSO_POSITIVO"]
BaseDatos = Annotated[Database, Depends(obtener_db)]
Postgres = Annotated[ConexionesPostgres, Depends(obtener_postgres)]


def _alerta_o_404(db: Database, alerta_id: str) -> dict:
    alerta = db["alertas"].find_one({"_id": alerta_id})
    if alerta is None:
        raise HTTPException(status_code=404, detail="Alerta no encontrada")
    return alerta


@app.get("/salud")
def salud() -> dict:
    return {"estado": "ok"}


@app.get("/alertas")
def listar_alertas(
    db: BaseDatos,
    desde: datetime | None = None,
    hasta: datetime | None = None,
    estado: EstadoAlerta | None = None,
    decision: Literal["REVISAR", "BLOQUEADA"] | None = None,
    cliente_id: str | None = None,
    limite: Annotated[int, Query(ge=1, le=500)] = 50,
    salto: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    consulta: dict = {}
    rango = {}
    if desde is not None:
        rango["$gte"] = desde
    if hasta is not None:
        rango["$lte"] = hasta
    if rango:
        consulta["fecha"] = rango
    if estado:
        consulta["estado"] = estado
    if decision:
        consulta["decision"] = decision
    if cliente_id:
        consulta["cliente_id"] = cliente_id
    coleccion = db["alertas"]
    cursor = (
        coleccion.find(consulta, {"perfil_cliente": 0, "features": 0})
        .sort("fecha", DESCENDING)
        .skip(salto)
        .limit(limite)
    )
    return {
        "total": coleccion.count_documents(consulta),
        "limite": limite,
        "salto": salto,
        "alertas": list(cursor),
    }


@app.get("/alertas/{alerta_id}")
def obtener_alerta(alerta_id: str, db: BaseDatos) -> dict:
    return _alerta_o_404(db, alerta_id)


@app.patch("/alertas/{alerta_id}/estado")
def cambiar_estado(alerta_id: str, cambio: CambioEstado, db: BaseDatos) -> dict:
    if cambio.estado not in ESTADOS_REVISION:
        raise HTTPException(
            status_code=422,
            detail=f"Estado no válido. Valores permitidos: {', '.join(ESTADOS_REVISION)}",
        )
    if not marcar_alerta(db["alertas"], alerta_id, cambio.estado):
        raise HTTPException(status_code=404, detail="Alerta no encontrada")
    return _alerta_o_404(db, alerta_id)


@app.get("/clientes/{cliente_id}")
def obtener_cliente(cliente_id: str, db: BaseDatos) -> dict:
    cliente = db["clientes"].find_one({"_id": cliente_id})
    if cliente is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    return cliente


@app.get("/informes/alertas-por-motivo")
def informe_alertas_por_motivo(
    db: BaseDatos,
    desde: datetime | None = None,
    hasta: datetime | None = None,
) -> list[dict]:
    return alertas_por_motivo_y_dia(db["alertas"], desde, hasta)


@app.get("/informes/falsos-positivos")
def informe_falsos_positivos(db: BaseDatos) -> list[dict]:
    return falsos_positivos_por_regla(db["alertas"])


@app.get("/informes/precision")
def informe_precision(db: BaseDatos) -> list[dict]:
    return precision_por_motivo(db["alertas"])


@app.get("/metricas/resumen")
def resumen_metricas(
    postgres: Postgres,
    minutos: Annotated[int, Query(ge=1, le=1440)] = 60,
) -> dict:
    fila = postgres.consultar_uno(SQL_RESUMEN, (minutos,))
    return {clave: float(valor) for clave, valor in fila.items()}
