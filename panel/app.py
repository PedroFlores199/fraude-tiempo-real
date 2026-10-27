import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from pymongo import MongoClient
from pymongo.database import Database

from comun.alertas import FALSO_POSITIVO, FRAUDE_CONFIRMADO, marcar_alerta
from comun.config import cargar_config
from comun.informes import (
    alertas_por_motivo_y_dia,
    falsos_positivos_por_regla,
    precision_por_motivo,
)
from panel.oyente import OyenteAlertas

COLORES_DECISION = {"BLOQUEADA": "#c0392b", "REVISAR": "#e67e22"}
SQL_METRICAS = """
    SELECT fecha, transacciones_por_segundo, porcentaje_bloqueadas,
           latencia_media_ms, latencia_max_ms
    FROM metricas
    WHERE fecha >= NOW() - INTERVAL '1 hour'
    ORDER BY fecha
"""


@st.cache_resource
def base_mongo() -> Database:
    config = cargar_config()
    return MongoClient(config.mongo_uri, tz_aware=True)[config.mongo_db]


@st.cache_resource
def oyente_alertas() -> OyenteAlertas:
    oyente = OyenteAlertas(base_mongo()["alertas"])
    oyente.cargar_recientes()
    oyente.iniciar()
    return oyente


def tabla_alertas(alertas: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": alerta["_id"],
                "fecha": alerta["fecha"],
                "cliente": alerta["cliente_id"],
                "importe (€)": alerta["transaccion"]["importe"],
                "comercio": alerta["transaccion"]["comercio"],
                "ciudad": alerta["transaccion"]["ciudad"],
                "decisión": alerta["decision"],
                "motivos": ", ".join(alerta["motivos"]),
                "puntuación": alerta["puntuacion_modelo"],
                "estado": alerta["estado"],
            }
            for alerta in alertas
        ]
    )


def leer_metricas() -> pd.DataFrame:
    config = cargar_config()
    with psycopg2.connect(config.postgres_dsn) as conexion, conexion.cursor() as cursor:
        cursor.execute(SQL_METRICAS)
        columnas = [columna.name for columna in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columnas)


@st.fragment(run_every="2s")
def seccion_alertas() -> None:
    oyente = oyente_alertas()
    alertas = oyente.instantanea()
    pendientes = [a for a in alertas if a["estado"] == "PENDIENTE"]
    columnas = st.columns(4)
    columnas[0].metric("Alertas en memoria", len(alertas))
    columnas[1].metric("Pendientes de revisar", len(pendientes))
    columnas[2].metric("Bloqueadas", sum(a["decision"] == "BLOQUEADA" for a in alertas))
    columnas[3].metric("Recibidas por change stream", oyente.recibidas)
    if not alertas:
        st.info("Todavía no hay alertas. Aparecerán aquí en cuanto el streaming las genere.")
        return
    st.dataframe(tabla_alertas(alertas), hide_index=True, use_container_width=True)

    st.subheader("Revisión de alertas pendientes")
    if not pendientes:
        st.caption("No hay alertas pendientes.")
        return
    opciones = {a["_id"]: a for a in pendientes}
    seleccion = st.selectbox(
        "Alerta",
        list(opciones),
        format_func=lambda i: (
            f"{opciones[i]['cliente_id']} · {opciones[i]['transaccion']['importe']:.2f} € · "
            f"{', '.join(opciones[i]['motivos'])}"
        ),
        key="alerta_seleccionada",
    )
    izquierda, derecha = st.columns(2)
    if izquierda.button("Confirmar fraude", type="primary", use_container_width=True):
        marcar_alerta(base_mongo()["alertas"], seleccion, FRAUDE_CONFIRMADO)
        st.toast("Alerta marcada como fraude confirmado")
    if derecha.button("Falso positivo", use_container_width=True):
        marcar_alerta(base_mongo()["alertas"], seleccion, FALSO_POSITIVO)
        st.toast("Alerta marcada como falso positivo")


@st.fragment(run_every="5s")
def seccion_mapa() -> None:
    alertas = oyente_alertas().instantanea()
    if not alertas:
        st.info("No hay alertas que mostrar en el mapa.")
        return
    puntos = pd.DataFrame(
        [
            {
                "lat": alerta["transaccion"]["lat"],
                "lon": alerta["transaccion"]["lon"],
                "decisión": alerta["decision"],
                "importe": alerta["transaccion"]["importe"],
                "cliente": alerta["cliente_id"],
                "ciudad": alerta["transaccion"]["ciudad"],
                "motivos": ", ".join(alerta["motivos"]),
            }
            for alerta in alertas
        ]
    )
    figura = px.scatter_map(
        puntos,
        lat="lat",
        lon="lon",
        color="decisión",
        size="importe",
        size_max=25,
        color_discrete_map=COLORES_DECISION,
        hover_data=["cliente", "ciudad", "importe", "motivos"],
        zoom=4.6,
        center={"lat": 40.2, "lon": -3.7},
        map_style="carto-positron",
        height=620,
    )
    st.plotly_chart(figura, use_container_width=True)


@st.fragment(run_every="10s")
def seccion_metricas() -> None:
    metricas = leer_metricas()
    if metricas.empty:
        st.info("Aún no hay métricas en PostgreSQL de la última hora.")
        return
    ultima = metricas.iloc[-1]
    columnas = st.columns(3)
    columnas[0].metric("Transacciones por segundo", f"{ultima.transacciones_por_segundo:.1f}")
    columnas[1].metric("% bloqueadas", f"{ultima.porcentaje_bloqueadas:.2f} %")
    columnas[2].metric("Latencia media", f"{ultima.latencia_media_ms:.0f} ms")
    st.plotly_chart(
        px.line(
            metricas, x="fecha", y="transacciones_por_segundo", title="Transacciones por segundo"
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.line(
            metricas,
            x="fecha",
            y=["latencia_media_ms", "latencia_max_ms"],
            title="Latencia de decisión (ms)",
        ),
        use_container_width=True,
    )


def seccion_informes() -> None:
    alertas = base_mongo()["alertas"]
    st.button("Actualizar informes")
    por_motivo = pd.DataFrame(alertas_por_motivo_y_dia(alertas))
    if por_motivo.empty:
        st.info("No hay alertas para agregar.")
        return
    st.plotly_chart(
        px.bar(por_motivo, x="dia", y="alertas", color="motivo", title="Alertas por motivo y día"),
        use_container_width=True,
    )
    izquierda, derecha = st.columns(2)
    izquierda.subheader("Falsos positivos por regla")
    izquierda.dataframe(
        pd.DataFrame(falsos_positivos_por_regla(alertas)), hide_index=True, use_container_width=True
    )
    derecha.subheader("Precisión frente a la verdad del generador")
    derecha.dataframe(
        pd.DataFrame(precision_por_motivo(alertas)), hide_index=True, use_container_width=True
    )


def main() -> None:
    st.set_page_config(page_title="Detección de fraude", layout="wide")
    st.title("Detección de fraude en tiempo real")
    pestanas = st.tabs(["Alertas en vivo", "Mapa de alertas", "Métricas", "Informes"])
    with pestanas[0]:
        seccion_alertas()
    with pestanas[1]:
        seccion_mapa()
    with pestanas[2]:
        seccion_metricas()
    with pestanas[3]:
        seccion_informes()


if __name__ == "__main__":
    main()
