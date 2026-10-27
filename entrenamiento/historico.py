from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from comun.features import calcular_features
from comun.tiempo import hora_local
from generador.perfiles import generar_perfiles
from generador.transacciones import Evento, GeneradorTransacciones
from streaming.enriquecimiento.estado_tarjetas import EstadoTarjetasMemoria
from streaming.modelo.modelo import matriz

INICIO_HISTORICO = datetime(2026, 9, 7, tzinfo=UTC)
PASO_S = 60
ACTIVIDAD_POR_HORA = (
    0.15,
    0.1,
    0.08,
    0.08,
    0.1,
    0.2,
    0.4,
    0.7,
    0.9,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    0.95,
    0.9,
    0.9,
    0.95,
    1.0,
    1.0,
    0.9,
    0.7,
    0.45,
    0.25,
)


def simular_eventos(
    perfiles: list[dict],
    horas: float,
    tps_medio: float,
    semilla: int,
    tasa_fraude: float = 0.015,
    intervalo_minimo_s: float = 15.0,
    inicio: datetime = INICIO_HISTORICO,
) -> list[Evento]:
    generador = GeneradorTransacciones(
        perfiles,
        tasa_fraude=tasa_fraude,
        intervalo_minimo_s=intervalo_minimo_s,
        semilla=semilla,
    )
    pico = tps_medio * len(ACTIVIDAD_POR_HORA) / sum(ACTIVIDAD_POR_HORA)
    eventos: list[Evento] = []
    for paso in range(int(horas * 3600 / PASO_S)):
        momento = inicio + timedelta(seconds=paso * PASO_S)
        tps = pico * ACTIVIDAD_POR_HORA[hora_local(momento)]
        eventos.extend(generador.generar(momento, PASO_S, tps))
    return sorted(eventos, key=lambda evento: evento.enviar_en)


def construir_dataset(perfiles: list[dict], eventos: list[Evento]) -> pd.DataFrame:
    por_id = {perfil["_id"]: perfil for perfil in perfiles}
    estado = EstadoTarjetasMemoria()
    filas = []
    for evento in eventos:
        transaccion = evento.transaccion
        perfil = por_id.get(transaccion["cliente_id"])
        previo = estado.leer(
            transaccion["tarjeta_id"], evento.fecha_hora, transaccion["id_transaccion"]
        )
        features = calcular_features(transaccion, previo, perfil)
        estado.registrar(transaccion)
        filas.append(
            {
                "id_transaccion": transaccion["id_transaccion"],
                "fecha_hora": evento.fecha_hora,
                "es_fraude": int(evento.es_fraude),
                "patron": evento.patron or "normal",
                "transaccion": transaccion,
                "perfil": perfil,
                "features": features,
            }
        )
    return pd.DataFrame(filas)


def generar_dataset(
    clientes: int,
    dias: float,
    tps_medio: float,
    semilla: int,
    semilla_perfiles: int = 42,
) -> pd.DataFrame:
    perfiles = generar_perfiles(clientes, semilla_perfiles)
    eventos = simular_eventos(perfiles, dias * 24, tps_medio, semilla)
    return construir_dataset(perfiles, eventos)


def dividir_temporal(
    datos: pd.DataFrame, fraccion: float = 0.8
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordenados = datos.sort_values("fecha_hora").reset_index(drop=True)
    corte = int(len(ordenados) * fraccion)
    return ordenados.iloc[:corte], ordenados.iloc[corte:]


def matriz_y_etiquetas(datos: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    return matriz(list(datos["features"])), datos["es_fraude"].to_numpy(dtype=int)
