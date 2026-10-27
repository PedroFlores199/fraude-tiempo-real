import random
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from comun.geo import distancia_km
from comun.tiempo import a_iso, desde_iso, hora_local
from generador.catalogo import (
    CANAL_DE_COMERCIO,
    CATEGORIA_DE_COMERCIO,
    CATEGORIAS_RARAS,
    CIUDADES,
    COMERCIOS,
)

VIAJE_IMPOSIBLE = "viaje_imposible"
PRUEBA_TARJETA = "prueba_tarjeta"
IMPORTE_DESPROPORCIONADO = "importe_desproporcionado"
MADRUGADA_INUSUAL = "madrugada_inusual"

PATRONES_DIURNOS = (VIAJE_IMPOSIBLE, PRUEBA_TARJETA, IMPORTE_DESPROPORCIONADO)
HORAS_MADRUGADA = range(0, 6)
DISTANCIA_MINIMA_VIAJE_KM = 350.0
BLOQUEO_TRAS_FRAUDE_S = 900
INTENTOS_ELECCION = 25
RETRASO_MINIMO_S = 5.0
RETRASO_MAXIMO_S = 150.0


@dataclass(frozen=True)
class Evento:
    transaccion: dict
    fecha_hora: datetime
    enviar_en: datetime
    es_fraude: bool
    patron: str | None = None


class GeneradorTransacciones:
    def __init__(
        self,
        perfiles: list[dict],
        tasa_fraude: float = 0.015,
        tasa_retraso: float = 0.03,
        intervalo_minimo_s: float = 15.0,
        semilla: int | None = None,
    ) -> None:
        if not perfiles:
            raise ValueError("Se necesita al menos un perfil de cliente")
        self._rng = random.Random(semilla)
        self._tasa_fraude = tasa_fraude
        self._tasa_retraso = tasa_retraso
        self._intervalo = timedelta(seconds=intervalo_minimo_s)
        self._tarjetas = [
            (perfil, tarjeta)
            for perfil in perfiles
            for tarjeta in perfil["tarjetas"]
            if tarjeta.get("activa", True)
        ]
        self._libre_desde: dict[str, datetime] = {}
        self._pendiente_normal = 0.0
        self._deuda_fraude = 0.0
        self.saturado = False

    @property
    def num_tarjetas(self) -> int:
        return len(self._tarjetas)

    @property
    def capacidad_tps(self) -> float:
        return self.num_tarjetas / self._intervalo.total_seconds()

    def generar(self, inicio: datetime, duracion_s: float, tps: float) -> list[Evento]:
        objetivo = tps * duracion_s
        self._deuda_fraude += objetivo * self._tasa_fraude
        eventos: list[Evento] = []
        while self._deuda_fraude >= 1.0:
            momento = inicio + timedelta(seconds=self._rng.uniform(0, duracion_s))
            incidente = self._incidente(momento)
            self._deuda_fraude -= sum(1 for evento in incidente if evento.es_fraude)
            eventos.extend(incidente)

        self._pendiente_normal += objetivo - len(eventos)
        normales = int(self._pendiente_normal)
        self._pendiente_normal -= normales
        desplazamientos = sorted(self._rng.uniform(0, duracion_s) for _ in range(normales))
        for desplazamiento in desplazamientos:
            eventos.append(self._normal(inicio + timedelta(seconds=desplazamiento)))

        return [self._con_retraso(evento) for evento in eventos]

    def _con_retraso(self, evento: Evento) -> Evento:
        if self._rng.random() >= self._tasa_retraso:
            return evento
        retraso = timedelta(seconds=self._rng.uniform(RETRASO_MINIMO_S, RETRASO_MAXIMO_S))
        return replace(evento, enviar_en=evento.fecha_hora + retraso)

    def _elegir_tarjeta(self, momento: datetime, solo_en_horario: bool = True):
        hora = hora_local(momento)
        candidata = None
        for intento in range(INTENTOS_ELECCION):
            perfil, tarjeta = self._rng.choice(self._tarjetas)
            if self._libre_desde.get(tarjeta["tarjeta_id"], momento) > momento:
                continue
            candidata = (perfil, tarjeta)
            horario = perfil["horario_tipico"]
            en_horario = horario["inicio"] <= hora < horario["fin"]
            if en_horario or not solo_en_horario or intento >= INTENTOS_ELECCION // 2:
                return candidata
        if candidata is not None:
            return candidata
        self.saturado = True
        return self._rng.choice(self._tarjetas)

    def _ocupar(self, tarjeta: dict, hasta: datetime) -> None:
        actual = self._libre_desde.get(tarjeta["tarjeta_id"])
        if actual is None or hasta > actual:
            self._libre_desde[tarjeta["tarjeta_id"]] = hasta

    def _posicion(self, ciudad: str) -> tuple[float, float]:
        lat, lon = CIUDADES[ciudad]
        return (
            round(lat + self._rng.uniform(-0.03, 0.03), 6),
            round(lon + self._rng.uniform(-0.03, 0.03), 6),
        )

    def _comercio_habitual(self, perfil: dict, categoria: str) -> str:
        frecuentes = [
            comercio
            for comercio in perfil["comercios_frecuentes"]
            if CATEGORIA_DE_COMERCIO.get(comercio) == categoria
        ]
        if frecuentes and self._rng.random() < 0.7:
            return self._rng.choice(frecuentes)
        return self._rng.choice(list(COMERCIOS[categoria]))

    def _categoria_habitual(self, perfil: dict) -> str:
        categorias = list(perfil["categorias"])
        pesos = [perfil["categorias"][c]["compras_mes"] for c in categorias]
        return self._rng.choices(categorias, weights=pesos)[0]

    def _transaccion(
        self,
        perfil: dict,
        tarjeta: dict,
        momento: datetime,
        comercio: str,
        importe: float,
        ciudad: str | None = None,
    ) -> dict:
        ciudad = ciudad or perfil["ciudad"]
        lat, lon = self._posicion(ciudad)
        return {
            "id_transaccion": str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            "cliente_id": perfil["_id"],
            "tarjeta_id": tarjeta["tarjeta_id"],
            "importe": round(max(0.5, min(importe, 99_999.0)), 2),
            "moneda": "EUR",
            "comercio": comercio,
            "categoria": CATEGORIA_DE_COMERCIO[comercio],
            "canal": CANAL_DE_COMERCIO[comercio],
            "ciudad": ciudad,
            "lat": lat,
            "lon": lon,
            "fecha_hora": a_iso(momento),
        }

    def _evento(self, transaccion: dict, es_fraude: bool, patron: str | None) -> Evento:
        fecha = desde_iso(transaccion["fecha_hora"])
        return Evento(transaccion, fecha, fecha, es_fraude, patron)

    def _importe_normal(self, perfil: dict, categoria: str) -> float:
        media = perfil["categorias"][categoria]["gasto_medio"]
        return self._rng.gauss(media, media * 0.3)

    def _normal(self, momento: datetime) -> Evento:
        perfil, tarjeta = self._elegir_tarjeta(momento)
        self._ocupar(tarjeta, momento + self._intervalo)
        categoria = self._categoria_habitual(perfil)
        comercio = self._comercio_habitual(perfil, categoria)
        transaccion = self._transaccion(
            perfil, tarjeta, momento, comercio, self._importe_normal(perfil, categoria)
        )
        return self._evento(transaccion, False, None)

    def _incidente(self, momento: datetime) -> list[Evento]:
        patrones = list(PATRONES_DIURNOS)
        if hora_local(momento) in HORAS_MADRUGADA:
            patrones.append(MADRUGADA_INUSUAL)
        patron = self._rng.choice(patrones)
        perfil, tarjeta = self._elegir_tarjeta(momento, solo_en_horario=False)
        if patron == VIAJE_IMPOSIBLE:
            eventos = self._viaje_imposible(perfil, tarjeta, momento)
        elif patron == PRUEBA_TARJETA:
            eventos = self._prueba_tarjeta(perfil, tarjeta, momento)
        elif patron == IMPORTE_DESPROPORCIONADO:
            eventos = self._importe_desproporcionado(perfil, tarjeta, momento)
        else:
            eventos = self._madrugada(perfil, tarjeta, momento)
        ultimo = max(evento.fecha_hora for evento in eventos)
        self._ocupar(tarjeta, ultimo + timedelta(seconds=BLOQUEO_TRAS_FRAUDE_S))
        return eventos

    def _viaje_imposible(self, perfil: dict, tarjeta: dict, momento: datetime) -> list[Evento]:
        origen_lat, origen_lon = CIUDADES[perfil["ciudad"]]
        lejanas = [
            ciudad
            for ciudad, (lat, lon) in CIUDADES.items()
            if distancia_km(origen_lat, origen_lon, lat, lon) > DISTANCIA_MINIMA_VIAJE_KM
        ]
        destino = self._rng.choice(lejanas)
        categoria = self._categoria_habitual(perfil)
        primera = self._transaccion(
            perfil,
            tarjeta,
            momento,
            self._comercio_habitual(perfil, categoria),
            self._importe_normal(perfil, categoria),
        )
        segundo_momento = momento + timedelta(minutes=self._rng.uniform(3, 20))
        categoria_fraude = self._rng.choice(list(perfil["categorias"]))
        comercios_presenciales = [
            comercio
            for comercio, canal in COMERCIOS[categoria_fraude].items()
            if canal == "presencial"
        ] or list(COMERCIOS["moda"])
        segunda = self._transaccion(
            perfil,
            tarjeta,
            segundo_momento,
            self._rng.choice(comercios_presenciales),
            self._importe_normal(perfil, categoria_fraude) * self._rng.uniform(1.0, 3.0),
            ciudad=destino,
        )
        return [
            self._evento(primera, False, None),
            self._evento(segunda, True, VIAJE_IMPOSIBLE),
        ]

    def _prueba_tarjeta(self, perfil: dict, tarjeta: dict, momento: datetime) -> list[Evento]:
        comercios_online = [
            comercio
            for comercio, canal in CANAL_DE_COMERCIO.items()
            if canal == "online" and comercio not in perfil["comercios_frecuentes"]
        ]
        comercio = self._rng.choice(comercios_online)
        cantidad = self._rng.randint(6, 10)
        desplazamientos = sorted(self._rng.uniform(0, 45) for _ in range(cantidad))
        eventos = []
        for desplazamiento in desplazamientos:
            instante = momento + timedelta(seconds=desplazamiento)
            transaccion = self._transaccion(
                perfil, tarjeta, instante, comercio, self._rng.uniform(0.5, 3.0)
            )
            eventos.append(self._evento(transaccion, True, PRUEBA_TARJETA))
        return eventos

    def _importe_desproporcionado(
        self, perfil: dict, tarjeta: dict, momento: datetime
    ) -> list[Evento]:
        categoria = self._categoria_habitual(perfil)
        media = perfil["categorias"][categoria]["gasto_medio"]
        transaccion = self._transaccion(
            perfil,
            tarjeta,
            momento,
            self._comercio_habitual(perfil, categoria),
            media * self._rng.uniform(10.0, 14.0),
        )
        return [self._evento(transaccion, True, IMPORTE_DESPROPORCIONADO)]

    def _madrugada(self, perfil: dict, tarjeta: dict, momento: datetime) -> list[Evento]:
        raras = [c for c in CATEGORIAS_RARAS if c not in perfil["categorias"]]
        categoria = self._rng.choice(raras)
        comercios = [
            comercio
            for comercio in COMERCIOS[categoria]
            if comercio not in perfil["comercios_frecuentes"]
        ]
        transaccion = self._transaccion(
            perfil,
            tarjeta,
            momento,
            self._rng.choice(comercios),
            max(20.0, self._rng.gauss(150.0, 60.0)),
        )
        return [self._evento(transaccion, True, MADRUGADA_INUSUAL)]
