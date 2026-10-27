import argparse
import heapq
import itertools
import json
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from comun.config import cargar_config
from comun.esquema import errores_validacion
from comun.registro import configurar_logging
from comun.tiempo import ZONA_LOCAL, ahora_utc
from generador.transacciones import Evento, GeneradorTransacciones

logger = logging.getLogger(__name__)

TICK_S = 0.1
INTERVALO_INFORME_S = 10.0
CONFIG_KAFKA = {
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
    "compression.type": "lz4",
}


class AlmacenEtiquetas:
    def __init__(self, coleccion: Collection) -> None:
        self._coleccion = coleccion

    def guardar(self, eventos: list[Evento]) -> None:
        documentos = [
            {
                "_id": evento.transaccion["id_transaccion"],
                "es_fraude": True,
                "patron": evento.patron,
                "tarjeta_id": evento.transaccion["tarjeta_id"],
                "fecha": evento.fecha_hora,
            }
            for evento in eventos
            if evento.es_fraude
        ]
        if not documentos:
            return
        try:
            self._coleccion.insert_many(documentos, ordered=False)
        except BulkWriteError as error:
            duplicados = [e for e in error.details["writeErrors"] if e["code"] == 11000]
            if len(duplicados) != len(error.details["writeErrors"]):
                raise


class PlanificadorEnvios:
    def __init__(self) -> None:
        self._cola: list[tuple[datetime, int, Evento]] = []
        self._secuencia = itertools.count()

    def __len__(self) -> int:
        return len(self._cola)

    def agregar(self, eventos: list[Evento]) -> None:
        for evento in eventos:
            heapq.heappush(self._cola, (evento.enviar_en, next(self._secuencia), evento))

    def extraer_listos(self, ahora: datetime) -> list[Evento]:
        listos = []
        while self._cola and self._cola[0][0] <= ahora:
            listos.append(heapq.heappop(self._cola)[2])
        return listos

    def extraer_ocurridos(self, ahora: datetime) -> tuple[list[Evento], int]:
        pendientes = [entrada[2] for entrada in sorted(self._cola)]
        self._cola.clear()
        ocurridos = [evento for evento in pendientes if evento.fecha_hora <= ahora]
        return ocurridos, len(pendientes) - len(ocurridos)


class ProductorTransacciones:
    def __init__(
        self,
        productor,
        topico: str,
        etiquetas: AlmacenEtiquetas | None = None,
        tasa_mensajes_rotos: float = 0.0,
        observador: Callable[[Evento, float], None] | None = None,
        semilla: int | None = None,
    ) -> None:
        self._productor = productor
        self._topico = topico
        self._etiquetas = etiquetas
        self._tasa_rotos = tasa_mensajes_rotos
        self._observador = observador
        self._rng = random.Random(semilla)
        self.enviadas = 0
        self.fraudes = 0
        self.rotas = 0
        self.rechazadas = 0

    def _informar_entrega(self, error, mensaje) -> None:
        if error is not None:
            logger.error("No se pudo entregar el mensaje a Kafka: %s", error)

    def _valor(self, transaccion: dict) -> tuple[bytes | None, bool]:
        texto = json.dumps(transaccion, ensure_ascii=False)
        if self._tasa_rotos and self._rng.random() < self._tasa_rotos:
            self.rotas += 1
            return texto[: self._rng.randint(1, len(texto) - 1)].encode("utf-8"), False
        errores = errores_validacion(transaccion)
        if errores:
            self.rechazadas += 1
            logger.error(
                "Transacción %s descartada: %s", transaccion.get("id_transaccion"), errores
            )
            return None, False
        return texto.encode("utf-8"), True

    def enviar(self, eventos: list[Evento], reloj: Callable[[], float] = time.time) -> None:
        intactos = []
        for evento in eventos:
            valor, intacto = self._valor(evento.transaccion)
            if valor is None:
                continue
            instante = reloj()
            self._productor.produce(
                self._topico,
                key=evento.transaccion["tarjeta_id"].encode("utf-8"),
                value=valor,
                timestamp=int(instante * 1000),
                on_delivery=self._informar_entrega,
            )
            self.enviadas += 1
            if not intacto:
                continue
            intactos.append(evento)
            self.fraudes += int(evento.es_fraude)
            if self._observador is not None:
                self._observador(evento, instante)
        self._productor.poll(0)
        if self._etiquetas is not None:
            self._etiquetas.guardar(intactos)

    def ejecutar(
        self,
        generador: GeneradorTransacciones,
        tps: float,
        duracion_s: float | None = None,
        desfase: timedelta = timedelta(0),
        reloj: Callable[[], float] = time.time,
        dormir: Callable[[float], None] = time.sleep,
    ) -> None:
        planificador = PlanificadorEnvios()
        inicio = reloj()
        ultimo = inicio
        ultimo_informe = inicio
        enviadas_informe = 0
        while duracion_s is None or reloj() - inicio < duracion_s:
            ahora = reloj()
            simulado = _a_fecha(ahora) + desfase
            planificador.agregar(generador.generar(_a_fecha(ultimo) + desfase, ahora - ultimo, tps))
            ultimo = ahora
            self.enviar(planificador.extraer_listos(simulado), reloj)
            if ahora - ultimo_informe >= INTERVALO_INFORME_S:
                tps_real = (self.enviadas - enviadas_informe) / (ahora - ultimo_informe)
                logger.info(
                    "Enviadas: %d | fraude: %d (%.2f %%) | rotas: %d | pendientes: %d | TPS: %.1f",
                    self.enviadas,
                    self.fraudes,
                    100 * self.fraudes / max(self.enviadas, 1),
                    self.rotas,
                    len(planificador),
                    tps_real,
                )
                if generador.saturado:
                    logger.warning(
                        "Población insuficiente: %d tarjetas admiten %.0f TPS sin reutilizar "
                        "una tarjeta antes del intervalo mínimo",
                        generador.num_tarjetas,
                        generador.capacidad_tps,
                    )
                ultimo_informe = ahora
                enviadas_informe = self.enviadas
            dormir(max(0.0, TICK_S - (reloj() - ahora)))
        ocurridos, futuros = planificador.extraer_ocurridos(_a_fecha(reloj()) + desfase)
        self.enviar(ocurridos, reloj)
        self._productor.flush(30)
        logger.info(
            "Fin del envío: %d transacciones enviadas y %d programadas a futuro descartadas",
            self.enviadas,
            futuros,
        )


def _a_fecha(instante: float) -> datetime:
    return datetime.fromtimestamp(instante, tz=UTC)


def desfase_para_hora(hora_inicio: str, ahora: datetime) -> timedelta:
    horas, minutos = (int(parte) for parte in hora_inicio.split(":"))
    local = ahora.astimezone(ZONA_LOCAL)
    objetivo = local.replace(hour=horas, minute=minutos, second=0, microsecond=0)
    return objetivo - local


def crear_productor_kafka(servidores: str) -> Producer:
    return Producer({"bootstrap.servers": servidores, **CONFIG_KAFKA})


def cargar_perfiles(coleccion: Collection) -> list[dict]:
    return list(coleccion.find({}, {"fecha_alta": 0}))


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Envía transacciones sintéticas a Kafka")
    parser.add_argument("--tps", type=float, default=config.tps)
    parser.add_argument("--duracion", type=float, default=None)
    parser.add_argument("--tasa-fraude", type=float, default=config.tasa_fraude)
    parser.add_argument("--tasa-retraso", type=float, default=config.tasa_retraso)
    parser.add_argument("--tasa-mensajes-rotos", type=float, default=0.0)
    parser.add_argument("--hora-inicio", default=None)
    parser.add_argument("--semilla", type=int, default=None)
    args = parser.parse_args()

    if not 50 <= args.tps <= 500:
        logger.warning("El enunciado contempla entre 50 y 500 TPS; se usarán %.0f", args.tps)

    with MongoClient(config.mongo_uri, tz_aware=True) as cliente:
        base = cliente[config.mongo_db]
        perfiles = cargar_perfiles(base["clientes"])
        if not perfiles:
            logger.error("No hay perfiles en MongoDB. Ejecuta antes: python -m generador.perfiles")
            return
        generador = GeneradorTransacciones(
            perfiles,
            tasa_fraude=args.tasa_fraude,
            tasa_retraso=args.tasa_retraso,
            intervalo_minimo_s=config.intervalo_minimo_tarjeta_s,
            semilla=args.semilla,
        )
        if generador.capacidad_tps < args.tps:
            logger.warning(
                "Con %d tarjetas solo caben %.0f TPS respetando %d s entre compras de una "
                "misma tarjeta. Genera más clientes: python -m generador.perfiles --clientes %d",
                generador.num_tarjetas,
                generador.capacidad_tps,
                config.intervalo_minimo_tarjeta_s,
                clientes_necesarios(
                    args.tps,
                    config.intervalo_minimo_tarjeta_s,
                    generador.num_tarjetas / len(perfiles),
                ),
            )
        desfase = (
            desfase_para_hora(args.hora_inicio, ahora_utc()) if args.hora_inicio else timedelta(0)
        )
        productor = ProductorTransacciones(
            crear_productor_kafka(config.kafka_servidores),
            config.topico_transacciones,
            etiquetas=AlmacenEtiquetas(base["etiquetas"]),
            tasa_mensajes_rotos=args.tasa_mensajes_rotos,
            semilla=args.semilla,
        )
        logger.info(
            "Generando %.0f TPS con %d clientes y %d tarjetas en el topic %s",
            args.tps,
            len(perfiles),
            generador.num_tarjetas,
            config.topico_transacciones,
        )
        productor.ejecutar(generador, args.tps, args.duracion, desfase)


def clientes_necesarios(tps: float, intervalo_s: float, tarjetas_por_cliente: float) -> int:
    return int(tps * intervalo_s / max(tarjetas_por_cliente, 1.0) * 1.2) + 1


if __name__ == "__main__":
    main()
