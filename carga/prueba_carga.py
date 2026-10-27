import argparse
import json
import logging
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer
from pymongo import MongoClient

from carga.estadisticas import percentiles
from comun.config import Config, cargar_config
from comun.registro import configurar_logging
from generador.perfiles import asegurar_poblacion
from generador.productor import (
    ProductorTransacciones,
    cargar_perfiles,
    clientes_necesarios,
    crear_productor_kafka,
)
from generador.transacciones import Evento, GeneradorTransacciones

logger = logging.getLogger(__name__)

LIMITE_LATENCIA_MS = 5000.0
TARJETAS_POR_CLIENTE_ESTIMADAS = 2.0


class RegistroLatencias:
    def __init__(self) -> None:
        self._candado = threading.Lock()
        self.enviados: dict[str, float] = {}
        self.decididos: dict[str, float] = {}

    def al_enviar(self, evento: Evento, instante: float) -> None:
        with self._candado:
            self.enviados[evento.transaccion["id_transaccion"]] = instante

    def al_decidir(self, id_transaccion: str, instante: float) -> None:
        with self._candado:
            self.decididos.setdefault(id_transaccion, instante)

    def latencias_ms(self) -> list[float]:
        with self._candado:
            return [
                (self.decididos[i] - enviado) * 1000
                for i, enviado in self.enviados.items()
                if i in self.decididos
            ]

    def pendientes(self) -> int:
        with self._candado:
            return sum(1 for i in self.enviados if i not in self.decididos)


def escuchar_decisiones(consumidor, registro: RegistroLatencias, parar: threading.Event) -> None:
    while not parar.is_set():
        mensaje = consumidor.poll(0.5)
        if mensaje is None or mensaje.error():
            continue
        decision = json.loads(mensaje.value())
        registro.al_decidir(decision["id_transaccion"], time.time())


def crear_consumidor(config: Config) -> Consumer:
    consumidor = Consumer(
        {
            "bootstrap.servers": config.kafka_servidores,
            "group.id": f"prueba-carga-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumidor.subscribe([config.topico_decisiones])
    limite = time.time() + 30
    while not consumidor.assignment() and time.time() < limite:
        consumidor.poll(0.5)
    return consumidor


def resumen(registro: RegistroLatencias, tps: float, duracion_s: float, poblacion: dict) -> dict:
    latencias = registro.latencias_ms()
    enviados = len(registro.enviados)
    return {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "tps_objetivo": tps,
        "duracion_s": duracion_s,
        "tps_enviado": round(enviados / duracion_s, 1),
        "transacciones_enviadas": enviados,
        "decisiones_recibidas": len(latencias),
        "sin_decision": registro.pendientes(),
        "latencia_extremo_a_extremo_ms": percentiles(latencias),
        "porcentaje_bajo_5_s": round(
            100 * sum(1 for x in latencias if x <= LIMITE_LATENCIA_MS) / max(len(latencias), 1), 3
        ),
        "poblacion": poblacion,
    }


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Prueba de carga extremo a extremo")
    parser.add_argument("--tps", type=float, default=500)
    parser.add_argument("--duracion", type=float, default=300)
    parser.add_argument("--espera-final", type=float, default=30)
    parser.add_argument("--salida", default="carga/resultados")
    args = parser.parse_args()

    with MongoClient(config.mongo_uri, tz_aware=True) as cliente:
        base = cliente[config.mongo_db]
        necesarios = clientes_necesarios(
            args.tps, config.intervalo_minimo_tarjeta_s, TARJETAS_POR_CLIENTE_ESTIMADAS
        )
        nuevos = asegurar_poblacion(base["clientes"], necesarios)
        perfiles = cargar_perfiles(base["clientes"])
    generador = GeneradorTransacciones(
        perfiles,
        tasa_fraude=config.tasa_fraude,
        tasa_retraso=config.tasa_retraso,
        intervalo_minimo_s=config.intervalo_minimo_tarjeta_s,
    )
    poblacion = {
        "clientes": len(perfiles),
        "clientes_nuevos": nuevos,
        "tarjetas": generador.num_tarjetas,
        "capacidad_tps": round(generador.capacidad_tps, 1),
    }
    logger.info("Población para la prueba: %s", poblacion)

    registro = RegistroLatencias()
    consumidor = crear_consumidor(config)
    parar = threading.Event()
    hilo = threading.Thread(target=escuchar_decisiones, args=(consumidor, registro, parar))
    hilo.start()
    productor = ProductorTransacciones(
        crear_productor_kafka(config.kafka_servidores),
        config.topico_transacciones,
        observador=registro.al_enviar,
    )
    try:
        logger.info("Enviando %.0f TPS durante %.0f s", args.tps, args.duracion)
        productor.ejecutar(generador, args.tps, args.duracion)
        limite = time.time() + args.espera_final
        while registro.pendientes() and time.time() < limite:
            time.sleep(1)
    finally:
        parar.set()
        hilo.join()
        consumidor.close()

    informe = resumen(registro, args.tps, args.duracion, poblacion)
    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)
    fichero = salida / f"prueba_carga_{datetime.now():%Y%m%d_%H%M%S}.json"
    fichero.write_text(json.dumps(informe, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Resultado:\n%s", json.dumps(informe, indent=2, ensure_ascii=False))
    logger.info("Informe guardado en %s", fichero)


if __name__ == "__main__":
    main()
