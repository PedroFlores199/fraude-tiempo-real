import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from comun.config import Config, cargar_config
from comun.esquema import cargar_esquema
from comun.registro import configurar_logging
from streaming.ejecutor import escribir_rafagas_particion, evaluar_particiones
from streaming.flujos import (
    ESQUEMA_DECISION,
    compras_por_ventana,
    mensajes_decision,
    mensajes_error,
    parsear,
    resumen_lote,
    separar,
)
from streaming.metricas import AcumuladorMetricas, EscritorMetricas, ResumenLote
from streaming.sesion import crear_sesion

logger = logging.getLogger(__name__)


class ProcesadorDecisiones:
    def __init__(
        self,
        config: Config,
        acumulador: AcumuladorMetricas | None = None,
        escritor: EscritorMetricas | None = None,
        publicar: Callable[[DataFrame], None] | None = None,
    ) -> None:
        self._config = config
        self._acumulador = acumulador or AcumuladorMetricas(config.intervalo_metricas_s)
        self._escritor = escritor or EscritorMetricas(config.postgres_dsn)
        self._publicar = publicar or self._publicar_en_kafka

    def _publicar_en_kafka(self, mensajes: DataFrame) -> None:
        (
            mensajes.write.format("kafka")
            .option("kafka.bootstrap.servers", self._config.kafka_servidores)
            .option("topic", self._config.topico_decisiones)
            .save()
        )

    def __call__(self, lote: DataFrame, id_lote: int) -> None:
        lote.persist()
        try:
            resumen = resumen_lote(lote).first()
            if resumen is None or resumen["transacciones"] == 0:
                return
            self._publicar(mensajes_decision(lote))
            fila = self._acumulador.registrar(
                ResumenLote(
                    transacciones=int(resumen["transacciones"]),
                    bloqueadas=int(resumen["bloqueadas"]),
                    revisadas=int(resumen["revisadas"]),
                    suma_latencia_ms=float(resumen["suma_latencia_ms"]),
                    max_latencia_ms=float(resumen["max_latencia_ms"]),
                )
            )
            if fila is not None:
                self._escritor.insertar(fila)
                logger.info(
                    "Lote %d: %.1f TPS, %.2f %% bloqueadas, latencia media %.0f ms",
                    id_lote,
                    fila.transacciones_por_segundo,
                    fila.porcentaje_bloqueadas,
                    fila.latencia_media_ms,
                )
        finally:
            lote.unpersist()


def escribir_rafagas(lote: DataFrame, id_lote: int) -> None:
    lote.foreachPartition(escribir_rafagas_particion)


def leer_kafka(spark: SparkSession, config: Config) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_servidores)
        .option("subscribe", config.topico_transacciones)
        .option("startingOffsets", config.kafka_offsets_iniciales)
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )


def iniciar_consultas(spark: SparkSession, config: Config) -> list[StreamingQuery]:
    esquema = cargar_esquema()
    validas, errores = separar(parsear(leer_kafka(spark, config), esquema), esquema)
    checkpoints = config.checkpoint_dir.rstrip("/")

    consulta_errores = (
        mensajes_error(errores)
        .writeStream.format("kafka")
        .queryName("errores")
        .option("kafka.bootstrap.servers", config.kafka_servidores)
        .option("topic", config.topico_errores)
        .option("checkpointLocation", f"{checkpoints}/errores")
        .start()
    )
    consulta_decisiones = (
        validas.mapInPandas(evaluar_particiones, ESQUEMA_DECISION)
        .writeStream.queryName("decisiones")
        .foreachBatch(ProcesadorDecisiones(config))
        .option("checkpointLocation", f"{checkpoints}/decisiones")
        .trigger(processingTime=f"{config.trigger_segundos} seconds")
        .start()
    )
    consulta_ventanas = (
        compras_por_ventana(validas, config.watermark)
        .writeStream.queryName("ventanas")
        .outputMode("update")
        .foreachBatch(escribir_rafagas)
        .option("checkpointLocation", f"{checkpoints}/ventanas")
        .start()
    )
    return [consulta_errores, consulta_decisiones, consulta_ventanas]


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    spark = crear_sesion(config)
    consultas = iniciar_consultas(spark, config)
    logger.info("Consultas en marcha: %s", ", ".join(c.name for c in consultas))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
